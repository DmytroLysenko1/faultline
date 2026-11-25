package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

const (
	layerTracing = "tracing"
	layerHTTP    = "http"
	layerStats   = "stats"

	stageTracingStart = "tracing/start"
	stageHTTPListen   = "http/listen"
	stageHTTPProbe    = "http/probe_otlp"
	stageHTTPRoutes   = "http/routes"

	eventForcedStop = "lifecycle.forced_stop"

	componentAPI = "api"
	network      = "tcp"
	httpGrace    = 2 * time.Second
	traceFlush   = 200 * time.Millisecond
)

type selfTracing interface {
	TracerProvider() trace.TracerProvider
	Shutdown(ctx context.Context) error
}

type apiServer interface {
	Serve(listener net.Listener) error
	Shutdown(ctx context.Context) error
	Close() error
}

type apiRoutes func(tracer trace.TracerProvider) (http.Handler, error)

type tracerSource interface {
	tracerProvider() trace.TracerProvider
}

type tracingUnit struct {
	endpoint string
	logger   *slog.Logger
	tracing  selfTracing
}

func tracingLayer(cfg *Config, logger *slog.Logger) (layer, *tracingUnit) {
	traced := &tracingUnit{
		endpoint: cfg.SelfTracingEndpoint,
		logger:   logger,
	}
	return layer{
		name:          layerTracing,
		unit:          traced,
		stopBudget:    traceFlush,
		contributions: selfTracingContribution(cfg),
	}, traced
}

func (u *tracingUnit) start(ctx context.Context, _ unitSupervisor) error {
	tracing, err := provideTracing(ctx, u.endpoint, u.logger)
	if err != nil {
		return &stageError{
			stage: stageTracingStart,
			err:   err,
		}
	}
	u.tracing = tracing
	return nil
}

func provideTracing(ctx context.Context, endpoint string, logger *slog.Logger) (selfTracing, error) {
	if endpoint == "" {
		return selftrace.Disabled(), nil
	}
	tracing, err := selftrace.New(ctx, endpoint, logger)
	if err != nil {
		return nil, fmt.Errorf("build self-tracing: %w", err)
	}
	return tracing, nil
}

func (u *tracingUnit) stop(ctx context.Context) error {
	if u.tracing == nil {
		return nil
	}
	if err := u.tracing.Shutdown(ctx); err != nil {
		u.logger.WarnContext(ctx, eventForcedStop,
			"layer", layerTracing,
			"error", err,
		)
	}
	return nil
}

func (u *tracingUnit) tracerProvider() trace.TracerProvider {
	return u.tracing.TracerProvider()
}

type apiUnit struct {
	address string
	probe   string
	routes  apiRoutes
	tracer  tracerSource
	logger  *slog.Logger
	sup     unitSupervisor
	server  apiServer
}

func httpLayer(
	cfg *Config,
	tracer tracerSource,
	routes apiRoutes,
	otlpProbe string,
	logger *slog.Logger,
) layer {
	return layer{
		name: layerHTTP,
		unit: &apiUnit{
			address: cfg.APIAddr,
			probe:   otlpProbe,
			routes:  routes,
			tracer:  tracer,
			logger:  logger,
		},
		components: []string{componentAPI},
		stopBudget: httpGrace + exitGrace,
	}
}

func (u *apiUnit) start(ctx context.Context, sup unitSupervisor) error {
	u.sup = sup
	listener, err := u.listen(ctx)
	if err != nil {
		return err
	}
	handler, err := u.routes(u.tracer.tracerProvider())
	if err != nil {
		routesErr := &stageError{
			stage: stageHTTPRoutes,
			err:   err,
		}
		return errors.Join(routesErr, listener.Close())
	}
	u.server = connectapi.NewServer(handler)
	sup.spawn(componentAPI, func() error {
		return withoutSentinel(u.server.Serve(listener), http.ErrServerClosed)
	})
	return nil
}

func (u *apiUnit) listen(ctx context.Context) (net.Listener, error) {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, network, u.address)
	if err != nil {
		return nil, &stageError{
			stage: stageHTTPListen,
			err:   fmt.Errorf("listen api %s: %w", u.address, err),
		}
	}
	if err := u.probeOTLP(ctx, &config); err != nil {
		return nil, errors.Join(
			err,
			listener.Close(),
		)
	}
	return listener, nil
}

func (u *apiUnit) stop(ctx context.Context) error {
	if u.server == nil {
		return nil
	}
	u.shutdown(ctx)
	exited, err := u.sup.wait(ctx, componentAPI)
	if !exited {
		return errStepAbandoned
	}
	return err
}

func (u *apiUnit) probeOTLP(ctx context.Context, config *net.ListenConfig) error {
	if u.probe == "" {
		return nil
	}
	probe, err := config.Listen(ctx, network, u.probe)
	if err != nil {
		return &stageError{
			stage: stageHTTPProbe,
			err:   fmt.Errorf("listen otlp %s: %w", u.probe, err),
		}
	}
	if err := probe.Close(); err != nil {
		return &stageError{
			stage: stageHTTPProbe,
			err:   fmt.Errorf("close the otlp probe %s: %w", u.probe, err),
		}
	}
	return nil
}

func (u *apiUnit) shutdown(ctx context.Context) {
	graceCtx, cancel := context.WithTimeout(ctx, httpGrace)
	defer cancel()
	err := u.server.Shutdown(graceCtx)
	if err == nil {
		return
	}
	u.logger.WarnContext(ctx, eventForcedStop,
		"layer", layerHTTP,
		"error", err,
	)
	if closeErr := u.server.Close(); closeErr != nil {
		u.logger.WarnContext(ctx, eventForcedStop,
			"layer", layerHTTP,
			"error", closeErr,
		)
	}
}
