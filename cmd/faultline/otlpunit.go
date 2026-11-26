package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	layerOTLP       = "otlp"
	stageOTLPListen = "otlp/listen"
	stageOTLPTap    = "otlp/tap"
	otlpStopGrace   = 3500 * time.Millisecond
	componentOTLP   = "otlp"
)

type ingestServer interface {
	Serve(listener net.Listener) error
	GracefulStop()
	Stop()
}

type journalSource interface {
	opened() *journal.Journal
}

type tapSource interface {
	tap() (*otlp.Tap, error)
}

type otlpUnit struct {
	address  string
	journals journalSource
	taps     tapSource
	rules    *operation.Rules
	logger   *slog.Logger
	sup      unitSupervisor
	server   ingestServer
	accepted atomic.Pointer[ingest.AcceptBatch]
}

func otlpLayer(cfg *Config, journals journalSource, taps tapSource, logger *slog.Logger) layer {
	receiver := &otlpUnit{
		address:  cfg.OTLPAddr,
		journals: journals,
		taps:     taps,
		rules:    cfg.RouteRules,
		logger:   logger,
	}
	return layer{
		name:        layerOTLP,
		unit:        receiver,
		statsGroups: receiver.statsGroups,
		components:  []string{componentOTLP},
		stopBudget:  otlpStopGrace + exitGrace,
		contributions: []contribution{
			{
				name:  "otlp requests",
				bytes: otlp.WorstHeapBytes(),
			},
		},
	}
}

func (u *otlpUnit) start(ctx context.Context, sup unitSupervisor) error {
	u.sup = sup
	gate, err := u.taps.tap()
	if err != nil {
		return &stageError{
			stage: stageOTLPTap,
			err:   err,
		}
	}
	var config net.ListenConfig
	listener, err := config.Listen(ctx, network, u.address)
	if err != nil {
		return &stageError{
			stage: stageOTLPListen,
			err:   fmt.Errorf("listen otlp %s: %w", u.address, err),
		}
	}
	accept := ingest.NewAcceptBatch(u.journals.opened(), operation.NewNormalizer(u.rules))
	u.accepted.Store(accept)
	u.server = otlp.NewServer(accept, gate, otlp.DefaultOptions(u.logger, systemClock{}))
	limited := otlp.LimitListener(listener)
	sup.spawn(componentOTLP, func() error {
		return withoutSentinel(u.server.Serve(limited), grpc.ErrServerStopped)
	})
	return nil
}

func (u *otlpUnit) stop(ctx context.Context) error {
	if u.server == nil {
		return nil
	}
	forced := u.drain(ctx)
	exited, err := u.sup.wait(ctx, componentOTLP)
	if !exited {
		return withForcedCause(forced)
	}
	if forced != nil {
		u.logger.WarnContext(ctx, eventForcedStop, "layer", layerOTLP, "error", forced)
	}
	return err
}

func (u *otlpUnit) drain(ctx context.Context) error {
	graceCtx, cancel := context.WithTimeout(ctx, otlpStopGrace)
	defer cancel()
	if closedWithin(graceCtx, inBackground(u.server.GracefulStop)) {
		return nil
	}
	inBackground(u.server.Stop)
	return graceCtx.Err()
}

func (u *otlpUnit) statsGroups(context.Context) []slog.Attr {
	accept := u.accepted.Load()
	if accept == nil {
		return nil
	}
	return []slog.Attr{ingestGroup(accept.Stats())}
}

func inBackground(stop func()) <-chan struct{} {
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()
	return stopped
}

func closedWithin(ctx context.Context, stopped <-chan struct{}) bool {
	select {
	case <-stopped:
		return true
	case <-ctx.Done():
		return false
	}
}

func withForcedCause(forced error) error {
	if forced == nil {
		return errStepAbandoned
	}
	return fmt.Errorf("%w: %w", errStepAbandoned, forced)
}
