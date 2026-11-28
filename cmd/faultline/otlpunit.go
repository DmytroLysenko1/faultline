package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/connlimit"
)

const (
	layerOTLP                = "otlp"
	stageOTLPListen          = "otlp/listen"
	stageOTLPTap             = "otlp/tap"
	stageOTLPAdmission       = "otlp/admission"
	otlpStopGrace            = 3500 * time.Millisecond
	admissionEntriesPerGrant = 2
	componentOTLP            = "otlp"
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
	address     string
	ingest      ingestConfig
	journals    journalSource
	taps        tapSource
	rules       *operation.Rules
	logger      *slog.Logger
	sup         unitSupervisor
	server      ingestServer
	accepted    atomic.Pointer[ingest.AcceptBatch]
	admitted    atomic.Pointer[admission.Admission]
	connections atomic.Pointer[connlimit.Listener]
}

func otlpLayer(
	cfg *Config,
	limits ingestConfig,
	journals journalSource,
	taps tapSource,
	logger *slog.Logger,
) layer {
	receiver := &otlpUnit{
		address:  cfg.OTLPAddr,
		ingest:   limits,
		journals: journals,
		taps:     taps,
		rules:    cfg.RouteRules,
		logger:   logger,
	}
	return layer{
		name:          layerOTLP,
		unit:          receiver,
		statsGroups:   receiver.statsGroups,
		components:    []string{componentOTLP},
		stopBudget:    otlpStopGrace + exitGrace,
		contributions: otlpContributions(limits),
	}
}

func otlpContributions(limits ingestConfig) []contribution {
	return []contribution{
		{
			name:  "otlp requests",
			bytes: otlp.WorstHeapBytes(),
		},
		{
			name:  "otlp connections",
			bytes: otlp.ConnectionsHeapBytes(limits.MaxConnections),
		},
		{
			name:  "admission tokens",
			bytes: admission.EntryHeapBytes * admissionEntriesPerGrant * tokens.MaxGrants,
		},
	}
}

func (u *otlpUnit) start(ctx context.Context, sup unitSupervisor) error {
	u.sup = sup
	gate, err := u.tap()
	if err != nil {
		return err
	}
	books, err := u.admission()
	if err != nil {
		return err
	}
	listener, err := u.listen(ctx)
	if err != nil {
		return err
	}
	u.serve(listener, otlp.Guards{
		Tap: gate,
		Gate: &admission.Gate{
			Admission: books,
			Marks:     listener,
		},
	})
	return nil
}

func (u *otlpUnit) tap() (*otlp.Tap, error) {
	gate, err := u.taps.tap()
	if err != nil {
		return nil, &stageError{
			stage: stageOTLPTap,
			err:   err,
		}
	}
	return gate, nil
}

func (u *otlpUnit) admission() (*admission.Admission, error) {
	books, err := admission.New(otlp.AdmissionLimits(u.ingest.RequestsPerSecond), systemClock{})
	if err != nil {
		return nil, &stageError{
			stage: stageOTLPAdmission,
			err:   err,
		}
	}
	return books, nil
}

func (u *otlpUnit) listen(ctx context.Context) (*connlimit.Listener, error) {
	var config net.ListenConfig
	inner, err := config.Listen(ctx, network, u.address)
	if err != nil {
		return nil, &stageError{
			stage: stageOTLPListen,
			err:   fmt.Errorf("listen otlp %s: %w", u.address, err),
		}
	}
	return u.limit(inner)
}

func (u *otlpUnit) limit(inner net.Listener) (*connlimit.Listener, error) {
	limits := otlp.ConnectionLimits(u.ingest.MaxConnections)
	listener, err := connlimit.NewListener(inner, limits, systemClock{}, u.logger)
	if err != nil {
		return nil, &stageError{
			stage: stageOTLPListen,
			err:   errors.Join(err, inner.Close()),
		}
	}
	return listener, nil
}

func (u *otlpUnit) serve(listener *connlimit.Listener, guards otlp.Guards) {
	accept := ingest.NewAcceptBatch(u.journals.opened(), operation.NewNormalizer(u.rules))
	u.publishStats(guards.Gate.Admission, listener, accept)
	u.server = otlp.NewServer(accept, guards, otlp.DefaultOptions(u.logger, systemClock{}))
	u.sup.spawn(componentOTLP, func() error {
		return withoutSentinel(u.server.Serve(listener), grpc.ErrServerStopped)
	})
}

func (u *otlpUnit) publishStats(admitted *admission.Admission, listener *connlimit.Listener, accept *ingest.AcceptBatch) {
	u.admitted.Store(admitted)
	u.connections.Store(listener)
	u.accepted.Store(accept)
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

func (u *otlpUnit) statsGroups(_ context.Context) []slog.Attr {
	accept := u.accepted.Load()
	if accept == nil {
		return nil
	}
	return []slog.Attr{
		ingestGroup(accept.Stats()),
		admissionGroup(u.admitted.Load().Stats()),
		connectionsGroup(u.connections.Load().Stats()),
	}
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
