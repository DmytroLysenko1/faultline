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
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
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

type unmarkedConnections struct{}

func (unmarkedConnections) Authenticated(_ net.Addr, _ credential.ID) {}

type otlpUnit struct {
	address  string
	ingest   ingestConfig
	journals journalSource
	taps     tapSource
	rules    *operation.Rules
	logger   *slog.Logger
	sup      unitSupervisor
	server   ingestServer
	accepted atomic.Pointer[ingest.AcceptBatch]
	admitted atomic.Pointer[admission.Admission]
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
		contributions: otlpContributions(),
	}
}

func otlpContributions() []contribution {
	return []contribution{
		{
			name:  "otlp requests",
			bytes: otlp.WorstHeapBytes(),
		},
		{
			name:  "admission tokens",
			bytes: admission.EntryHeapBytes * admissionEntriesPerGrant * tokens.MaxGrants,
		},
	}
}

func (u *otlpUnit) start(ctx context.Context, sup unitSupervisor) error {
	u.sup = sup
	guards, err := u.guards()
	if err != nil {
		return err
	}
	listener, err := u.listen(ctx)
	if err != nil {
		return err
	}
	u.serve(listener, guards)
	return nil
}

func (u *otlpUnit) listen(ctx context.Context) (net.Listener, error) {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, network, u.address)
	if err != nil {
		return nil, &stageError{
			stage: stageOTLPListen,
			err:   fmt.Errorf("listen otlp %s: %w", u.address, err),
		}
	}
	return listener, nil
}

func (u *otlpUnit) serve(listener net.Listener, guards otlp.Guards) {
	accept := ingest.NewAcceptBatch(u.journals.opened(), operation.NewNormalizer(u.rules))
	u.publishStats(guards.Gate.Admission, accept)
	u.server = otlp.NewServer(accept, guards, otlp.DefaultOptions(u.logger, systemClock{}))
	limited := otlp.LimitListener(listener)
	u.sup.spawn(componentOTLP, func() error {
		return withoutSentinel(u.server.Serve(limited), grpc.ErrServerStopped)
	})
}

func (u *otlpUnit) publishStats(admitted *admission.Admission, accept *ingest.AcceptBatch) {
	u.admitted.Store(admitted)
	u.accepted.Store(accept)
}

func (u *otlpUnit) guards() (otlp.Guards, error) {
	gate, err := u.taps.tap()
	if err != nil {
		return otlp.Guards{}, &stageError{
			stage: stageOTLPTap,
			err:   err,
		}
	}
	books, err := admission.New(otlp.AdmissionLimits(u.ingest.RequestsPerSecond), systemClock{})
	if err != nil {
		return otlp.Guards{}, &stageError{
			stage: stageOTLPAdmission,
			err:   err,
		}
	}
	return otlp.Guards{
		Tap: gate,
		Gate: &admission.Gate{
			Admission: books,
			Marks:     unmarkedConnections{},
		},
	}, nil
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
