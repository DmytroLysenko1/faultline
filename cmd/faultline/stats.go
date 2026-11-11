package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	statsInterval = 30 * time.Second
	lagSlack      = 30 * time.Second
)

var errReporterStarted = errors.New("stats reporter already started")

type snapshotSource interface {
	Latest(ctx context.Context) (*topology.Snapshot, error)
}

type ingestCounters interface {
	Stats() ingest.Stats
}

type journalGauge interface {
	Stats() memjournal.Stats
}

type pipelineTimings interface {
	TakeTimings() processing.Timings
}

type wallClock interface {
	Now() time.Time
}

type statsReporter struct {
	snapshots snapshotSource
	ingest    ingestCounters
	journal   journalGauge
	timings   pipelineTimings
	alerts    *selfalert.Evaluator
	clock     wallClock
	logger    *slog.Logger
	interval  time.Duration
	started   atomic.Bool
}

func (r *statsReporter) Run(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return errReporterStarted
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.report(ctx, true)
			return nil
		case <-ticker.C:
			r.report(ctx, false)
		}
	}
}

func (r *statsReporter) report(ctx context.Context, final bool) {
	logCtx := context.WithoutCancel(ctx)
	snapshot, err := r.snapshots.Latest(logCtx)
	if err != nil {
		r.logger.WarnContext(logCtx, "pipeline.stats_failed", "error", err)
		return
	}
	timings := r.timings.TakeTimings()
	now := r.clock.Now()
	if !final {
		r.logTransitions(logCtx, r.alerts.Evaluate(sampleOf(snapshot, now)))
	}
	r.logger.LogAttrs(logCtx, slog.LevelInfo, "pipeline.stats",
		slog.Bool("final", final),
		slog.Uint64("revision", snapshot.Revision),
		slog.Int("nodes", len(snapshot.Nodes)),
		slog.Int("edges", len(snapshot.Edges)),
		pairingGroup(&snapshot.PairingStats),
		graphGroup(snapshot),
		windowsGroup(snapshot, now),
		cardinalityGroup(&snapshot.TrafficStats),
		sketchGroup(&snapshot.TrafficStats),
		pipelineGroup(timings),
		alertsGroup(r.alerts.Alerts()),
		ingestGroup(r.ingest.Stats()),
		journalGroup(r.journal.Stats()),
	)
}

func (r *statsReporter) logTransitions(ctx context.Context, transitions []selfalert.Transition) {
	for index := range transitions {
		transition := &transitions[index]
		r.logger.LogAttrs(ctx, levelOf(transition), transition.Event, transitionAttrs(transition)...)
	}
}

func provideStats(stored storage, counters ingestCounters, timings pipelineTimings, cfg *Config,
	logger *slog.Logger,
) *statsReporter {
	return &statsReporter{
		snapshots: stored.store,
		ingest:    counters,
		journal:   stored.journal,
		timings:   timings,
		alerts:    selfalert.NewEvaluator(alertThresholds(cfg)),
		clock:     systemClock{},
		logger:    logger,
		interval:  statsInterval,
	}
}

func alertThresholds(cfg *Config) selfalert.Thresholds {
	return selfalert.Thresholds{
		WatermarkLag: cfg.Topology.Windows.Delay + cfg.Journal.IdleAfter + cfg.Topology.EarlyFirePeriod + lagSlack,
	}
}
