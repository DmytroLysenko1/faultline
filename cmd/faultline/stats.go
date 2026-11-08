package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const statsInterval = 30 * time.Second

type snapshotSource interface {
	Latest(ctx context.Context) (*topology.Snapshot, error)
}

type ingestCounters interface {
	Stats() ingest.Stats
}

type journalGauge interface {
	Stats() memjournal.Stats
}

type statsReporter struct {
	snapshots snapshotSource
	ingest    ingestCounters
	journal   journalGauge
	logger    *slog.Logger
	interval  time.Duration
}

func (r *statsReporter) Run(ctx context.Context) error {
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
	snapshot, err := r.snapshots.Latest(context.WithoutCancel(ctx))
	if err != nil {
		r.logger.Warn("pipeline.stats_failed", "error", err)
		return
	}
	r.logger.Info("pipeline.stats",
		slog.Bool("final", final),
		slog.Uint64("revision", snapshot.Revision),
		slog.Int("nodes", len(snapshot.Nodes)),
		slog.Int("edges", len(snapshot.Edges)),
		pairingGroup(&snapshot.PairingStats),
		graphGroup(snapshot.GraphStats),
		ingestGroup(r.ingest.Stats()),
		journalGroup(r.journal.Stats()),
	)
}

func pairingGroup(stats *matcher.Stats) slog.Attr {
	return slog.Group("pairing",
		slog.Uint64("observed", stats.Observed),
		slog.Uint64("matched", stats.Matched),
		slog.Uint64("duplicate", stats.Duplicate),
		slog.Uint64("unpairable", stats.Unpairable),
		slog.Uint64("evicted", stats.Evicted),
		slog.Uint64("expired", stats.Expired),
		slog.Uint64("pending", stats.Pending),
		slog.Uint64("expired_caller", stats.ExpiredCaller),
		slog.Uint64("evicted_caller", stats.EvictedCaller),
		slog.Uint64("tombstone_evicted", stats.TombstoneEvicted),
	)
}

func graphGroup(stats graph.Stats) slog.Attr {
	return slog.Group("graph",
		slog.Uint64("graph_overflow", stats.GraphOverflow),
		slog.Uint64("unanswered_overflow", stats.UnansweredOverflow),
		slog.Uint64("node_overflow", stats.NodeOverflow),
	)
}

func ingestGroup(stats ingest.Stats) slog.Attr {
	return slog.Group("ingest",
		slog.Int64("accepted_spans", stats.AcceptedSpans),
		slog.Int64("rejected_spans", stats.RejectedSpans),
		slog.Int64("overloaded_responses", stats.OverloadedResponses),
		slog.Int64("too_large_responses", stats.TooLargeResponses),
	)
}

func journalGroup(stats memjournal.Stats) slog.Attr {
	return slog.Group("journal",
		slog.Int64("bytes", stats.Bytes),
		slog.Int("batches", stats.Batches),
		slog.Int("spans", stats.Spans),
	)
}
