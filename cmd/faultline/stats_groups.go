package main

import (
	"log/slog"
	"slices"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

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

func graphGroup(snapshot *topology.Snapshot) slog.Attr {
	return slog.Group("graph",
		slog.Uint64("graph_overflow", snapshot.GraphStats.GraphOverflow),
		slog.Uint64("unanswered_overflow", snapshot.GraphStats.UnansweredOverflow),
		slog.Uint64("node_overflow", snapshot.GraphStats.NodeOverflow),
		slog.Uint64("calls_total", callsTotal(snapshot)),
		slog.Uint64("served_total", servedTotal(snapshot)),
	)
}

func callsTotal(snapshot *topology.Snapshot) uint64 {
	var total uint64
	for index := range snapshot.Edges {
		total += snapshot.Edges[index].Calls
	}
	return total
}

func servedTotal(snapshot *topology.Snapshot) uint64 {
	var total uint64
	for index := range snapshot.Nodes {
		total += snapshot.Nodes[index].Served
	}
	return total
}

func windowsGroup(snapshot *topology.Snapshot, now time.Time) slog.Attr {
	stats := &snapshot.TrafficStats
	counters := []any{
		slog.Float64("window_size_seconds", snapshot.WindowSize.Seconds()),
		slog.Bool("watermark_known", snapshot.WatermarkKnown),
		slog.Uint64("heartbeats", stats.Heartbeats),
		slog.Uint64("finals", stats.Finals),
		slog.Uint64("untimed", stats.Untimed),
		slog.Uint64("latency_unknown", stats.LatencyUnknown),
		engineGroup("subjects", &stats.Subjects),
		engineGroup("operations", &stats.Operations),
		engineGroup("overflow", &stats.Overflow),
	}
	return slog.Group("windows", slices.Concat(counters, watermarkAttrs(snapshot, now))...)
}

func watermarkAttrs(snapshot *topology.Snapshot, now time.Time) []any {
	if !snapshot.WatermarkKnown {
		return nil
	}
	return []any{
		slog.String("watermark", snapshot.Watermark.UTC().Format(time.RFC3339Nano)),
		slog.Float64("watermark_lag_seconds", now.Sub(snapshot.Watermark).Seconds()),
	}
}

func engineGroup(name string, stats *stream.Stats) slog.Attr {
	return slog.Group(name,
		slog.Int("series", stats.Series),
		slog.Int("capacity", stats.Capacity),
		slog.Uint64("offered", stats.Offered),
		slog.Uint64("on_time", stats.OnTime),
		slog.Uint64("late_accepted", stats.LateAccepted),
		slog.Uint64("late_dropped", stats.LateDropped),
		slog.Uint64("no_watermark", stats.NoWatermark),
		slog.Uint64("clock_skew_reassigned", stats.Reassigned),
		slog.Uint64("panes_early", stats.PanesEarly),
		slog.Uint64("panes_on_time", stats.PanesOnTime),
		slog.Uint64("panes_late", stats.PanesLate),
		slog.Uint64("retired", stats.Retired),
	)
}

func cardinalityGroup(stats *traffic.Stats) slog.Attr {
	return slog.Group("cardinality",
		slog.Int("operations", stats.Cardinality.Operations),
		slog.Int("operation_series", stats.Operations.Series),
		slog.Int("overflow_series", stats.Overflow.Series),
		slog.Int("services_over_limit", stats.Cardinality.OwnersOverLimit),
		slog.Bool("series_limit_reached", stats.SeriesLimitReached),
	)
}

func sketchGroup(stats *traffic.Stats) slog.Attr {
	return slog.Group("sketch",
		slog.Int("entries_in_use", stats.Sketch.InUse),
		slog.Int("budget", stats.Sketch.Budget),
		slog.Uint64("collapsed_adds", stats.Sketch.CollapsedAdds),
		slog.Uint64("collapsed_sketches", stats.Sketch.CollapsedSketches),
		slog.Uint64("dropped_samples", stats.Sketch.DroppedSamples),
	)
}

func pipelineGroup(timings processing.Timings) slog.Attr {
	return slog.Group("pipeline",
		slog.Int64("iteration_max_us", timings.Iteration.Microseconds()),
		slog.Int64("apply_max_us", timings.Apply.Microseconds()),
		slog.Int64("fire_max_us", timings.Fire.Microseconds()),
		slog.Int64("publish_max_us", timings.Publish.Microseconds()),
	)
}

func alertsGroup(alerts []selfalert.Alert) slog.Attr {
	attrs := make([]any, 0, len(alerts))
	for _, alert := range alerts {
		attrs = append(attrs, slog.Bool(alert.Event, alert.Raised))
	}
	return slog.Group("alerts", attrs...)
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
