package main

import (
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	maxListedOwners  = 16
	stateRaised      = "raised"
	stateCleared     = "cleared"
	reasonLowTraffic = "low_traffic"
)

func sampleOf(snapshot *topology.Snapshot, now time.Time) *selfalert.Sample {
	stats := &snapshot.TrafficStats
	return &selfalert.Sample{
		Now:                now,
		Watermark:          snapshot.Watermark,
		WatermarkKnown:     snapshot.WatermarkKnown,
		Offered:            stats.Subjects.Offered,
		Reassigned:         stats.Subjects.Reassigned,
		LateDropped:        stats.Subjects.LateDropped,
		OwnersOverLimit:    stats.Cardinality.OwnersOverLimit,
		Owners:             stats.OwnersOverLimit[:min(len(stats.OwnersOverLimit), maxListedOwners)],
		SeriesLimitReached: stats.SeriesLimitReached,
		DroppedSamples:     stats.Sketch.DroppedSamples,
	}
}

func levelOf(transition *selfalert.Transition) slog.Level {
	if transition.Raised {
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

func transitionAttrs(transition *selfalert.Transition) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("state", stateOf(transition)),
		slog.Float64("value", transition.Reading.Value),
		slog.Float64("threshold", transition.Reading.Threshold),
		slog.Uint64("volume", transition.Reading.Volume),
	}
	if transition.LowTraffic {
		attrs = append(attrs, slog.String("reason", reasonLowTraffic))
	}
	if len(transition.Owners) > 0 {
		attrs = append(attrs, slog.Any("services", ownerNames(transition.Owners)))
	}
	return attrs
}

func stateOf(transition *selfalert.Transition) string {
	if transition.Raised {
		return stateRaised
	}
	return stateCleared
}

func ownerNames(owners []resource.ServiceRef) []string {
	names := make([]string, 0, len(owners))
	for _, owner := range owners {
		names = append(names, owner.Namespace+"/"+owner.Name)
	}
	return names
}
