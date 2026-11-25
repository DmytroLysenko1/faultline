package main

import (
	"context"
	"log/slog"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
)

const eventStatsFailed = "pipeline.stats_failed"

func (u *partitionUnit) sample(ctx context.Context, sample *selfalert.Sample) {
	snapshot, err := u.graph.Latest(ctx)
	if err != nil {
		return
	}
	fillSample(snapshot, sample)
}

func (u *partitionUnit) statsGroups(ctx context.Context) []slog.Attr {
	snapshot, err := u.graph.Latest(ctx)
	if err != nil {
		u.logger.WarnContext(ctx, eventStatsFailed, "error", err)
		return nil
	}
	return []slog.Attr{
		slog.Uint64("revision", snapshot.Revision),
		slog.Int("nodes", len(snapshot.Nodes)),
		slog.Int("edges", len(snapshot.Edges)),
		pairingGroup(&snapshot.PairingStats),
		graphGroup(snapshot),
		windowsGroup(snapshot, u.clock.Now()),
		cardinalityGroup(&snapshot.TrafficStats),
		sketchGroup(&snapshot.TrafficStats),
		pipelineGroup(u.pipeline.TakeTimings()),
	}
}
