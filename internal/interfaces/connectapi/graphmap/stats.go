package graphmap

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

func windowStats(stats *red.WindowStats) *faultlinev1.WindowStats {
	if stats.IsZero() {
		return nil
	}
	mapped := &faultlinev1.WindowStats{
		Start:          timestamppb.New(time.Unix(0, stats.StartUnixNano)),
		Kind:           paneKind(stats.Kind),
		Revision:       stats.Revision,
		Requests:       stats.Requests,
		FailedRequests: stats.Failed,
		LatencySamples: stats.LatencySamples,
		Observed:       durationpb.New(stats.Observed),
	}
	if stats.LatencySamples > 0 {
		mapped.P50 = durationpb.New(stats.P50)
		mapped.P95 = durationpb.New(stats.P95)
		mapped.P99 = durationpb.New(stats.P99)
	}
	return mapped
}

func paneKind(kind red.PaneKind) faultlinev1.PaneKind {
	switch kind {
	case red.PaneEarly:
		return faultlinev1.PaneKind_PANE_KIND_EARLY
	case red.PaneOnTime:
		return faultlinev1.PaneKind_PANE_KIND_ON_TIME
	case red.PaneLate:
		return faultlinev1.PaneKind_PANE_KIND_LATE
	}
	return faultlinev1.PaneKind_PANE_KIND_UNSPECIFIED
}

func watermark(snapshot *topology.Snapshot) *timestamppb.Timestamp {
	if !snapshot.WatermarkKnown {
		return nil
	}
	return timestamppb.New(snapshot.Watermark)
}
