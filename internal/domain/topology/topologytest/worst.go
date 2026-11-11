package topologytest

import (
	"math"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
)

func WorstSummary(kind red.PaneKind) red.WindowStats {
	return red.WindowStats{
		StartUnixNano:  math.MaxInt64,
		Kind:           kind,
		Revision:       math.MaxUint32,
		Requests:       math.MaxUint64,
		Failed:         math.MaxUint64,
		LatencySamples: math.MaxUint64,
		P50:            time.Duration(math.MaxInt64),
		P95:            time.Duration(math.MaxInt64),
		P99:            time.Duration(math.MaxInt64),
		Observed:       time.Duration(math.MaxInt64),
	}
}

func WithWorstRED(snapshot *topology.Snapshot) *topology.Snapshot {
	for i := range snapshot.Nodes {
		snapshot.Nodes[i].Served = math.MaxUint64
		snapshot.Nodes[i].Live = WorstSummary(red.PaneEarly)
		snapshot.Nodes[i].Final = WorstSummary(red.PaneOnTime)
	}
	for i := range snapshot.Edges {
		snapshot.Edges[i].Live = WorstSummary(red.PaneEarly)
		snapshot.Edges[i].Final = WorstSummary(red.PaneOnTime)
	}
	snapshot.WindowSize = time.Duration(math.MaxInt64)
	snapshot.Watermark = time.Unix(0, math.MaxInt64)
	snapshot.WatermarkKnown = true
	return snapshot
}
