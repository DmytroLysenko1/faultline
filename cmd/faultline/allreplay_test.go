package main

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
)

const (
	replayRounds     = 120
	replayRoundEvery = time.Second
	replaySeedHigh   = 4
	replaySeedLow    = 2
)

type replayOutcome struct {
	SeriesLimitReached bool
	OverflowOffered    uint64
	CollapsedAdds      uint64
	DroppedSamples     uint64
}

func TestTheAllDefaultsKeepEverySampleOfAHundredThousandSeries(t *testing.T) {
	setup, err := loadAll(processOf(processorEnvironment(t, map[string]string{})))
	if err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	fleet := topologytest.Fleet{
		Services:       500,
		CalleesEach:    4,
		OperationsEach: 40,
		CallsEach:      4,
	}
	if fleet.Series() != setup.Partition.Limits.MaxSeries {
		t.Fatalf("fleet has %d series, want MaxSeries %d", fleet.Series(), setup.Partition.Limits.MaxSeries)
	}
	started := telemetrytest.Epoch()
	builder, err := topology.NewBuilder(&setup.Partition.Limits, started)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	rng := rand.New(rand.NewPCG(replaySeedHigh, replaySeedLow))
	for round := range replayRounds {
		builder.Apply(fleet.Round(t, rng, started.Add(time.Duration(round)*replayRoundEvery)))
		builder.Fire()
	}
	stats := builder.Snapshot().TrafficStats
	want := replayOutcome{
		SeriesLimitReached: false,
		OverflowOffered:    0,
		CollapsedAdds:      0,
		DroppedSamples:     0,
	}
	got := replayOutcome{
		SeriesLimitReached: stats.SeriesLimitReached,
		OverflowOffered:    stats.Overflow.Offered,
		CollapsedAdds:      stats.Sketch.CollapsedAdds,
		DroppedSamples:     stats.Sketch.DroppedSamples,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("replay at the all defaults (-want +got):\n%s\narena %+v", diff, stats.Sketch)
	}
}
