package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const quickFirePeriod = 250 * time.Millisecond

// TestPipelineCadenceSharesTheEarlyFirePeriodAndLeavesRoomForTheLargestRequest: one setting drives
// both the Fire cadence and the early-pane coverage, and the held batches stop one largest
// request short of a full journal.
func TestPipelineCadenceSharesTheEarlyFirePeriodAndLeavesRoomForTheLargestRequest(t *testing.T) {
	tests := []struct {
		Name string
		Args Config
		Want processing.Cadence
	}{
		{
			Name: "defaults",
			Args: defaultConfig(),
			Want: processing.Cadence{
				FirePeriod:   defaultEarlyFirePeriod,
				MaxHeldBytes: defaultJournalMaxBytes - otlp.MaxBatchFootprint(),
			},
		},
		{
			Name: "the smallest journal",
			Args: configWith(func(cfg *Config) {
				cfg.Journal.MaxBytes = minJournalMaxBytes
				cfg.Topology.EarlyFirePeriod = quickFirePeriod
			}),
			Want: processing.Cadence{FirePeriod: quickFirePeriod, MaxHeldBytes: minJournalMaxBytes - otlp.MaxBatchFootprint()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := pipelineCadence(&tt.Args)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("cadence (-want +got):\n%s", diff)
			}
			if err := got.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}
