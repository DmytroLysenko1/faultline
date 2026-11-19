package topology_test

import (
	"bytes"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func TestTheCheckpointSectionsAreTheOnesTheBuilderWrites(t *testing.T) {
	steady, _ := steadyBuilder(t)

	err := checkpoint.Walk(bytes.NewReader(writeTB(t, steady)), topology.CheckpointSections()...)

	if err != nil {
		t.Fatalf("Walk with CheckpointSections: %v", err)
	}
}

func TestAWrittenCheckpointFitsTheWorstImageOfItsLimits(t *testing.T) {
	steady, _ := steadyBuilder(t)

	image := int64(len(writeTB(t, steady)))

	if worst := builderLimits().WorstCheckpointBytes(); image > worst {
		t.Fatalf("image of %d bytes is over the worst image %d of its limits", image, worst)
	}
}

func TestTheWorstImageGrowsWithEveryLimitItCovers(t *testing.T) {
	tests := []struct {
		name  string
		raise func(l *topology.Limits)
	}{
		{name: "pending halves", raise: func(l *topology.Limits) { l.MaxPending *= 2 }},
		{name: "tombstones", raise: func(l *topology.Limits) { l.MaxTombstones *= 2 }},
		{name: "nodes", raise: func(l *topology.Limits) { l.MaxNodes *= 2 }},
		{name: "edges", raise: func(l *topology.Limits) { l.MaxEdges *= 2 }},
		{name: "series", raise: func(l *topology.Limits) { l.MaxSeries *= 2 }},
		{name: "sketch entries", raise: func(l *topology.Limits) { l.SketchEntries *= 2 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := builderLimits()
			raised := builderLimits()
			tt.raise(raised)

			if before, after := base.WorstCheckpointBytes(), raised.WorstCheckpointBytes(); after <= before {
				t.Fatalf("worst image %d after raising %s, want more than %d", after, tt.name, before)
			}
		})
	}
}
