//go:build !race

package tombstone_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
)

func steadySet(t *testing.T, limit int) any {
	t.Helper()
	stones, err := tombstone.New(limit)
	if err != nil {
		t.Fatalf("tombstone.New: %v", err)
	}
	for i := range uint64(telemetrytest.SteadyStateRounds * limit) {
		stones.Add(ids.NewCallKey(telemetrytest.NumberedTrace(i), ids.SpanID{
			1,
		}), tombstone.BothSides, time.Minute)
	}
	return &stones
}

func TestWorstCaseHeapCoversSteadyEviction(t *testing.T) {
	for _, limit := range []int{1_000, 100_000, 900_000, 1_000_000} {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			grown := telemetrytest.HeapGrowth(func() any {
				return steadySet(t, limit)
			})
			if model := uint64(tombstone.WorstCaseHeapBytes(limit)); grown > model {
				t.Errorf("limit %d: heap grew %d bytes, model %d", limit, grown, model)
			}
		})
	}
}
