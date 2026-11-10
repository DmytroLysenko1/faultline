//go:build !race

package matcher_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
)

const (
	worstStringLimit  = 10_000
	sharedServiceName = "frontend"
)

func limitsOf(pending int) matcher.Limits {
	return matcher.Limits{
		TTL:           time.Minute,
		MaxPending:    pending,
		TombstoneTTL:  time.Minute,
		MaxTombstones: 1,
	}
}

func callerHalf(t *testing.T, index uint64, source resource.Resource) span.Span {
	t.Helper()
	return telemetrytest.MustSpan(t, source, &span.Fields{
		TraceID: telemetrytest.NumberedTrace(index),
		SpanID: ids.SpanID{
			1,
		},
		Kind: kind.Client,
	})
}

func steadyGrowth(t *testing.T, pending int, sourceOf func(index int) resource.Resource) uint64 {
	t.Helper()
	return telemetrytest.HeapGrowth(func() any {
		pairing, err := matcher.New(limitsOf(pending))
		if err != nil {
			t.Fatalf("matcher.New: %v", err)
		}
		for index := range telemetrytest.SteadyStateRounds * pending {
			half := callerHalf(t, uint64(index), sourceOf(index))
			pairing.Match(&half, telemetrytest.Epoch())
		}
		return pairing
	})
}

func TestWorstCaseHeapCoversSteadyEvictionOfPendingHalves(t *testing.T) {
	shared := resource.Resource{
		ServiceName: sharedServiceName,
	}
	for _, pending := range []int{1_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("pending %d", pending), func(t *testing.T) {
			grown := steadyGrowth(t, pending, func(int) resource.Resource {
				return shared
			})
			if model := uint64(limitsOf(pending).WorstCaseHeapBytes()); grown > model {
				t.Errorf("heap grew %d bytes, model %d", grown, model)
			}
		})
	}
}

func TestWorstCaseHeapCoversUniqueServiceStrings(t *testing.T) {
	grown := steadyGrowth(t, worstStringLimit, telemetrytest.WorstResource)
	if model := uint64(limitsOf(worstStringLimit).WorstCaseHeapBytes()); grown > model {
		t.Errorf("heap grew %d bytes with the longest service strings, model %d", grown, model)
	}
}
