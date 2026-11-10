//go:build !race

package cardinality_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

const (
	probeOwners    = 60_000
	nameDigits     = 8
	defaultSeries  = 100_000
	afterMapGrowth = 1<<17 + 1
)

func longestName(n int) string {
	prefix := fmt.Sprintf("%0*d", nameDigits, n)
	return prefix + strings.Repeat("x", operation.MaxNameBytes-len(prefix))
}

func fullRegistry(t *testing.T, operations int) *cardinality.Registry {
	t.Helper()
	registry := newRegistry(t, perService, probeOwners)
	for n := range operations {
		op := operation.Operation{Protocol: operation.ProtocolHTTP, Name: longestName(n)}
		if _, ok := registry.Acquire(handle.Node(n/perService), op); !ok {
			t.Fatalf("Acquire(%d) refused", n)
		}
	}
	return registry
}

func TestWorstHeapCoversLiveOperationsWithTheLongestNames(t *testing.T) {
	for _, operations := range []int{perService, defaultSeries, afterMapGrowth} {
		grown := telemetrytest.HeapGrowth(func() any {
			return fullRegistry(t, operations)
		})
		if model := cardinality.WorstHeapBytes(operations, probeOwners); grown > uint64(model) {
			t.Errorf("%d operations took %d bytes, model %d", operations, grown, model)
		}
	}
}
