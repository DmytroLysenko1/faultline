package tombstone_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
)

const benchTombstones = 1_000_000

func BenchmarkHeapPerTombstone(b *testing.B) {
	for b.Loop() {
		before := telemetrytest.HeapInUse()
		stones, err := tombstone.New(benchTombstones)
		if err != nil {
			b.Fatalf("tombstone.New: %v", err)
		}
		for i := range uint64(benchTombstones) {
			stones.Add(ids.NewCallKey(telemetrytest.NumberedTrace(i), ids.SpanID{
				1,
			}), tombstone.BothSides, time.Minute)
		}
		b.ReportMetric(float64(telemetrytest.HeapInUse()-before)/benchTombstones, "B/tombstone")
		runtime.KeepAlive(&stones)
	}
}
