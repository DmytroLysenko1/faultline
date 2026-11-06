package matcher_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
)

const benchPendingHalves = 100_000

func BenchmarkHeapPerPendingHalf(b *testing.B) {
	spans := lo.Times(benchPendingHalves, func(i int) span.Span {
		return telemetrytest.NumberedSpan(b, kind.Client, uint64(i), "frontend")
	})
	limits := matcher.Limits{
		TTL:           time.Minute,
		MaxPending:    benchPendingHalves,
		TombstoneTTL:  time.Minute,
		MaxTombstones: 1,
	}
	for b.Loop() {
		before := telemetrytest.HeapInUse()
		pairing, err := matcher.New(limits)
		if err != nil {
			b.Fatalf("matcher.New: %v", err)
		}
		for _, observed := range spans {
			pairing.Match(observed, telemetrytest.Epoch())
		}
		b.ReportMetric(float64(telemetrytest.HeapInUse()-before)/benchPendingHalves, "B/half")
		runtime.KeepAlive(pairing)
	}
}
