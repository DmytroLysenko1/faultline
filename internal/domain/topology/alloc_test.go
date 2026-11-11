package topology_test

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	steadyPairs       = 64
	steadyBatchStep   = 250 * time.Millisecond
	steadyWarmBatches = 400
	steadyRuns        = 200
	steadyServices    = 4
	// waitingHalfAllocations is the matcher's pending list of step 1: a list element and the boxed
	// half for the first half of every pair. A pooled pending list is slice 8 work (probes).
	waitingHalfAllocations = 2
)

var steadyNames = [steadyServices]string{"frontend", "cart", "checkout", "payment"}

// steadyBatches is traffic that keeps closing windows: fresh traces on known edges and operations,
// received in order, so after the warm-up every structure has its largest size.
func steadyBatches(tb testing.TB, count int) []batch.Batch {
	tb.Helper()
	start := telemetrytest.Epoch()
	batches := make([]batch.Batch, 0, count)
	var trace uint64
	for index := range count {
		at := start.Add(time.Duration(index) * steadyBatchStep)
		spans := make([]span.Span, 0, 2*steadyPairs)
		for pair := range steadyPairs {
			trace++
			caller, callee := steadyNames[pair%steadyServices], steadyNames[(pair+1)%steadyServices]
			spans = append(spans, timedPairTB(tb, trace, caller, callee, at, pair%redFailOneIn == 0)...)
		}
		received, err := batch.New(spans, at)
		if err != nil {
			tb.Fatalf("batch.New: %v", err)
		}
		batches = append(batches, received)
	}
	return batches
}

func steadyBuilder(tb testing.TB) (*topology.Builder, []batch.Batch) {
	tb.Helper()
	builder, err := topology.NewBuilder(builderLimits(), telemetrytest.Epoch())
	if err != nil {
		tb.Fatalf("NewBuilder: %v", err)
	}
	batches := steadyBatches(tb, steadyWarmBatches+steadyRuns+1)
	for _, received := range batches[:steadyWarmBatches] {
		builder.Apply(received)
		builder.Fire()
	}
	return builder, batches[steadyWarmBatches:]
}

// TestApplyAndFireAllocateOnlyForWaitingHalves: the builder buffers, the graph and traffic add no
// allocation per event once warm, closing windows included.
func TestApplyAndFireAllocateOnlyForWaitingHalves(t *testing.T) {
	builder, batches := steadyBuilder(t)
	next := 0
	allocations := testing.AllocsPerRun(steadyRuns, func() {
		builder.Apply(batches[next])
		builder.Fire()
		next++
	})
	if want := float64(waitingHalfAllocations * steadyPairs); allocations != want {
		t.Errorf("Apply + Fire of %d pairs allocated %.1f times per batch, want %.0f", steadyPairs, allocations, want)
	}
}

// BenchmarkBuilderApplyPerSpan replays only fresh batches: an old one would go back in time and
// repeat traces, which measures the duplicate path and stops the windows.
func BenchmarkBuilderApplyPerSpan(b *testing.B) {
	builder, batches := steadyBuilder(b)
	spans, next := 0, 0
	b.ReportAllocs()
	for b.Loop() {
		if next == len(batches) {
			b.StopTimer()
			builder, batches = steadyBuilder(b)
			next = 0
			b.StartTimer()
		}
		builder.Apply(batches[next])
		spans += len(batches[next].Spans)
		next++
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(spans), "ns/span")
}
