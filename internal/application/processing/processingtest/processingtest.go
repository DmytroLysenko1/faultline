package processingtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
)

const (
	Caller = "frontend"
	Callee = "checkout"

	spansPerPair     = 2
	pairingTTL       = 10 * time.Second
	maxPending       = 100_000
	tombstoneTTL     = time.Minute
	maxTombstones    = 1_000_000
	maxNodes         = 10_000
	maxEdges         = 50_000
	prefillServices  = 250
	prefillCallsEach = 200
	prefilledEdges   = prefillServices * prefillCallsEach
	prefilledTraces  = prefilledEdges
	servicePrefix    = "service-%03d"
)

type Delivery struct {
	batch    batch.Batch
	released atomic.Int32
}

func NewDelivery(received batch.Batch) *Delivery {
	return &Delivery{
		batch: received,
	}
}

func (d *Delivery) Batch() batch.Batch {
	return d.batch
}

func (d *Delivery) Bytes() int64 {
	return d.batch.Footprint()
}

func (d *Delivery) Release() {
	d.released.Add(1)
}

func (d *Delivery) Released() int {
	return int(d.released.Load())
}

type IterationFeed struct {
	iterations [][]processing.Delivery
}

func NewIterationFeed(iterations ...[]processing.Delivery) *IterationFeed {
	return &IterationFeed{
		iterations: iterations,
	}
}

func (f *IterationFeed) Receive(ctx context.Context, _ int, _ time.Duration) ([]processing.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("receive: %w", err)
	}
	if len(f.iterations) == 0 {
		return nil, processing.ErrFeedClosed
	}
	next := f.iterations[0]
	f.iterations = f.iterations[1:]
	return next, nil
}

func NewBuilder(tb testing.TB) *topology.Builder {
	tb.Helper()
	limits := topologytest.Limits()
	limits.PairingTTL, limits.MaxPending = pairingTTL, maxPending
	limits.TombstoneTTL, limits.MaxTombstones = tombstoneTTL, maxTombstones
	limits.MaxNodes, limits.MaxEdges = maxNodes, maxEdges
	builder, err := topology.NewBuilder(&limits, telemetrytest.Epoch())
	if err != nil {
		tb.Fatalf("NewBuilder: %v", err)
	}
	return builder
}

func Pairs(tb testing.TB, firstTrace uint64, count int) batch.Batch {
	tb.Helper()
	return pairsBetween(tb, firstTrace, count, Caller, Callee)
}

func CallerHalf(tb testing.TB, trace uint64) batch.Batch {
	tb.Helper()
	return callerHalves(tb, trace, 1)
}

func PrefilledBuilder(tb testing.TB) *topology.Builder {
	tb.Helper()
	builder := NewBuilder(tb)
	spans := make([]span.Span, 0, spansPerPair*prefilledEdges)
	for source := range prefillServices {
		for step := range prefillCallsEach {
			target := (source + step + 1) % prefillServices
			trace := uint64(source*prefillCallsEach + step)
			spans = append(spans, pair(tb, trace, serviceName(source), serviceName(target))...)
		}
	}
	builder.Apply(mustBatch(tb, spans))
	if edges := len(builder.Snapshot().Edges); edges != prefilledEdges {
		tb.Fatalf("prefilled graph has %d edges, want %d", edges, prefilledEdges)
	}
	return builder
}

func SinglePairsAfterPrefill(tb testing.TB, first uint64, count int) []batch.Batch {
	tb.Helper()
	batches := make([]batch.Batch, 0, count)
	for offset := range count {
		trace := prefilledTraces + first + uint64(offset)
		batches = append(batches, pairsBetween(tb, trace, 1, serviceName(0), serviceName(1)))
	}
	return batches
}

func CallerHalvesAfterPrefill(tb testing.TB, first uint64, count int) batch.Batch {
	tb.Helper()
	return callerHalves(tb, prefilledTraces+first, count)
}

func TotalCalls(snapshot *topology.Snapshot) uint64 {
	var total uint64
	for i := range snapshot.Edges {
		total += snapshot.Edges[i].Calls
	}
	return total
}

func pair(tb testing.TB, trace uint64, caller, callee string) []span.Span {
	tb.Helper()
	return []span.Span{
		telemetrytest.NumberedSpan(tb, kind.Client, trace, caller),
		telemetrytest.NumberedSpan(tb, kind.Server, trace, callee),
	}
}

func pairsBetween(tb testing.TB, firstTrace uint64, count int, caller, callee string) batch.Batch {
	tb.Helper()
	spans := make([]span.Span, 0, spansPerPair*count)
	for offset := range count {
		spans = append(spans, pair(tb, firstTrace+uint64(offset), caller, callee)...)
	}
	return mustBatch(tb, spans)
}

func callerHalves(tb testing.TB, firstTrace uint64, count int) batch.Batch {
	tb.Helper()
	spans := make([]span.Span, 0, count)
	for offset := range count {
		spans = append(spans, telemetrytest.NumberedSpan(tb, kind.Client, firstTrace+uint64(offset), Caller))
	}
	return mustBatch(tb, spans)
}

func serviceName(index int) string {
	return fmt.Sprintf(servicePrefix, index)
}

func mustBatch(tb testing.TB, spans []span.Span) batch.Batch {
	tb.Helper()
	created, err := batch.New(spans, telemetrytest.Epoch())
	if err != nil {
		tb.Fatalf("batch.New: %v", err)
	}
	return created
}
