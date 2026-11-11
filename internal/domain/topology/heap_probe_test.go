//go:build !race

package topology_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
)

const (
	largestRequestSpans = 148_470
	probePending        = 200_000
	probeTombstones     = 1_000
	probeNodes          = 10_000
	probeEdges          = 50_000
)

func halves(t *testing.T, spanKind kind.Kind, service string) batch.Batch {
	t.Helper()
	spans := make([]span.Span, 0, largestRequestSpans)
	for index := range uint64(largestRequestSpans) {
		spans = append(spans, telemetrytest.NumberedSpan(t, spanKind, index, service))
	}
	created, err := batch.New(spans, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	return created
}

func TestWorstCaseHeapCoversApplyOfTheLargestBatch(t *testing.T) {
	limits := topologytest.Limits()
	limits.PairingTTL, limits.MaxPending = time.Minute, probePending
	limits.TombstoneTTL, limits.MaxTombstones = time.Minute, probeTombstones
	limits.MaxNodes, limits.MaxEdges = probeNodes, probeEdges
	builder, err := topology.NewBuilder(&limits, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	builder.Apply(halves(t, kind.Client, "frontend"))
	closing := halves(t, kind.Server, "checkout")
	before := int64(telemetrytest.HeapInUse())
	sampler := telemetrytest.StartPeakSampler(t)
	builder.Apply(closing)
	peak := int64(sampler.Stop())
	after := int64(telemetrytest.HeapInUse())
	runtime.KeepAlive(builder)
	runtime.KeepAlive(closing)
	if transient := peak - max(before, after); transient > topology.ApplyHeapBytes(largestRequestSpans) {
		t.Errorf("Apply held %d transient bytes above the structures, model %d", transient,
			topology.ApplyHeapBytes(largestRequestSpans))
	}
}

const (
	snapshotProbeServices = 1_000
	snapshotProbeFanOut   = 5
)

// TestSnapshotHeapIsTheViewModel: a snapshot holds one copy of every node and edge with its
// summaries and nothing more; the traffic summaries are read in place.
func TestSnapshotHeapIsTheViewModel(t *testing.T) {
	limits := topologytest.Limits()
	limits.MaxNodes, limits.MaxEdges = snapshotProbeServices, snapshotProbeServices*snapshotProbeFanOut
	limits.MaxPending = 2 * limits.MaxEdges
	builder, err := topology.NewBuilder(&limits, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	spans := make([]span.Span, 0, 2*limits.MaxEdges)
	var trace uint64
	for caller := range snapshotProbeServices {
		for step := range snapshotProbeFanOut {
			trace++
			callee := (caller + step + 1) % snapshotProbeServices
			spans = append(spans,
				telemetrytest.NumberedSpan(t, kind.Client, trace, fmt.Sprintf("service-%04d", caller)),
				telemetrytest.NumberedSpan(t, kind.Server, trace, fmt.Sprintf("service-%04d", callee)))
		}
	}
	received, err := batch.New(spans, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	builder.Apply(received)
	builder.Fire()
	builder.Snapshot()
	grown := telemetrytest.HeapGrowth(func() any {
		return builder.Snapshot()
	})
	runtime.KeepAlive(builder)
	if model := uint64(limits.ViewHeapBytes()); grown > model {
		t.Errorf("a full snapshot took %d bytes, model %d", grown, model)
	}
}
