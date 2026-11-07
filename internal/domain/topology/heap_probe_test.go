//go:build !race

package topology_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
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
	builder, err := topology.NewBuilder(topology.Limits{
		PairingTTL:    time.Minute,
		MaxPending:    probePending,
		TombstoneTTL:  time.Minute,
		MaxTombstones: probeTombstones,
		MaxNodes:      probeNodes,
		MaxEdges:      probeEdges,
	}, telemetrytest.Epoch())
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
