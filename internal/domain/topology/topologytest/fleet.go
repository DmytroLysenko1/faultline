package topologytest

import (
	"math"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const (
	fleetServicePrefix   = "service-"
	fleetOperationPrefix = "GET /operation-"
	fleetCallerSpanID    = 1
	fleetCalleeSpanID    = 2
	fastestLatency       = time.Microsecond
	slowestLatency       = 30 * time.Second
	spansPerCall         = 2
)

type Fleet struct {
	Services       int
	CalleesEach    int
	OperationsEach int
	CallsEach      int
}

func (f Fleet) Series() int {
	return f.Services * (1 + f.CalleesEach) * f.OperationsEach
}

func (f Fleet) Edges() int {
	return f.Services * f.CalleesEach
}

func (f Fleet) Round(tb testing.TB, rng *rand.Rand, at time.Time) batch.Batch {
	tb.Helper()
	calls := f.Edges() * f.CallsEach
	spans := make([]span.Span, 0, spansPerCall*calls)
	for index := range calls {
		edge := index / f.CallsEach
		caller := edge / f.CalleesEach
		callee := (caller + 1 + edge%f.CalleesEach) % f.Services
		spans = append(spans, f.call(tb, rng, caller, callee, at)...)
	}
	received, err := batch.New(spans, at)
	if err != nil {
		tb.Fatalf("batch.New: %v", err)
	}
	return received
}

func (f Fleet) call(tb testing.TB, rng *rand.Rand, caller, callee int, at time.Time) []span.Span {
	tb.Helper()
	called := operation.Operation{
		Protocol: operation.ProtocolHTTP,
		Name:     fleetOperationPrefix + strconv.Itoa(rng.IntN(f.OperationsEach)),
	}
	end := uint64(at.UnixNano())
	start := end - wideLatency(rng)
	client := span.Fields{
		TraceID: telemetrytest.NumberedTrace(rng.Uint64()),
		SpanID: ids.SpanID{
			fleetCallerSpanID,
		},
		Kind:      kind.Client,
		Start:     start,
		End:       end,
		Operation: called,
	}
	server := client
	server.SpanID = ids.SpanID{
		fleetCalleeSpanID,
	}
	server.ParentSpanID = client.SpanID
	server.Kind = kind.Server
	return []span.Span{
		telemetrytest.MustSpan(tb, fleetService(caller), &client),
		telemetrytest.MustSpan(tb, fleetService(callee), &server),
	}
}

func fleetService(index int) resource.Resource {
	return resource.Resource{
		ServiceName:      fleetServicePrefix + strconv.Itoa(index),
		K8sNamespaceName: telemetrytest.Namespace,
	}
}

func wideLatency(rng *rand.Rand) uint64 {
	logRange := math.Log(float64(slowestLatency) / float64(fastestLatency))
	return uint64(float64(fastestLatency) * math.Exp(rng.Float64()*logRange))
}
