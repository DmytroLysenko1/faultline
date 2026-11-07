package otlp_test

import (
	"errors"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

func emptySpans(count int) ptrace.Traces {
	traces := ptrace.NewTraces()
	spans := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	for range count {
		spans.AppendEmpty()
	}
	return traces
}

func TestNoAcceptedRequestCarriesMoreThanMaxSpansPerRequest(t *testing.T) {
	weigher := decodelimit.NewWeigher(decodelimit.Limits{
		MaxDecodedBytes: otlp.MaxDecodedBytes,
		MaxNestingDepth: otlp.MaxNestingDepth,
	})
	wire := otlptest.Encode(t, emptySpans(otlp.MaxSpansPerRequest+1))
	if _, err := weigher.Weigh(wire); !errors.Is(err, decodelimit.ErrTooHeavy) {
		t.Errorf("request of MaxSpansPerRequest+1 empty spans weighed with %v, want ErrTooHeavy", err)
	}
}

func TestWorstHeapIsTheInFlightBudget(t *testing.T) {
	const want = otlp.MaxConnections * otlp.MaxConcurrentStreams * otlp.MaxRecvMsgBytes * otlp.ExportHeapRatio
	if got := otlp.WorstHeapBytes(); got != want {
		t.Errorf("WorstHeapBytes = %d, want %d", got, want)
	}
}
