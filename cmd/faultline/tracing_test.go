package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
)

const tracedExports = 50

type recordedTracing struct {
	provider *sdktrace.TracerProvider
}

func (r recordedTracing) TracerProvider() trace.TracerProvider {
	return r.provider
}

func (r recordedTracing) Shutdown(ctx context.Context) error {
	return r.provider.Shutdown(ctx)
}

type recordedSpan struct {
	Name      string
	Kind      trace.SpanKind
	HasParent bool
}

func spansOf(recorder *tracetest.SpanRecorder) []recordedSpan {
	var spans []recordedSpan
	for _, span := range recorder.Ended() {
		spans = append(spans, recordedSpan{
			Name:      span.Name(),
			Kind:      span.SpanKind(),
			HasParent: span.Parent().IsValid(),
		})
	}
	return spans
}

func storedCalls(t *testing.T, sys *system) uint64 {
	t.Helper()
	snapshot, err := sys.store.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	return processingtest.TotalCalls(snapshot)
}

func TestIngestPathIsNotTraced(t *testing.T) {
	cfg := e2eConfig(t)
	recorder := tracetest.NewSpanRecorder()
	sys, err := assemble(&cfg, testLogger(&logBuffer{}), wiring{
		feedOf: journalFeed,
		tracing: recordedTracing{
			provider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)),
		},
	})
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	started := launchSystem(t, &cfg, sys)
	for exported := range uint32(tracedExports) {
		if _, err := started.exporter.Export(t.Context(), callRequest(exported)); err != nil {
			t.Fatalf("Export %d: %v", exported, err)
		}
	}
	eventually(t, "every export in the graph", func() bool {
		return storedCalls(t, sys) == tracedExports
	})
	if spans := spansOf(recorder); len(spans) != 0 {
		t.Fatalf("spans after ingest and processing = %v, want none", spans)
	}
	calls(t, started.graphs)
	if status := healthStatus(t, started.http1, started.open); status != http.StatusOK {
		t.Fatalf("healthz = %d, want %d", status, http.StatusOK)
	}
	want := []recordedSpan{
		{
			Name: "faultline.v1.GraphService/GetGraph",
			Kind: trace.SpanKindServer,
		},
	}
	if diff := cmp.Diff(want, spansOf(recorder)); diff != "" {
		t.Errorf("spans after one GetGraph and one /healthz (-want +got):\n%s", diff)
	}
	if err := started.stop(t); err != nil {
		t.Errorf("run = %v, want nil", err)
	}
}
