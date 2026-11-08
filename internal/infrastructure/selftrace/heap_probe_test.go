//go:build !race

package selftrace

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const (
	offeredBatches = 3
	probeSpanName  = "faultline.v1.GraphService/GetGraph"
)

type blockedExporter struct {
	release chan struct{}
}

func (b blockedExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	<-b.release
	return nil
}

func (blockedExporter) Shutdown(context.Context) error {
	return nil
}

func emitInterceptorSpans(ctx context.Context, tracer trace.Tracer, count int) {
	for range count {
		_, span := tracer.Start(ctx, probeSpanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "connect_rpc"),
				attribute.String("rpc.service", "faultline.v1.GraphService"),
				attribute.String("rpc.method", "GetGraph"),
			),
		)
		span.SetAttributes(attribute.String("rpc.connect_rpc.error_code", "deadline_exceeded"))
		span.SetStatus(codes.Error, "deadline_exceeded")
		span.End()
	}
}

func TestWorstCaseHeapCoversTheQueueAndTheBatchInExport(t *testing.T) {
	exporter := blockedExporter{
		release: make(chan struct{}),
	}
	var tracing *Tracing
	grown := telemetrytest.HeapGrowth(func() any {
		tracing = withExporter(exporter, slog.New(slog.DiscardHandler))
		emitInterceptorSpans(t.Context(), tracing.TracerProvider().Tracer(probeSpanName), offeredBatches*QueueSize)
		return tracing
	})
	close(exporter.release)
	if err := tracing.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	t.Logf("held %d bytes for %d offered spans", grown, offeredBatches*QueueSize)
	if model := uint64(WorstHeapBytes()); grown > model {
		t.Errorf("self-tracing held %d bytes, model %d", grown, model)
	}
}
