package selftrace

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	QueueSize     = 512
	BatchTimeout  = 5 * time.Second
	ExportTimeout = 90 * time.Millisecond
	SpanHeapBytes = 2 << 10

	heldBatches    = 2
	serviceNameKey = "service.name"
	serviceName    = "faultline"
)

type flusher interface {
	Shutdown(ctx context.Context) error
}

type idle struct{}

func (idle) Shutdown(context.Context) error {
	return nil
}

type Tracing struct {
	provider trace.TracerProvider
	flusher  flusher
}

func Disabled() *Tracing {
	return &Tracing{
		provider: noop.NewTracerProvider(),
		flusher:  idle{},
	}
}

func New(ctx context.Context, endpoint string, logger *slog.Logger) (*Tracing, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(ExportTimeout),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled: false,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("self-tracing exporter %s: %w", endpoint, err)
	}
	return withExporter(exporter, logger), nil
}

func withExporter(exporter sdktrace.SpanExporter, logger *slog.Logger) *Tracing {
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(&loggingExporter{
			next:   exporter,
			logger: logger,
		},
			sdktrace.WithMaxQueueSize(QueueSize),
			sdktrace.WithMaxExportBatchSize(QueueSize),
			sdktrace.WithBatchTimeout(BatchTimeout),
			sdktrace.WithExportTimeout(ExportTimeout),
		),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String(serviceNameKey, serviceName))),
	)
	return &Tracing{
		provider: provider,
		flusher:  provider,
	}
}

func (t *Tracing) TracerProvider() trace.TracerProvider {
	return t.provider
}

func (t *Tracing) Shutdown(ctx context.Context) error {
	if err := t.flusher.Shutdown(ctx); err != nil {
		return fmt.Errorf("flush self-tracing: %w", err)
	}
	return nil
}

func WorstHeapBytes() int64 {
	return heldBatches * QueueSize * SpanHeapBytes
}

type loggingExporter struct {
	next   sdktrace.SpanExporter
	logger *slog.Logger
}

func (e *loggingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.next.ExportSpans(ctx, spans); err != nil {
		e.logger.Warn("self_tracing.export_failed", "spans", len(spans), "error", err)
	}
	return nil
}

func (e *loggingExporter) Shutdown(ctx context.Context) error {
	if err := e.next.Shutdown(ctx); err != nil {
		return fmt.Errorf("shut down self-tracing exporter: %w", err)
	}
	return nil
}
