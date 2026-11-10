package spanmap_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/spanmap"
)

const benchSpans = 512

type discardJournal struct{}

func (discardJournal) Append(context.Context, batch.Batch) error { return nil }

type benchClock struct{}

func (benchClock) Now() time.Time { return time.Unix(1_700_000_000, 0) }

func demoTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "frontend")
	resourceSpans.Resource().Attributes().PutStr("k8s.namespace.name", "shop")
	scope := resourceSpans.ScopeSpans().AppendEmpty()
	start := pcommon.Timestamp(1_700_000_000_000_000_000)
	for i := range benchSpans {
		observed := scope.Spans().AppendEmpty()
		observed.SetTraceID(pcommon.TraceID{1, byte(i), byte(i >> 8)})
		observed.SetSpanID(pcommon.SpanID{byte(i + 1), byte(i >> 8), 1})
		observed.SetStartTimestamp(start)
		observed.SetEndTimestamp(start + 12_000_000)
		decorate(observed, i)
	}
	return traces
}

func decorate(observed ptrace.Span, i int) {
	attributes := observed.Attributes()
	if i%2 == 0 {
		observed.SetKind(ptrace.SpanKindServer)
		attributes.PutStr("http.request.method", "GET")
		attributes.PutStr("http.route", "/api/products/{id}")
		attributes.PutStr("url.path", "/api/products/OLJCESPC7Z")
		attributes.PutInt("http.response.status_code", 200)
		attributes.PutStr("user_agent.original", "python-requests/2.31.0")
		return
	}
	observed.SetKind(ptrace.SpanKindClient)
	attributes.PutStr("rpc.system", "grpc")
	attributes.PutStr("rpc.service", "oteldemo.ProductCatalogService")
	attributes.PutStr("rpc.method", "GetProduct")
	attributes.PutInt("rpc.grpc.status_code", 0)
	attributes.PutStr("net.peer.name", "product-catalog")
}

func BenchmarkMapping512(b *testing.B) {
	source := spanmap.NewSource(demoTraces())
	b.ReportAllocs()
	for b.Loop() {
		for range source.All() {
		}
	}
}

func BenchmarkAcceptBatch512(b *testing.B) {
	source := spanmap.NewSource(demoTraces())
	accept := newAcceptBatch()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := accept.Execute(b.Context(), source); err != nil {
			b.Fatal(err)
		}
	}
}
