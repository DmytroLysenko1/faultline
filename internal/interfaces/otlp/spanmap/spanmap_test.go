package spanmap_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/response"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/spanmap"
)

const (
	unavailableCode    = 14
	allocationRuns     = 100
	stopAfter          = 2
	hostileArrayValues = 10_000
)

type pair struct {
	Resource resource.Resource
	Fields   span.Fields
}

func collect(source spanmap.Source) []pair {
	var pairs []pair
	for observed, fields := range source.All() {
		pairs = append(pairs, pair{
			Resource: observed,
			Fields:   fields,
		})
	}
	return pairs
}

func newSpan(scope ptrace.ScopeSpans, id byte) ptrace.Span {
	created := scope.Spans().AppendEmpty()
	created.SetTraceID(pcommon.TraceID{
		id,
	})
	created.SetSpanID(pcommon.SpanID{
		id,
	})
	return created
}

func newResource(traces ptrace.Traces, service string) ptrace.ScopeSpans {
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	attributes := resourceSpans.Resource().Attributes()
	attributes.PutStr("service.name", service)
	attributes.PutStr("k8s.namespace.name", "otel-demo")
	attributes.PutStr("service.namespace", "shop")
	attributes.PutStr("k8s.pod.name", service+"-pod")
	attributes.PutStr("service.version", "ignored")
	return resourceSpans.ScopeSpans().AppendEmpty()
}

func TestSourceMapsResourceAndSpanFields(t *testing.T) {
	traces := ptrace.NewTraces()
	observed := newSpan(newResource(traces, "checkout"), 1)
	observed.SetParentSpanID(pcommon.SpanID{
		9,
	})
	observed.SetKind(ptrace.SpanKindServer)
	observed.Status().SetCode(ptrace.StatusCodeError)
	observed.Attributes().PutInt("rpc.grpc.status_code", unavailableCode)
	observed.Attributes().PutStr("http.route", "/cart")
	want := []pair{
		{
			Resource: resource.Resource{
				ServiceName:      "checkout",
				ServiceNamespace: "shop",
				K8sNamespaceName: "otel-demo",
				K8sPodName:       "checkout-pod",
			},
			Fields: span.Fields{
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					1,
				},
				ParentSpanID: ids.SpanID{
					9,
				},
				Kind:        kind.Server,
				StatusError: true,
				Attributes: response.Attributes{
					"rpc.grpc.status_code": "14",
				},
			},
		},
	}
	if diff := cmp.Diff(want, collect(spanmap.NewSource(traces))); diff != "" {
		t.Errorf("mapped spans (-want +got):\n%s", diff)
	}
}

func TestKindsOutsideOTLPBecomeUnspecified(t *testing.T) {
	tests := []struct {
		Name string
		Args ptrace.SpanKind
		Want kind.Kind
	}{
		{
			Name: "client",
			Args: ptrace.SpanKindClient,
			Want: kind.Client,
		},
		{
			Name: "consumer is the last OTLP kind",
			Args: ptrace.SpanKindConsumer,
			Want: kind.Consumer,
		},
		{
			Name: "negative kind",
			Args: -1,
			Want: kind.Unspecified,
		},
		{
			Name: "one past consumer",
			Args: ptrace.SpanKindConsumer + 1,
			Want: kind.Unspecified,
		},
		{
			Name: "value that would wrap into a valid uint8",
			Args: 1<<8 + ptrace.SpanKindClient,
			Want: kind.Unspecified,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			traces := ptrace.NewTraces()
			newSpan(newResource(traces, "cart"), 1).SetKind(tt.Args)
			got := collect(spanmap.NewSource(traces))
			if diff := cmp.Diff(tt.Want, got[0].Fields.Kind); diff != "" {
				t.Errorf("kind (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNonStringIdentityIsMissingNotStringified(t *testing.T) {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutInt("service.name", unavailableCode)
	newSpan(resourceSpans.ScopeSpans().AppendEmpty(), 1)
	got := collect(spanmap.NewSource(traces))
	if got[0].Resource.ServiceName != "" {
		t.Errorf("ServiceName = %q, want empty for a non-string attribute", got[0].Resource.ServiceName)
	}
}

func TestSpanWithoutRelevantAttributesAllocatesNoMap(t *testing.T) {
	traces := ptrace.NewTraces()
	observed := newSpan(newResource(traces, "cart"), 1)
	observed.Attributes().PutStr("http.route", "/cart")
	source := spanmap.NewSource(traces)
	got := collect(source)
	if got[0].Fields.Attributes != nil {
		t.Errorf("Attributes = %v, want nil when no attribute is relevant", got[0].Fields.Attributes)
	}
	allocations := testing.AllocsPerRun(allocationRuns, func() {
		for _, fields := range source.All() {
			if fields.Attributes != nil {
				t.Errorf("unexpected attributes %v", fields.Attributes)
			}
		}
	})
	if allocations > 1 {
		t.Errorf("walking one span allocates %.0f times, want at most the iterator closure", allocations)
	}
}

func TestSourceWalksEveryResourceAndScopeInOrder(t *testing.T) {
	traces := ptrace.NewTraces()
	firstScope := newResource(traces, "cart")
	newSpan(firstScope, 1)
	newSpan(firstScope, 2)
	secondResource := traces.ResourceSpans().AppendEmpty()
	secondResource.Resource().Attributes().PutStr("service.name", "payment")
	newSpan(secondResource.ScopeSpans().AppendEmpty(), 3)
	newSpan(secondResource.ScopeSpans().AppendEmpty(), 4)
	source := spanmap.NewSource(traces)
	got := collect(source)
	services := make([]string, 0, len(got))
	for _, mapped := range got {
		services = append(services, mapped.Resource.ServiceName)
	}
	if diff := cmp.Diff([]string{"cart", "cart", "payment", "payment"}, services); diff != "" {
		t.Errorf("services in order (-want +got):\n%s", diff)
	}
	if source.Len() != len(got) {
		t.Errorf("Len() = %d, want the %d spans the iterator yields", source.Len(), len(got))
	}
}

func TestIteratorStopsWhenYieldReturnsFalse(t *testing.T) {
	traces := ptrace.NewTraces()
	scope := newResource(traces, "cart")
	for id := range byte(5) {
		newSpan(scope, id+1)
	}
	seen := 0
	for range spanmap.NewSource(traces).All() {
		seen++
		if seen == stopAfter {
			break
		}
	}
	if seen != stopAfter {
		t.Errorf("iterator yielded %d spans after break, want %d", seen, stopAfter)
	}
}

func TestEmptyRequestYieldsNothing(t *testing.T) {
	source := spanmap.NewSource(ptrace.NewTraces())
	if diff := cmp.Diff([]pair{}, collect(source), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("empty request (-want +got):\n%s", diff)
	}
	if source.Len() != 0 {
		t.Errorf("Len() = %d, want 0", source.Len())
	}
}

func TestNonScalarRelevantAttributeIsIgnoredWithoutAllocation(t *testing.T) {
	traces := ptrace.NewTraces()
	observed := newSpan(newResource(traces, "cart"), 1)
	values := observed.Attributes().PutEmptySlice("error.type")
	for range hostileArrayValues {
		values.AppendEmpty()
	}
	observed.Attributes().PutEmptyMap("http.status_code").PutStr("nested", "503")
	observed.Attributes().PutEmptyBytes("rpc.response.status_code").FromRaw([]byte("UNAVAILABLE"))
	source := spanmap.NewSource(traces)
	if got := collect(source)[0].Fields.Attributes; got != nil {
		t.Errorf("Attributes = %v, want nil: only scalar values decide the outcome", got)
	}
	allocations := testing.AllocsPerRun(allocationRuns, func() {
		for range source.All() {
		}
	})
	if allocations > 1 {
		t.Errorf("non-scalar attributes cost %.0f allocations per span, want none beyond the iterator", allocations)
	}
}
