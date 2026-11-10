package spanmap

import (
	"iter"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/response"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

const (
	attrServiceName      = "service.name"
	attrServiceNamespace = "service.namespace"
	attrK8sNamespaceName = "k8s.namespace.name"
	attrK8sPodName       = "k8s.pod.name"
)

type Source struct {
	traces ptrace.Traces
}

func NewSource(traces ptrace.Traces) Source {
	return Source{
		traces: traces,
	}
}

func (s Source) Len() int {
	return s.traces.SpanCount()
}

func (s Source) All() iter.Seq2[resource.Resource, *ingest.Record] {
	return func(yield func(resource.Resource, *ingest.Record) bool) {
		var record ingest.Record
		for _, resourceSpans := range s.traces.ResourceSpans().All() {
			if !yieldResource(resourceSpans, &record, yield) {
				return
			}
		}
	}
}

func yieldResource(
	resourceSpans ptrace.ResourceSpans,
	record *ingest.Record,
	yield func(resource.Resource, *ingest.Record) bool,
) bool {
	source := resourceOf(resourceSpans.Resource().Attributes())
	for _, scopeSpans := range resourceSpans.ScopeSpans().All() {
		if !yieldScope(source, scopeSpans, record, yield) {
			return false
		}
	}
	return true
}

func yieldScope(
	source resource.Resource,
	scopeSpans ptrace.ScopeSpans,
	record *ingest.Record,
	yield func(resource.Resource, *ingest.Record) bool,
) bool {
	for _, observed := range scopeSpans.Spans().All() {
		fill(record, observed)
		if !yield(source, record) {
			return false
		}
	}
	return true
}

func resourceOf(attributes pcommon.Map) resource.Resource {
	return resource.Resource{
		ServiceName:      stringValue(attributes, attrServiceName),
		ServiceNamespace: stringValue(attributes, attrServiceNamespace),
		K8sNamespaceName: stringValue(attributes, attrK8sNamespaceName),
		K8sPodName:       stringValue(attributes, attrK8sPodName),
	}
}

func stringValue(attributes pcommon.Map, key string) string {
	if value, found := attributes.Get(key); found {
		return value.Str()
	}
	return ""
}

func fill(record *ingest.Record, observed ptrace.Span) {
	*record = ingest.Record{Fields: span.Fields{
		TraceID:      ids.TraceID(observed.TraceID()),
		SpanID:       ids.SpanID(observed.SpanID()),
		ParentSpanID: ids.SpanID(observed.ParentSpanID()),
		Kind:         kindOf(observed.Kind()),
		StatusError:  observed.Status().Code() == ptrace.StatusCodeError,
		Start:        uint64(observed.StartTimestamp()),
		End:          uint64(observed.EndTimestamp()),
	}}
	for key, value := range observed.Attributes().All() {
		collect(record, key, value)
	}
}

func collect(record *ingest.Record, key string, value pcommon.Value) {
	if value.Type() == pcommon.ValueTypeStr {
		collectText(record, key, value.Str())
		return
	}
	if !response.Relevant(key) {
		return
	}
	if text, scalar := scalarText(value); scalar {
		record.Fields.Attributes.Set(key, text)
	}
}

func collectText(record *ingest.Record, key, text string) {
	if record.Fields.Attributes.Set(key, text) {
		return
	}
	record.Facts.Set(key, text)
}

func kindOf(spanKind ptrace.SpanKind) kind.Kind {
	if spanKind < ptrace.SpanKindUnspecified || spanKind > ptrace.SpanKindConsumer {
		return kind.Unspecified
	}
	return kind.Kind(spanKind)
}

func scalarText(value pcommon.Value) (string, bool) {
	switch value.Type() {
	case pcommon.ValueTypeStr, pcommon.ValueTypeInt, pcommon.ValueTypeDouble, pcommon.ValueTypeBool:
		return value.AsString(), true
	case pcommon.ValueTypeEmpty, pcommon.ValueTypeMap, pcommon.ValueTypeSlice, pcommon.ValueTypeBytes:
		return "", false
	}
	return "", false
}
