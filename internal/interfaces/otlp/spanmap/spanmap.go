package spanmap

import (
	"iter"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

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

	relevantAttributesHint = 2
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

func (s Source) All() iter.Seq2[resource.Resource, span.Fields] {
	return func(yield func(resource.Resource, span.Fields) bool) {
		for _, resourceSpans := range s.traces.ResourceSpans().All() {
			if !yieldResource(resourceSpans, yield) {
				return
			}
		}
	}
}

func yieldResource(
	resourceSpans ptrace.ResourceSpans,
	yield func(resource.Resource, span.Fields) bool,
) bool {
	source := resourceOf(resourceSpans.Resource().Attributes())
	for _, scopeSpans := range resourceSpans.ScopeSpans().All() {
		if !yieldScope(source, scopeSpans, yield) {
			return false
		}
	}
	return true
}

func yieldScope(
	source resource.Resource,
	scopeSpans ptrace.ScopeSpans,
	yield func(resource.Resource, span.Fields) bool,
) bool {
	for _, observed := range scopeSpans.Spans().All() {
		if !yield(source, fieldsOf(observed)) {
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

func fieldsOf(observed ptrace.Span) span.Fields {
	return span.Fields{
		TraceID:      ids.TraceID(observed.TraceID()),
		SpanID:       ids.SpanID(observed.SpanID()),
		ParentSpanID: ids.SpanID(observed.ParentSpanID()),
		Kind:         kindOf(observed.Kind()),
		StatusError:  observed.Status().Code() == ptrace.StatusCodeError,
		Attributes:   attributesOf(observed.Attributes()),
	}
}

func kindOf(spanKind ptrace.SpanKind) kind.Kind {
	if spanKind < ptrace.SpanKindUnspecified || spanKind > ptrace.SpanKindConsumer {
		return kind.Unspecified
	}
	return kind.Kind(spanKind)
}

func attributesOf(attributes pcommon.Map) response.Attributes {
	var relevant response.Attributes
	for key, value := range attributes.All() {
		text, decides := decisive(key, value)
		if !decides {
			continue
		}
		if relevant == nil {
			relevant = make(response.Attributes, relevantAttributesHint)
		}
		relevant[key] = text
	}
	return relevant
}

func decisive(key string, value pcommon.Value) (string, bool) {
	if !response.Relevant(key) {
		return "", false
	}
	return scalarText(value)
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
