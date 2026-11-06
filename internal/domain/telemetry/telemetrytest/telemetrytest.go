package telemetrytest

import (
	"encoding/binary"
	"runtime"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

const (
	Namespace        = "shop"
	CallerSpanID     = 1
	calleeSpanID     = 2
	epochUnixSeconds = 1_700_000_000
)

type SpanSpec struct {
	Kind       kind.Kind
	Trace      byte
	SpanID     byte
	ParentID   byte
	Service    string
	Attributes map[string]string
}

func Epoch() time.Time {
	return time.Unix(epochUnixSeconds, 0)
}

func Service(name string) resource.ServiceRef {
	return resource.ServiceRef{
		Namespace: Namespace,
		Name:      name,
	}
}

func NewSpan(tb testing.TB, spec SpanSpec) span.Span {
	tb.Helper()
	fields := span.Fields{
		TraceID: ids.TraceID{
			spec.Trace,
		},
		SpanID: ids.SpanID{
			spec.SpanID,
		},
		Kind:       spec.Kind,
		Attributes: spec.Attributes,
	}
	if spec.ParentID != 0 {
		fields.ParentSpanID = ids.SpanID{
			spec.ParentID,
		}
	}
	return MustSpan(tb, resource.Resource{
		ServiceName:      spec.Service,
		K8sNamespaceName: Namespace,
	}, fields)
}

func Client(tb testing.TB, trace, spanID byte, name string, attributes map[string]string) span.Span {
	tb.Helper()
	return NewSpan(tb, SpanSpec{
		Kind:       kind.Client,
		Trace:      trace,
		SpanID:     spanID,
		Service:    name,
		Attributes: attributes,
	})
}

func Server(tb testing.TB, trace, spanID byte, name string, attributes map[string]string) span.Span {
	tb.Helper()
	spec := SpanSpec{
		Kind:       kind.Server,
		Trace:      trace,
		SpanID:     spanID,
		ParentID:   CallerSpanID,
		Service:    name,
		Attributes: attributes,
	}
	return NewSpan(tb, spec)
}

func ValidResource() resource.Resource {
	return resource.Resource{
		ServiceName:      "checkout",
		K8sNamespaceName: "otel-demo",
	}
}

func ValidFields() span.Fields {
	return span.Fields{
		TraceID: ids.TraceID{
			1,
		},
		SpanID: ids.SpanID{
			2,
		},
		Kind: kind.Client,
	}
}

func ResourceWith(edit func(*resource.Resource)) resource.Resource {
	source := ValidResource()
	edit(&source)
	return source
}

func FieldsWith(edit func(*span.Fields)) span.Fields {
	fields := ValidFields()
	edit(&fields)
	return fields
}

func MustSpan(tb testing.TB, source resource.Resource, fields span.Fields) span.Span {
	tb.Helper()
	created, err := span.New(source, fields)
	if err != nil {
		tb.Fatalf("span.New: %v", err)
	}
	return created
}

func NumberedTrace(n uint64) ids.TraceID {
	var trace ids.TraceID
	binary.BigEndian.PutUint64(trace[:8], n+1)
	return trace
}

func NumberedSpan(tb testing.TB, spanKind kind.Kind, n uint64, serviceName string) span.Span {
	tb.Helper()
	fields := span.Fields{
		TraceID: NumberedTrace(n),
		SpanID: ids.SpanID{
			CallerSpanID,
		},
		Kind: spanKind,
	}
	if spanKind == kind.Server {
		fields.SpanID = ids.SpanID{
			calleeSpanID,
		}
		fields.ParentSpanID = ids.SpanID{
			CallerSpanID,
		}
	}
	return MustSpan(tb, resource.Resource{
		ServiceName:      serviceName,
		K8sNamespaceName: Namespace,
	}, fields)
}

func HeapInUse() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}
