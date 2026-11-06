package span_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

type spanArgs struct {
	Resource resource.Resource
	Fields   span.Fields
}

func TestNewSpanRejects(t *testing.T) {
	tests := []struct {
		Name    string
		Args    spanArgs
		Want    string
		WantErr error
	}{
		{
			Name: "zero trace id",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.TraceID = ids.TraceID{}
				}),
			},
			Want:    "invalid span: trace_id: zero",
			WantErr: span.ErrInvalid,
		},
		{
			Name: "zero span id",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.SpanID = ids.SpanID{}
				}),
			},
			Want:    "invalid span: span_id: zero",
			WantErr: span.ErrInvalid,
		},
		{
			Name: "invalid resource is an invalid span",
			Args: spanArgs{
				Resource: telemetrytest.ResourceWith(func(r *resource.Resource) {
					r.ServiceName = ""
				}),
				Fields: telemetrytest.ValidFields(),
			},
			Want:    "invalid span: invalid resource: service.name: missing",
			WantErr: span.ErrInvalid,
		},
		{
			Name: "invalid resource keeps its own sentinel",
			Args: spanArgs{
				Resource: telemetrytest.ResourceWith(func(r *resource.Resource) {
					r.K8sPodName = "pod\x00"
				}),
				Fields: telemetrytest.ValidFields(),
			},
			Want:    "invalid span: invalid resource: k8s.pod.name: control character",
			WantErr: resource.ErrInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := span.New(tt.Args.Resource, tt.Args.Fields)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("reason mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewSpanAccepts(t *testing.T) {
	tests := []struct {
		Name    string
		Args    spanArgs
		Want    span.Span
		WantErr error
	}{
		{
			Name: "client copies identity and ids",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields:   telemetrytest.ValidFields(),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind: kind.Client,
			},
		},
		{
			Name: "server span without parent is a root",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.Kind = kind.Server
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind: kind.Server,
			},
		},
		{
			Name: "server span keeps its parent",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.Kind = kind.Server
					f.ParentSpanID = ids.SpanID{
						9,
					}
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				ParentSpanID: ids.SpanID{
					9,
				},
				Kind: kind.Server,
			},
		},
		{
			Name: "unknown kind is kept as unspecified",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.Kind = kind.Consumer + 1
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind: kind.Unspecified,
			},
		},
		{
			Name: "internal span is accepted",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.Kind = kind.Internal
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind: kind.Internal,
			},
		},
		{
			Name: "response attributes set Failed",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.Attributes = map[string]string{
						"http.response.status_code": "503",
					}
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind:   kind.Client,
				Failed: true,
			},
		},
		{
			Name: "status ERROR sets Failed",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.StatusError = true
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind:   kind.Client,
				Failed: true,
			},
		},
		{
			Name: "database attribute sets CallsExternalSystem",
			Args: spanArgs{
				Resource: telemetrytest.ValidResource(),
				Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
					f.Attributes = map[string]string{
						"db.system.name": "postgresql",
					}
				}),
			},
			Want: span.Span{
				Service: resource.ServiceRef{
					Namespace: "otel-demo",
					Name:      "checkout",
				},
				TraceID: ids.TraceID{
					1,
				},
				SpanID: ids.SpanID{
					2,
				},
				Kind:                kind.Client,
				CallsExternalSystem: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := span.New(tt.Args.Resource, tt.Args.Fields)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("span mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSpanIdentityStrings(t *testing.T) {
	got := telemetrytest.MustSpan(t, telemetrytest.ValidResource(), telemetrytest.ValidFields()).IdentityStrings()
	if diff := cmp.Diff([span.IdentityStringCount]string{
		"otel-demo",
		"checkout",
	}, got); diff != "" {
		t.Errorf("identity strings mismatch (-want +got):\n%s", diff)
	}
}
