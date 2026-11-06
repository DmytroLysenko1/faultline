package graph_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
)

func TestServiceNodeID(t *testing.T) {
	tests := []struct {
		Name    string
		Args    resource.ServiceRef
		Want    string
		WantErr error
	}{
		{
			Name: "plain",
			Args: resource.ServiceRef{
				Namespace: "otel-demo",
				Name:      "checkout",
			},
			Want: "svc:otel-demo/checkout",
		},
		{
			Name: "slash in namespace",
			Args: resource.ServiceRef{
				Namespace: "a/b",
				Name:      "c",
			},
			Want: "svc:a%2Fb/c",
		},
		{
			Name: "slash in name",
			Args: resource.ServiceRef{
				Namespace: "a",
				Name:      "b/c",
			},
			Want: "svc:a/b%2Fc",
		},
		{
			Name: "percent sign",
			Args: resource.ServiceRef{
				Namespace: "ns",
				Name:      "100%",
			},
			Want: "svc:ns/100%25",
		},
		{
			Name: "empty namespace",
			Args: resource.ServiceRef{
				Name: "cart",
			},
			Want: "svc:/cart",
		},
		{
			Name: "SDK fallback name",
			Args: resource.ServiceRef{
				Namespace: "ns",
				Name:      "unknown_service:java",
			},
			Want: "svc:ns/unknown_service:java",
		},
	}
	seen := make(map[string]string)
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := graph.ServiceNodeID(tt.Args)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Fatalf("id mismatch (-want +got):\n%s", diff)
			}
			if other, collides := seen[got]; collides {
				t.Fatalf("id %q collides with case %q", got, other)
			}
			seen[got] = tt.Name
			if diff := cmp.Diff(tt.Args, parseServiceNodeID(t, got)); diff != "" {
				t.Errorf("id is not reversible (-want +got):\n%s", diff)
			}
		})
	}
}

func parseServiceNodeID(t *testing.T, id string) resource.ServiceRef {
	t.Helper()
	namespace, name, found := strings.Cut(strings.TrimPrefix(id, graph.ServiceNodePrefix), "/")
	if !found {
		t.Fatalf("id %q has no separator", id)
	}
	unescapedNamespace, err := url.PathUnescape(namespace)
	if err != nil {
		t.Fatalf("unescape namespace of %q: %v", id, err)
	}
	unescapedName, err := url.PathUnescape(name)
	if err != nil {
		t.Fatalf("unescape name of %q: %v", id, err)
	}
	return resource.ServiceRef{
		Namespace: unescapedNamespace,
		Name:      unescapedName,
	}
}

func TestEdgeRecord(t *testing.T) {
	edge := graph.Edge{
		Source: "svc:shop/frontend",
		Target: "svc:shop/cart",
	}
	edge.Record(call.Call{
		Caller: telemetrytest.Service("frontend"),
		Callee: telemetrytest.Service("cart"),
	})
	edge.Record(call.Call{
		Caller: telemetrytest.Service("frontend"),
		Callee: telemetrytest.Service("cart"),
		Failed: true,
	})
	want := graph.Edge{
		Source:      "svc:shop/frontend",
		Target:      "svc:shop/cart",
		Calls:       2,
		FailedCalls: 1,
	}
	if diff := cmp.Diff(want, edge); diff != "" {
		t.Errorf("edge mismatch (-want +got):\n%s", diff)
	}
}

func TestNodeRecordUnanswered(t *testing.T) {
	node := graph.Node{
		ID:      "svc:shop/frontend",
		Service: telemetrytest.Service("frontend"),
	}
	node.RecordUnanswered(call.Unanswered{
		Service: telemetrytest.Service("frontend"),
		Failed:  true,
	})
	node.RecordUnanswered(call.Unanswered{
		Service: telemetrytest.Service("frontend"),
	})
	want := graph.Node{
		ID:                    "svc:shop/frontend",
		Service:               telemetrytest.Service("frontend"),
		UnansweredCalls:       2,
		UnansweredFailedCalls: 1,
	}
	if diff := cmp.Diff(want, node); diff != "" {
		t.Errorf("node mismatch (-want +got):\n%s", diff)
	}
}

func TestNewNode(t *testing.T) {
	service := resource.ServiceRef{
		Namespace: "a/b",
		Name:      "c",
	}
	want := graph.Node{
		ID:      "svc:a%2Fb/c",
		Service: service,
	}
	if diff := cmp.Diff(want, graph.NewNode(service)); diff != "" {
		t.Errorf("node mismatch (-want +got):\n%s", diff)
	}
}
