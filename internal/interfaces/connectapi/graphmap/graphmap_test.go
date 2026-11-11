package graphmap_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphmap"
)

func node(name string, unanswered, unansweredFailed uint64) topology.NodeView {
	return topology.NodeView{Node: graph.Node{
		ID:                    graph.ServiceNodeID(telemetrytest.Service(name)),
		Service:               telemetrytest.Service(name),
		UnansweredCalls:       unanswered,
		UnansweredFailedCalls: unansweredFailed,
	}}
}

func wireNode(name string, unanswered, unansweredFailed uint64) *faultlinev1.Node {
	return &faultlinev1.Node{
		Id: graph.ServiceNodeID(telemetrytest.Service(name)),
		Service: &faultlinev1.ServiceRef{
			Namespace: telemetrytest.Namespace,
			Name:      name,
		},
		UnansweredCalls:       unanswered,
		UnansweredFailedCalls: unansweredFailed,
	}
}

func edge(caller, callee string, calls, failed uint64) topology.EdgeView {
	return topology.EdgeView{Edge: graph.Edge{
		Source:      graph.ServiceNodeID(telemetrytest.Service(caller)),
		Target:      graph.ServiceNodeID(telemetrytest.Service(callee)),
		Calls:       calls,
		FailedCalls: failed,
	}}
}

func TestGraph(t *testing.T) {
	generatedAt := telemetrytest.Epoch().Add(time.Minute)
	tests := []struct {
		Name string
		Args *topology.Snapshot
		Want *faultlinev1.Graph
	}{
		{
			Name: "empty graph keeps revision and time",
			Args: &topology.Snapshot{
				GeneratedAt: generatedAt,
			},
			Want: &faultlinev1.Graph{
				GeneratedAt: timestamppb.New(generatedAt),
			},
		},
		{
			Name: "nodes without edges carry unanswered calls",
			Args: &topology.Snapshot{
				Revision:    3,
				GeneratedAt: generatedAt,
				Nodes: []topology.NodeView{
					node("cart", 0, 0),
					node("frontend", 5, 2),
				},
			},
			Want: &faultlinev1.Graph{
				Revision:    3,
				GeneratedAt: timestamppb.New(generatedAt),
				Nodes: []*faultlinev1.Node{
					wireNode("cart", 0, 0),
					wireNode("frontend", 5, 2),
				},
			},
		},
		{
			Name: "edges point at node positions in both directions",
			Args: &topology.Snapshot{
				Revision:    9,
				GeneratedAt: generatedAt,
				Nodes: []topology.NodeView{
					node("cart", 0, 0),
					node("checkout", 0, 0),
					node("frontend", 0, 0),
				},
				Edges: []topology.EdgeView{
					edge("checkout", "cart", 4, 0),
					edge("frontend", "cart", 10, 3),
					edge("frontend", "checkout", 1, 1),
				},
			},
			Want: &faultlinev1.Graph{
				Revision:    9,
				GeneratedAt: timestamppb.New(generatedAt),
				Nodes: []*faultlinev1.Node{
					wireNode("cart", 0, 0),
					wireNode("checkout", 0, 0),
					wireNode("frontend", 0, 0),
				},
				Edges: []*faultlinev1.Edge{
					{
						SourceIndex: 1,
						TargetIndex: 0,
						Calls:       4,
					},
					{
						SourceIndex: 2,
						TargetIndex: 0,
						Calls:       10,
						FailedCalls: 3,
					},
					{
						SourceIndex: 2,
						TargetIndex: 1,
						Calls:       1,
						FailedCalls: 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := graphmap.Graph(tt.Args)
			if err != nil {
				t.Fatalf("Graph: %v", err)
			}
			if diff := cmp.Diff(tt.Want, got, protocmp.Transform()); diff != "" {
				t.Errorf("wire graph mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGraphRejectsDanglingEdge(t *testing.T) {
	tests := []struct {
		Name    string
		Args    topology.EdgeView
		Want    string
		WantErr error
	}{
		{
			Name:    "missing caller",
			Args:    edge("payment", "cart", 1, 0),
			Want:    "edge points to a missing node: svc:shop/payment → svc:shop/cart",
			WantErr: graphmap.ErrDanglingEdge,
		},
		{
			Name:    "missing callee",
			Args:    edge("cart", "payment", 1, 0),
			Want:    "edge points to a missing node: svc:shop/cart → svc:shop/payment",
			WantErr: graphmap.ErrDanglingEdge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := graphmap.Graph(&topology.Snapshot{
				Nodes: []topology.NodeView{
					node("cart", 0, 0),
				},
				Edges: []topology.EdgeView{
					tt.Args,
				},
			})
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGraphRejectsNilSnapshot(t *testing.T) {
	if _, err := graphmap.Graph(nil); !errors.Is(err, graphmap.ErrNilSnapshot) {
		t.Errorf("err = %v, want %v", err, graphmap.ErrNilSnapshot)
	}
}
