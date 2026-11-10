package graph_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
)

type applyArgs struct {
	Limits     graph.Limits
	Services   []resource.ServiceRef
	Calls      []call.Call
	Unanswered []call.Unanswered
}

type applyResult struct {
	View  graph.View
	Stats graph.Stats
}

func node(name string) graph.Node {
	return graph.Node{
		ID:      graph.ServiceNodeID(telemetrytest.Service(name)),
		Service: telemetrytest.Service(name),
	}
}

func newTestGraph(t *testing.T, limits graph.Limits) *graph.Graph {
	t.Helper()
	services, err := graph.New(limits, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	return services
}

func TestNewRejectsInvalidLimits(t *testing.T) {
	tests := []struct {
		Name    string
		Args    graph.Limits
		Want    string
		WantErr error
	}{
		{
			Name: "zero max nodes",
			Args: graph.Limits{
				MaxEdges: 1,
			},
			Want:    "invalid graph limits: max nodes 0 must be in 1 … 2147483647",
			WantErr: graph.ErrInvalidLimits,
		},
		{
			Name: "zero max edges",
			Args: graph.Limits{
				MaxNodes: 1,
			},
			Want:    "invalid graph limits: max edges 0 must be in 1 … 2147483647",
			WantErr: graph.ErrInvalidLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := graph.New(tt.Args, telemetrytest.Epoch())
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGraphApply(t *testing.T) {
	frontendToCart := call.Call{
		Caller: telemetrytest.Service("frontend"),
		Callee: telemetrytest.Service("cart"),
		Failed: true,
	}
	changedAt := telemetrytest.Epoch().Add(time.Second)
	tests := []struct {
		Name    string
		Args    applyArgs
		Want    applyResult
		WantErr error
	}{
		{
			Name: "spans add nodes, calls add an edge, unanswered calls land on the caller",
			Args: applyArgs{
				Limits: graph.Limits{
					MaxNodes: 10,
					MaxEdges: 10,
				},
				Services: []resource.ServiceRef{
					telemetrytest.Service("frontend"),
					telemetrytest.Service("cart"),
				},
				Calls: []call.Call{
					frontendToCart,
				},
				Unanswered: []call.Unanswered{
					{
						Service: telemetrytest.Service("frontend"),
						Failed:  true,
					},
				},
			},
			Want: applyResult{
				View: graph.View{
					Revision:    1,
					GeneratedAt: changedAt,
					Nodes: []graph.Node{
						node("cart"),
						{
							ID:                    node("frontend").ID,
							Service:               telemetrytest.Service("frontend"),
							UnansweredCalls:       1,
							UnansweredFailedCalls: 1,
						},
					},
					Edges: []graph.Edge{
						{
							Source:      node("frontend").ID,
							Target:      node("cart").ID,
							Calls:       1,
							FailedCalls: 1,
						},
					},
				},
			},
		},
		{
			Name: "a repeated service adds one node",
			Args: applyArgs{
				Limits: graph.Limits{
					MaxNodes: 10,
					MaxEdges: 10,
				},
				Services: []resource.ServiceRef{
					telemetrytest.Service("cart"),
					telemetrytest.Service("cart"),
				},
			},
			Want: applyResult{
				View: graph.View{
					Revision:    1,
					GeneratedAt: changedAt,
					Nodes: []graph.Node{
						node("cart"),
					},
				},
			},
		},
		{
			Name: "a new edge beyond the edge limit is counted as overflow",
			Args: applyArgs{
				Limits: graph.Limits{
					MaxNodes: 10,
					MaxEdges: 1,
				},
				Services: []resource.ServiceRef{
					telemetrytest.Service("frontend"),
					telemetrytest.Service("cart"),
				},
				Calls: []call.Call{
					frontendToCart,
					{
						Caller: telemetrytest.Service("cart"),
						Callee: telemetrytest.Service("frontend"),
					},
				},
			},
			Want: applyResult{
				View: graph.View{
					Revision:    1,
					GeneratedAt: changedAt,
					Nodes: []graph.Node{
						node("cart"),
						node("frontend"),
					},
					Edges: []graph.Edge{
						{
							Source:      node("frontend").ID,
							Target:      node("cart").ID,
							Calls:       1,
							FailedCalls: 1,
						},
					},
				},
				Stats: graph.Stats{
					GraphOverflow: 1,
				},
			},
		},
		{
			Name: "everything beyond the limits is counted as overflow",
			Args: applyArgs{
				Limits: graph.Limits{
					MaxNodes: 1,
					MaxEdges: 1,
				},
				Services: []resource.ServiceRef{
					telemetrytest.Service("frontend"),
					telemetrytest.Service("cart"),
				},
				Calls: []call.Call{
					frontendToCart,
				},
				Unanswered: []call.Unanswered{
					{
						Service: telemetrytest.Service("cart"),
					},
				},
			},
			Want: applyResult{
				View: graph.View{
					Revision:    1,
					GeneratedAt: changedAt,
					Nodes: []graph.Node{
						node("frontend"),
					},
				},
				Stats: graph.Stats{
					GraphOverflow:      1,
					UnansweredOverflow: 1,
					NodeOverflow:       1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			services := newTestGraph(t, tt.Args.Limits)
			services.Apply(&graph.Change{
				ReceivedAt: changedAt,
				Services:   tt.Args.Services,
				Calls:      tt.Args.Calls,
				Unanswered: tt.Args.Unanswered,
			})
			got := applyResult{
				View:  services.CurrentView(),
				Stats: services.Stats,
			}
			if diff := cmp.Diff(tt.Want, got, cmpopts.EquateEmpty(), shapeOnly()); diff != "" {
				t.Errorf("graph mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCurrentViewRebuildsOnlyAfterChange(t *testing.T) {
	services := newTestGraph(t, graph.Limits{
		MaxNodes: 10,
		MaxEdges: 10,
	})
	frontend := []resource.ServiceRef{
		telemetrytest.Service("frontend"),
	}
	services.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch().Add(time.Second),
		Services:   frontend,
	})
	first := services.CurrentView()
	services.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch().Add(2 * time.Second),
		Services:   frontend,
	})
	if diff := cmp.Diff(first, services.CurrentView()); diff != "" {
		t.Errorf("view changed without new graph data (-first +second):\n%s", diff)
	}
}

func TestGeneratedAtNeverGoesBack(t *testing.T) {
	services := newTestGraph(t, graph.Limits{
		MaxNodes: 10,
		MaxEdges: 10,
	})
	services.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch().Add(2 * time.Second),
		Services: []resource.ServiceRef{
			telemetrytest.Service("frontend"),
		},
	})
	services.CurrentView()
	services.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch().Add(time.Second),
		Services: []resource.ServiceRef{
			telemetrytest.Service("cart"),
		},
	})
	got := services.CurrentView()
	want := graph.View{
		Revision:    2,
		GeneratedAt: telemetrytest.Epoch().Add(2 * time.Second),
		Nodes: []graph.Node{
			node("cart"),
			node("frontend"),
		},
	}
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty(), shapeOnly()); diff != "" {
		t.Errorf("view mismatch (-want +got):\n%s", diff)
	}
}

// shapeOnly compares the step-1 graph shape; handles and served counts have their own tests.
func shapeOnly() cmp.Option {
	return cmp.Options{
		cmpopts.IgnoreFields(graph.Node{}, "Handle", "Served"),
		cmpopts.IgnoreFields(graph.Edge{}, "Handle"),
	}
}
