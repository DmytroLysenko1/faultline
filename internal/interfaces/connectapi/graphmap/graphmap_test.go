package graphmap_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
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
				WindowSize:  durationpb.New(0),
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
				WindowSize:  durationpb.New(0),
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
				WindowSize:  durationpb.New(0),
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

const (
	windowSize    = 10 * time.Second
	earlyObserved = 4 * time.Second
	p50           = 3 * time.Millisecond
	p95           = 9 * time.Millisecond
	p99           = 14 * time.Millisecond
	unknownPane   = red.PaneKind(9)
)

func summary(kind red.PaneKind, start time.Time, revision uint32, samples uint64) red.WindowStats {
	stats := red.WindowStats{
		StartUnixNano:  start.UnixNano(),
		Kind:           kind,
		Revision:       revision,
		Requests:       12,
		Failed:         2,
		LatencySamples: samples,
		Observed:       windowSize,
	}
	if samples > 0 {
		stats.P50, stats.P95, stats.P99 = p50, p95, p99
	}
	return stats
}

func wireSummary(kind faultlinev1.PaneKind, start time.Time, revision uint32, samples uint64) *faultlinev1.WindowStats {
	stats := &faultlinev1.WindowStats{
		Start:          timestamppb.New(start),
		Kind:           kind,
		Revision:       revision,
		Requests:       12,
		FailedRequests: 2,
		LatencySamples: samples,
		Observed:       durationpb.New(windowSize),
	}
	if samples > 0 {
		stats.P50, stats.P95, stats.P99 = durationpb.New(p50), durationpb.New(p95), durationpb.New(p99)
	}
	return stats
}

// TestGraphCarriesTheREDOfEverySubject: a zero summary is an unset field, percentiles are set only
// with latency samples, and the watermark only once it is known.
func TestGraphCarriesTheREDOfEverySubject(t *testing.T) {
	window := telemetrytest.Epoch()
	previous := window.Add(-windowSize)
	cart := node("cart", 0, 0)
	cart.Served = 7
	cart.Live = summary(red.PaneEarly, window, 0, 12)
	cart.Live.Observed = earlyObserved
	cart.Final = summary(red.PaneOnTime, previous, 1, 0)
	call := edge("frontend", "cart", 30, 4)
	call.Final = summary(red.PaneLate, previous, 3, 12)
	call.Live = summary(red.PaneEarly, window, 0, 12)
	unknown := node("frontend", 0, 0)
	unknown.Final = summary(unknownPane, previous, 1, 0)
	tests := []struct {
		Name string
		Args *topology.Snapshot
		Want *faultlinev1.Graph
	}{
		{
			Name: "live and final of a node and an edge, served requests, window size and watermark",
			Args: &topology.Snapshot{
				WindowSize:     windowSize,
				Watermark:      previous,
				WatermarkKnown: true,
				Nodes:          []topology.NodeView{cart, unknown},
				Edges:          []topology.EdgeView{call},
			},
			Want: &faultlinev1.Graph{
				GeneratedAt: timestamppb.New(time.Time{}),
				WindowSize:  durationpb.New(windowSize),
				Watermark:   timestamppb.New(previous),
				Nodes: []*faultlinev1.Node{
					{
						Id:             cart.ID,
						Service:        &faultlinev1.ServiceRef{Namespace: telemetrytest.Namespace, Name: "cart"},
						ServedRequests: 7,
						Live: func() *faultlinev1.WindowStats {
							live := wireSummary(faultlinev1.PaneKind_PANE_KIND_EARLY, window, 0, 12)
							live.Observed = durationpb.New(earlyObserved)
							return live
						}(),
						Final: wireSummary(faultlinev1.PaneKind_PANE_KIND_ON_TIME, previous, 1, 0),
					},
					{
						Id:      unknown.ID,
						Service: &faultlinev1.ServiceRef{Namespace: telemetrytest.Namespace, Name: "frontend"},
						Final:   wireSummary(faultlinev1.PaneKind_PANE_KIND_UNSPECIFIED, previous, 1, 0),
					},
				},
				Edges: []*faultlinev1.Edge{
					{
						SourceIndex: 1,
						TargetIndex: 0,
						Calls:       30,
						FailedCalls: 4,
						Live:        wireSummary(faultlinev1.PaneKind_PANE_KIND_EARLY, window, 0, 12),
						Final:       wireSummary(faultlinev1.PaneKind_PANE_KIND_LATE, previous, 3, 12),
					},
				},
			},
		},
		{
			Name: "no summaries and an unknown watermark leave the fields unset",
			Args: &topology.Snapshot{
				WindowSize: windowSize,
				Watermark:  previous,
				Nodes:      []topology.NodeView{node("cart", 0, 0)},
			},
			Want: &faultlinev1.Graph{
				GeneratedAt: timestamppb.New(time.Time{}),
				WindowSize:  durationpb.New(windowSize),
				Nodes:       []*faultlinev1.Node{wireNode("cart", 0, 0)},
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
