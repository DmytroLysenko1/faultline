package graph_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

func services(names ...string) []resource.ServiceRef {
	refs := make([]resource.ServiceRef, 0, len(names))
	for _, name := range names {
		refs = append(refs, telemetrytest.Service(name))
	}
	return refs
}

func callOf(caller, callee string) call.Call {
	return call.Call{Caller: telemetrytest.Service(caller), Callee: telemetrytest.Service(callee)}
}

func copyOf(applied graph.Applied) graph.Applied {
	return graph.Applied{
		CallEdges:   append([]handle.Edge(nil), applied.CallEdges...),
		CallCallees: append([]handle.Node(nil), applied.CallCallees...),
		ServedNodes: append([]handle.Node(nil), applied.ServedNodes...),
	}
}

func TestApplyReportsTheSubjectsOfEveryCallAndServedSpanByIndex(t *testing.T) {
	built := newTestGraph(t, graph.Limits{MaxNodes: 3, MaxEdges: 1})
	first := copyOf(built.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch(),
		Services:   services("frontend", "cart", "checkout", "payment"),
		Calls:      []call.Call{callOf("frontend", "cart"), callOf("frontend", "checkout"), callOf("frontend", "cart")},
		Served:     services("cart", "payment", "cart"),
	}))
	second := copyOf(built.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch(),
		Calls:      []call.Call{callOf("frontend", "cart")},
	}))

	want := [2]graph.Applied{
		{
			CallEdges:   []handle.Edge{0, handle.None, 0},
			CallCallees: []handle.Node{1, handle.None, 1},
			ServedNodes: []handle.Node{1, handle.None, 1},
		},
		{CallEdges: []handle.Edge{0}, CallCallees: []handle.Node{1}},
	}
	if diff := cmp.Diff(want, [2]graph.Applied{first, second}); diff != "" {
		t.Errorf("[first Apply, second Apply] (-want +got):\n%s", diff)
	}
}

func TestHandlesAreDenseInCreationOrderAndServedRequestsAreCounted(t *testing.T) {
	built := newTestGraph(t, graph.Limits{MaxNodes: 3, MaxEdges: 2})
	built.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch(),
		Services:   services("frontend", "cart", "checkout"),
		Calls:      []call.Call{callOf("frontend", "checkout"), callOf("checkout", "cart")},
		Served:     services("cart", "cart", "checkout"),
	})
	view := built.CurrentView()

	type subject struct {
		ID     string
		Handle int32
		Served uint64
	}
	var got []subject
	for _, node := range view.Nodes {
		got = append(got, subject{node.ID, int32(node.Handle), node.Served})
	}
	for _, edge := range view.Edges {
		got = append(got, subject{edge.Source + " → " + edge.Target, int32(edge.Handle), 0})
	}
	want := []subject{
		{ID: node("cart").ID, Handle: 1, Served: 2},
		{ID: node("checkout").ID, Handle: 2, Served: 1},
		{ID: node("frontend").ID, Handle: 0},
		{ID: node("checkout").ID + " → " + node("cart").ID, Handle: 1},
		{ID: node("frontend").ID + " → " + node("checkout").ID, Handle: 0},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("subjects (-want +got):\n%s", diff)
	}
}

func TestAServedRequestAloneChangesTheGraph(t *testing.T) {
	built := newTestGraph(t, graph.Limits{MaxNodes: 1, MaxEdges: 1})
	built.Apply(&graph.Change{ReceivedAt: telemetrytest.Epoch(), Services: services("cart")})
	before := built.CurrentView().Revision

	built.Apply(&graph.Change{ReceivedAt: telemetrytest.Epoch(), Served: services("cart")})

	if after := built.CurrentView().Revision; after != before+1 {
		t.Errorf("revision %d → %d after a served request, want one step", before, after)
	}
}
