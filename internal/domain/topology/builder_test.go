package topology_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
)

func builderLimits() topology.Limits {
	return topology.Limits{
		PairingTTL:    10 * time.Second,
		MaxPending:    100,
		TombstoneTTL:  60 * time.Second,
		MaxTombstones: 100,
		MaxNodes:      100,
		MaxEdges:      100,
	}
}

func newTestBuilder(t *testing.T, limits topology.Limits) *topology.Builder {
	t.Helper()
	builder, err := topology.NewBuilder(limits, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder
}

func batchAt(t *testing.T, at time.Duration, spans ...span.Span) batch.Batch {
	t.Helper()
	received, err := batch.New(spans, telemetrytest.Epoch().Add(at))
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return received
}

func node(name string) graph.Node {
	return graph.Node{
		ID:      "svc:" + telemetrytest.Namespace + "/" + name,
		Service: telemetrytest.Service(name),
	}
}

func edge(caller, callee string, calls, failed uint64) graph.Edge {
	return graph.Edge{
		Source:      "svc:" + telemetrytest.Namespace + "/" + caller,
		Target:      "svc:" + telemetrytest.Namespace + "/" + callee,
		Calls:       calls,
		FailedCalls: failed,
	}
}

type snapshotView struct {
	Revision    uint64
	GeneratedAt time.Time
	Nodes       []graph.Node
	Edges       []graph.Edge
	Pairing     matcher.Stats
	Graph       graph.Stats
}

func viewOfSnapshot(snapshot *topology.Snapshot) snapshotView {
	return snapshotView{
		Revision:    snapshot.Revision,
		GeneratedAt: snapshot.GeneratedAt,
		Nodes:       snapshot.Nodes,
		Edges:       snapshot.Edges,
		Pairing:     snapshot.PairingStats,
		Graph:       snapshot.GraphStats,
	}
}

func TestBuilderEmptySnapshot(t *testing.T) {
	got := viewOfSnapshot(newTestBuilder(t, builderLimits()).Snapshot())
	if diff := cmp.Diff(snapshotView{
		GeneratedAt: telemetrytest.Epoch(),
	}, got, shapeOnly()); diff != "" {
		t.Errorf("empty snapshot mismatch (-want +got):\n%s", diff)
	}
}

func TestBuilderBuildsServiceGraph(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, time.Second,
		telemetrytest.Client(t, 1, 1, "frontend", nil),
		telemetrytest.Server(t, 1, 2, "cart", map[string]string{
			"http.response.status_code": "503",
		}),
		telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
			Kind:    kind.Server,
			Trace:   2,
			SpanID:  1,
			Service: "frontend",
		}),
		telemetrytest.Client(t, 3, 1, "checkout", map[string]string{
			"db.system.name": "postgresql",
		}),
		telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
			Kind:    kind.Internal,
			Trace:   4,
			SpanID:  1,
			Service: "payment",
		}),
	))
	want := snapshotView{
		Revision:    1,
		GeneratedAt: telemetrytest.Epoch().Add(time.Second),
		Nodes: []graph.Node{
			node("cart"),
			node("checkout"),
			node("frontend"),
		},
		Edges: []graph.Edge{
			edge("frontend", "cart", 1, 1),
		},
		Pairing: matcher.Stats{
			Observed:   4,
			Matched:    1,
			Unpairable: 2,
		},
	}
	if diff := cmp.Diff(want, viewOfSnapshot(builder.Snapshot()), shapeOnly()); diff != "" {
		t.Errorf("snapshot mismatch (-want +got):\n%s", diff)
	}
}

func TestBuilderOrdersNodesAndEdges(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, 0,
		telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil),
		telemetrytest.Client(t, 2, 1, "checkout", nil), telemetrytest.Server(t, 2, 2, "payment", nil),
		telemetrytest.Client(t, 3, 1, "checkout", nil), telemetrytest.Server(t, 3, 2, "cart", nil),
		telemetrytest.Client(t, 4, 1, "cart", nil), telemetrytest.Server(t, 4, 2, "cart", nil),
	))
	got := viewOfSnapshot(builder.Snapshot())
	wantNodes := []graph.Node{
		node("cart"),
		node("checkout"),
		node("frontend"),
		node("payment"),
	}
	wantEdges := []graph.Edge{
		edge("cart", "cart", 1, 0),
		edge("checkout", "cart", 1, 0),
		edge("checkout", "payment", 1, 0),
		edge("frontend", "cart", 1, 0),
	}
	if diff := cmp.Diff(wantNodes, got.Nodes, shapeOnly()); diff != "" {
		t.Errorf("nodes mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantEdges, got.Edges, shapeOnly()); diff != "" {
		t.Errorf("edges mismatch (-want +got):\n%s", diff)
	}
}

func TestBuilderRevisionMovesOnlyWithGraphData(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	pair := []span.Span{
		telemetrytest.Client(t, 1, 1, "frontend", nil),
		telemetrytest.Server(t, 1, 2, "cart", nil),
	}
	type check struct {
		Revision    uint64
		GeneratedAt time.Time
		Calls       uint64
		Duplicate   uint64
	}
	observe := func() check {
		view := viewOfSnapshot(builder.Snapshot())
		calls := lo.SumBy(view.Edges, func(e graph.Edge) uint64 {
			return e.Calls
		})
		return check{
			Revision:    view.Revision,
			GeneratedAt: view.GeneratedAt,
			Calls:       calls,
			Duplicate:   view.Pairing.Duplicate,
		}
	}
	steps := []struct {
		Name string
		Args func()
		Want check
	}{
		{
			Name: "first pair creates nodes and an edge",
			Args: func() {
				builder.Apply(batchAt(t, time.Second, pair...))
			},
			Want: check{
				Revision:    1,
				GeneratedAt: telemetrytest.Epoch().Add(time.Second),
				Calls:       1,
			},
		},
		{
			Name: "retried received changes only pairing stats",
			Args: func() {
				builder.Apply(batchAt(t, 2*time.Second, pair...))
			},
			Want: check{
				Revision:    1,
				GeneratedAt: telemetrytest.Epoch().Add(time.Second),
				Calls:       1,
				Duplicate:   2,
			},
		},
		{
			Name: "a counter on an existing edge moves the revision",
			Args: func() {
				builder.Apply(batchAt(
					t,
					3*time.Second,
					telemetrytest.Client(t, 2, 1, "frontend", nil),
					telemetrytest.Server(t, 2, 2, "cart", nil),
				))
			},
			Want: check{
				Revision:    2,
				GeneratedAt: telemetrytest.Epoch().Add(3 * time.Second),
				Calls:       2,
				Duplicate:   2,
			},
		},
		{
			Name: "several batches before one snapshot make one revision",
			Args: func() {
				builder.Apply(batchAt(
					t,
					4*time.Second,
					telemetrytest.Client(t, 3, 1, "frontend", nil),
					telemetrytest.Server(t, 3, 2, "cart", nil),
				))
				builder.Apply(batchAt(
					t,
					5*time.Second,
					telemetrytest.Client(t, 4, 1, "frontend", nil),
					telemetrytest.Server(t, 4, 2, "cart", nil),
				))
			},
			Want: check{
				Revision:    3,
				GeneratedAt: telemetrytest.Epoch().Add(5 * time.Second),
				Calls:       4,
				Duplicate:   2,
			},
		},
		{
			Name: "an unanswered call moves the revision",
			Args: func() {
				builder.Apply(batchAt(t, 6*time.Second, telemetrytest.Client(t, 5, 1, "frontend", nil)))
				builder.Apply(batchAt(t, 16*time.Second, telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
					Kind:    kind.Internal,
					Trace:   6,
					SpanID:  1,
					Service: "cart",
				})))
			},
			Want: check{
				Revision:    4,
				GeneratedAt: telemetrytest.Epoch().Add(16 * time.Second),
				Calls:       4,
				Duplicate:   2,
			},
		},
		{
			Name: "a snapshot without new batches keeps everything",
			Args: func() {},
			Want: check{
				Revision:    4,
				GeneratedAt: telemetrytest.Epoch().Add(16 * time.Second),
				Calls:       4,
				Duplicate:   2,
			},
		},
	}
	for _, tt := range steps {
		tt.Args()
		if diff := cmp.Diff(tt.Want, observe(), shapeOnly()); diff != "" {
			t.Fatalf("%s (-want +got):\n%s", tt.Name, diff)
		}
	}
}

func TestSnapshotIsUnaffectedByLaterApply(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, 0, telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil)))
	published := builder.Snapshot()
	before := viewOfSnapshot(published)

	builder.Apply(batchAt(
		t,
		time.Second,
		telemetrytest.Client(t, 2, 1, "frontend", nil),
		telemetrytest.Server(t, 2, 2, "cart", nil),
	))
	builder.Apply(batchAt(
		t,
		2*time.Second,
		telemetrytest.Client(t, 3, 1, "cart", nil),
		telemetrytest.Server(t, 3, 2, "payment", nil),
	))
	builder.Apply(batchAt(t, 3*time.Second, telemetrytest.Client(t, 4, 1, "frontend", nil)))
	builder.Apply(batchAt(t, 20*time.Second, telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
		Kind:    kind.Internal,
		Trace:   5,
		SpanID:  1,
		Service: "cart",
	})))
	builder.Snapshot()

	if diff := cmp.Diff(before, viewOfSnapshot(published)); diff != "" {
		t.Errorf("published snapshot changed (-before +after):\n%s", diff)
	}
}

func TestSnapshotMutationDoesNotLeakIntoNextSnapshot(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, 0, telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil)))
	first := builder.Snapshot()
	want := viewOfSnapshot(first)
	want.Nodes = slices.Clone(want.Nodes)
	want.Edges = slices.Clone(want.Edges)
	first.Nodes[0] = graph.Node{}
	first.Edges[0].Calls = 0

	if diff := cmp.Diff(want, viewOfSnapshot(builder.Snapshot())); diff != "" {
		t.Errorf("reader mutation reached the next snapshot (-want +got):\n%s", diff)
	}
}

func TestBuilderCountsUnansweredCallsOnTheCaller(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, 0,
		telemetrytest.Client(t, 1, 1, "frontend", map[string]string{
			"rpc.response.status_code": "UNAVAILABLE",
		}),
		telemetrytest.Client(t, 2, 1, "frontend", nil),
		telemetrytest.Server(t, 3, 2, "cart", nil),
	))
	builder.Apply(batchAt(t, 10*time.Second, telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
		Kind:    kind.Internal,
		Trace:   4,
		SpanID:  1,
		Service: "cart",
	})))
	frontend := node("frontend")
	frontend.UnansweredCalls = 2
	frontend.UnansweredFailedCalls = 1
	want := snapshotView{
		Revision:    1,
		GeneratedAt: telemetrytest.Epoch().Add(10 * time.Second),
		Nodes: []graph.Node{
			node("cart"),
			frontend,
		},
		Pairing: matcher.Stats{
			Observed:      3,
			Expired:       3,
			ExpiredCaller: 2,
		},
	}
	if diff := cmp.Diff(want, viewOfSnapshot(builder.Snapshot()), cmpopts.EquateEmpty(), shapeOnly()); diff != "" {
		t.Errorf("snapshot mismatch (-want +got):\n%s", diff)
	}
}

func TestBuilderLimits(t *testing.T) {
	tests := []struct {
		Name string
		Args func(t *testing.T) (topology.Limits, []batch.Batch)
		Want snapshotView
	}{
		{
			Name: "services beyond max nodes are counted per span and their calls overflow",
			Args: func(t *testing.T) (topology.Limits, []batch.Batch) {
				limits := builderLimits()
				limits.MaxNodes = 2
				return limits, []batch.Batch{
					batchAt(t, 0, telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil)),
					batchAt(
						t,
						time.Second,
						telemetrytest.Client(t, 2, 1, "frontend", nil),
						telemetrytest.Server(t, 2, 2, "payment", nil),
						telemetrytest.Client(t, 3, 1, "payment", nil),
					),
					batchAt(t, 11*time.Second, telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
						Kind:    kind.Internal,
						Trace:   4,
						SpanID:  1,
						Service: "cart",
					})),
				}
			},
			Want: snapshotView{
				Revision:    1,
				GeneratedAt: telemetrytest.Epoch(),
				Nodes: []graph.Node{
					node("cart"),
					node("frontend"),
				},
				Edges: []graph.Edge{
					edge("frontend", "cart", 1, 0),
				},
				Pairing: matcher.Stats{
					Observed:      5,
					Matched:       2,
					Expired:       1,
					ExpiredCaller: 1,
				},
				Graph: graph.Stats{
					GraphOverflow:      1,
					UnansweredOverflow: 1,
					NodeOverflow:       2,
				},
			},
		},
		{
			Name: "a new edge between existing nodes overflows at max edges",
			Args: func(t *testing.T) (topology.Limits, []batch.Batch) {
				limits := builderLimits()
				limits.MaxEdges = 1
				return limits, []batch.Batch{
					batchAt(t, 0, telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil)),
					batchAt(
						t,
						time.Second,
						telemetrytest.Client(t, 2, 1, "cart", nil),
						telemetrytest.Server(t, 2, 2, "frontend", nil),
					),
					batchAt(
						t,
						2*time.Second,
						telemetrytest.Client(t, 3, 1, "frontend", nil),
						telemetrytest.Server(t, 3, 2, "cart", nil),
					),
				}
			},
			Want: snapshotView{
				Revision:    1,
				GeneratedAt: telemetrytest.Epoch().Add(2 * time.Second),
				Nodes: []graph.Node{
					node("cart"),
					node("frontend"),
				},
				Edges: []graph.Edge{
					edge("frontend", "cart", 2, 0),
				},
				Pairing: matcher.Stats{
					Observed: 6,
					Matched:  3,
				},
				Graph: graph.Stats{
					GraphOverflow: 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			limits, batches := tt.Args(t)
			builder := newTestBuilder(t, limits)
			for _, received := range batches {
				builder.Apply(received)
			}
			if diff := cmp.Diff(tt.Want, viewOfSnapshot(builder.Snapshot()), shapeOnly()); diff != "" {
				t.Errorf("snapshot mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewBuilderRejectsInvalidLimits(t *testing.T) {
	tests := []struct {
		Name    string
		Args    func(*topology.Limits)
		Want    string
		WantErr error
	}{
		{
			Name: "zero max nodes",
			Args: func(l *topology.Limits) {
				l.MaxNodes = 0
			},
			Want:    "invalid topology limits: invalid graph limits: max nodes 0 must be in 1 … 2147483647",
			WantErr: topology.ErrInvalidLimits,
		},
		{
			Name: "zero max edges",
			Args: func(l *topology.Limits) {
				l.MaxEdges = 0
			},
			Want:    "invalid topology limits: invalid graph limits: max edges 0 must be in 1 … 2147483647",
			WantErr: topology.ErrInvalidLimits,
		},
		{
			Name: "pairing limits are checked too",
			Args: func(l *topology.Limits) {
				l.PairingTTL = 0
			},
			Want:    "invalid topology limits: invalid matcher limits: pairing ttl 0s must be positive",
			WantErr: topology.ErrInvalidLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			limits := builderLimits()
			tt.Args(&limits)
			_, err := topology.NewBuilder(limits, telemetrytest.Epoch())
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// shapeOnly compares the step-1 graph shape; handles and served counts have their own tests.
func shapeOnly() cmp.Option {
	return cmp.Options{
		cmpopts.IgnoreFields(graph.Node{}, "Handle", "Served"),
		cmpopts.IgnoreFields(graph.Edge{}, "Handle"),
	}
}
