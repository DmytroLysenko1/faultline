package graph_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	restoreScenarios = 500
	restoreChanges   = 30
	servicePool      = 7
	graphSection     = checkpoint.Tag(1)
)

var restoreLimits = graph.Limits{MaxNodes: 5, MaxEdges: 6}

func randomService(rng *rand.Rand) resource.ServiceRef {
	return telemetrytest.Service(fmt.Sprintf("svc-%d", rng.IntN(servicePool)))
}

// randomChange touches every part of Apply: new services past the node limit, calls past the edge
// limit, served and unanswered spans of known and unknown services.
func randomChange(rng *rand.Rand, at time.Time) graph.Change {
	change := graph.Change{ReceivedAt: at}
	for range rng.IntN(4) {
		change.Services = append(change.Services, randomService(rng))
	}
	for range rng.IntN(4) {
		change.Calls = append(change.Calls, call.Call{
			Caller: randomService(rng),
			Callee: randomService(rng),
			Failed: rng.IntN(3) == 0,
		})
	}
	for range rng.IntN(3) {
		change.Served = append(change.Served, randomService(rng))
	}
	for range rng.IntN(2) {
		change.Unanswered = append(change.Unanswered, call.Unanswered{Service: randomService(rng), Failed: rng.IntN(2) == 0})
	}
	return change
}

type graphRun struct {
	Applied []graph.Applied
	Views   []graph.View
	Stats   graph.Stats
}

func applyChanges(g *graph.Graph, rng *rand.Rand, from time.Time, count int) (graphRun, time.Time) {
	var observed graphRun
	at := from
	for range count {
		at = at.Add(time.Duration(rng.IntN(3000)-500) * time.Millisecond)
		applied := g.Apply(new(randomChange(rng, at)))
		observed.Applied = append(observed.Applied, graph.Applied{
			CallEdges:   append([]handle.Edge(nil), applied.CallEdges...),
			CallCallees: append([]handle.Node(nil), applied.CallCallees...),
			ServedNodes: append([]handle.Node(nil), applied.ServedNodes...),
		})
		if rng.IntN(3) == 0 {
			observed.Views = append(observed.Views, g.CurrentView())
		}
	}
	observed.Stats = g.Stats
	return observed, at
}

func graphCheckpoint(t *testing.T, g *graph.Graph) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(graphSection)
	g.WriteCheckpoint(w)
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreGraph(limits graph.Limits, data []byte) (*graph.Graph, error) {
	r := checkpoint.NewReader(bytes.NewReader(data))
	r.Begin(graphSection)
	restored, err := graph.Restore(limits, r)
	if err != nil {
		return nil, err
	}
	r.End()
	return restored, r.Finish()
}

// TestARestoredGraphContinuesLikeTheOriginal: nodes and edges keep their handles and counters and the
// view its revision, so the same changes give the same handles, views and overflow counts.
func TestARestoredGraphContinuesLikeTheOriginal(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261113, 1))
	for scenario := range restoreScenarios {
		original, err := graph.New(restoreLimits, telemetrytest.Epoch())
		if err != nil {
			t.Fatal(err)
		}
		_, last := applyChanges(original, rng, telemetrytest.Epoch(), rng.IntN(restoreChanges))
		if rng.IntN(2) == 0 {
			original.CurrentView()
		}
		data := graphCheckpoint(t, original)
		restored, err := restoreGraph(restoreLimits, data)
		if err != nil {
			t.Fatalf("scenario %d: restore: %v", scenario, err)
		}
		if !bytes.Equal(data, graphCheckpoint(t, restored)) {
			t.Fatalf("scenario %d: a restored graph writes other bytes", scenario)
		}
		seed := rng.Uint64()

		want, _ := applyChanges(original, rand.New(rand.NewPCG(seed, 2)), last, restoreChanges)
		got, _ := applyChanges(restored, rand.New(rand.NewPCG(seed, 2)), last, restoreChanges)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("scenario %d (-original +restored):\n%s", scenario, diff)
		}
	}
}

func TestAGraphCheckpointNeedsRoomForItsNodesAndEdges(t *testing.T) {
	original, err := graph.New(restoreLimits, telemetrytest.Epoch())
	if err != nil {
		t.Fatal(err)
	}
	applyChanges(original, rand.New(rand.NewPCG(5, 5)), telemetrytest.Epoch(), 3*restoreChanges)
	data := graphCheckpoint(t, original)
	tests := []struct {
		name    string
		limits  graph.Limits
		wantErr error
	}{
		{name: "fewer nodes", limits: graph.Limits{MaxNodes: 1, MaxEdges: restoreLimits.MaxEdges},
			wantErr: checkpoint.ErrCorrupt},
		{name: "fewer edges", limits: graph.Limits{MaxNodes: restoreLimits.MaxNodes, MaxEdges: 1},
			wantErr: checkpoint.ErrCorrupt},
		{name: "invalid limits", limits: graph.Limits{}, wantErr: graph.ErrInvalidLimits},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := restoreGraph(tt.limits, data)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func rawGraph(nodes []resource.ServiceRef, edges [][2]int) []byte {
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(graphSection)
	for range 3 {
		w.Uint(0)
	}
	w.Bool(true)
	w.Time(time.Time{})
	w.Bool(false)
	w.Time(time.Time{})
	w.Duration(0)
	w.Uint(0)
	w.Time(time.Time{})
	w.Count(len(nodes))
	for _, node := range nodes {
		node.WriteCheckpoint(w)
		w.Uint(0)
		w.Uint(0)
		w.Uint(0)
	}
	w.Count(len(edges))
	for _, edge := range edges {
		w.Count(edge[0])
		w.Count(edge[1])
		w.Uint(0)
		w.Uint(0)
	}
	w.End()
	return out.Bytes()
}

func TestAGraphCheckpointRefusesRepeatsAndUnknownNodes(t *testing.T) {
	a, b := telemetrytest.Service("a"), telemetrytest.Service("b")
	tests := []struct {
		name  string
		nodes []resource.ServiceRef
		edges [][2]int
	}{
		{name: "a repeated service", nodes: []resource.ServiceRef{a, a}},
		{name: "a repeated edge", nodes: []resource.ServiceRef{a, b}, edges: [][2]int{{0, 1}, {0, 1}}},
		{name: "an edge to a node that is not there", nodes: []resource.ServiceRef{a}, edges: [][2]int{{0, 1}}},
		{name: "a service without a name", nodes: []resource.ServiceRef{{Namespace: "shop"}}},
	}
	if _, err := restoreGraph(restoreLimits, rawGraph([]resource.ServiceRef{a, b}, [][2]int{{0, 1}})); err != nil {
		t.Fatalf("a valid raw graph: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := restoreGraph(restoreLimits, rawGraph(tt.nodes, tt.edges))

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}
