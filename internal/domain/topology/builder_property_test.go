package topology_test

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
)

const (
	oracleScenarios  = 2_000
	invariantSeed    = 20261110
	oracleSeed       = 20261111
	invariantRounds  = 5_000
	oracleWindowStep = 150 * time.Millisecond
)

func TestBuilderKeepsEdgeAccounting(t *testing.T) {
	rng := rand.New(rand.NewPCG(invariantSeed, invariantSeed))
	for scenario := range invariantRounds {
		pairing := topologytest.RandomLimits(rng)
		limits := topology.Limits{
			PairingTTL:    pairing.TTL,
			MaxPending:    pairing.MaxPending,
			TombstoneTTL:  pairing.TombstoneTTL,
			MaxTombstones: pairing.MaxTombstones,
			MaxNodes:      1 + rng.IntN(topologytest.ServiceCount),
			MaxEdges:      1 + rng.IntN(6),
		}
		builder := newTestBuilder(t, limits)
		previous := viewOfSnapshot(builder.Snapshot())
		var fed uint64
		for _, generated := range topologytest.RandomSteps(t, rng) {
			fed += participating(generated.Spans)
			builder.Apply(batch.Batch{
				Spans:      generated.Spans,
				ReceivedAt: telemetrytest.Epoch().Add(generated.At),
			})
			if rng.IntN(3) != 0 {
				continue
			}
			current := viewOfSnapshot(builder.Snapshot())
			checkSpanAccounting(t, scenario, &current, fed)
			checkEdgeAccounting(t, scenario, &current)
			checkRevision(t, scenario, &previous, &current)
			previous = current
		}
	}
}

func participating(spans []span.Span) uint64 {
	return uint64(lo.CountBy(spans, func(observed span.Span) bool {
		return observed.Kind == kind.Client || observed.Kind == kind.Server
	}))
}

func checkSpanAccounting(t *testing.T, scenario int, view *snapshotView, fed uint64) {
	t.Helper()
	if view.Pairing.Observed != fed {
		t.Fatalf("scenario %d: snapshot observed %d, fed %d", scenario, view.Pairing.Observed, fed)
	}
	if !view.Pairing.Balanced() {
		t.Fatalf("scenario %d: I1 broken on the snapshot: %+v", scenario, view.Pairing)
	}
}

func checkEdgeAccounting(t *testing.T, scenario int, view *snapshotView) {
	t.Helper()
	if e, found := lo.Find(view.Edges, func(e graph.Edge) bool {
		return e.FailedCalls > e.Calls
	}); found {
		t.Fatalf("scenario %d: edge %s→%s failed %d > calls %d", scenario, e.Source, e.Target, e.FailedCalls, e.Calls)
	}
	if n, found := lo.Find(view.Nodes, func(n graph.Node) bool {
		return n.UnansweredFailedCalls > n.UnansweredCalls
	}); found {
		t.Fatalf(
			"scenario %d: node %s unanswered failed %d > unanswered %d",
			scenario,
			n.ID,
			n.UnansweredFailedCalls,
			n.UnansweredCalls,
		)
	}
	calls := lo.SumBy(view.Edges, func(e graph.Edge) uint64 {
		return e.Calls
	})
	unanswered := lo.SumBy(view.Nodes, func(n graph.Node) uint64 {
		return n.UnansweredCalls
	})
	if calls != view.Pairing.Matched-view.Graph.GraphOverflow {
		t.Fatalf(
			"scenario %d: I3 broken: Σcalls %d != matched %d − overflow %d",
			scenario,
			calls,
			view.Pairing.Matched,
			view.Graph.GraphOverflow,
		)
	}
	if unanswered != view.Pairing.ExpiredCaller-view.Graph.UnansweredOverflow {
		t.Fatalf("scenario %d: I3 broken: Σunanswered %d != expired callers %d − overflow %d",
			scenario, unanswered, view.Pairing.ExpiredCaller, view.Graph.UnansweredOverflow)
	}
	ids := lo.SliceToMap(view.Nodes, func(n graph.Node) (string, bool) {
		return n.ID, true
	})
	if e, found := lo.Find(view.Edges, func(e graph.Edge) bool {
		return !ids[e.Source] || !ids[e.Target]
	}); found {
		t.Fatalf("scenario %d: edge %s→%s points outside the node set", scenario, e.Source, e.Target)
	}
	if !slices.IsSortedFunc(view.Nodes, graph.CompareNodes) || !slices.IsSortedFunc(view.Edges, graph.CompareEdges) {
		t.Fatalf("scenario %d: snapshot is not sorted", scenario)
	}
}

func checkRevision(t *testing.T, scenario int, previous, current *snapshotView) {
	t.Helper()
	sameData := cmp.Equal(previous.Nodes, current.Nodes) && cmp.Equal(previous.Edges, current.Edges)
	switch {
	case sameData && current.Revision != previous.Revision:
		t.Fatalf("scenario %d: revision moved %d → %d without a graph change", scenario, previous.Revision, current.Revision)
	case !sameData && current.Revision != previous.Revision+1:
		t.Fatalf("scenario %d: graph changed but revision went %d → %d", scenario, previous.Revision, current.Revision)
	}
}

type oracleEdge struct {
	Caller, Callee string
}

type oracleCounts struct {
	Calls, Failed uint64
}

func TestBuilderGraphMatchesOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(oracleSeed, oracleSeed))
	for scenario := range oracleScenarios {
		spans, truth := oracleScenario(t, rng)
		builder := newTestBuilder(t, builderLimits())
		for _, received := range oracleBatches(rng, spans) {
			builder.Apply(received)
		}
		got := lo.SliceToMap(builder.Snapshot().Edges, func(e graph.Edge) (oracleEdge, oracleCounts) {
			return oracleEdge{
				Caller: e.Source,
				Callee: e.Target,
			}, oracleCounts{
				Calls:  e.Calls,
				Failed: e.FailedCalls,
			}
		})
		if diff := cmp.Diff(truth, got); diff != "" {
			t.Fatalf("scenario %d: graph differs from the oracle (-want +got):\n%s", scenario, diff)
		}
	}
}

func oracleScenario(t *testing.T, rng *rand.Rand) ([]span.Span, map[oracleEdge]oracleCounts) {
	t.Helper()
	unavailable := map[string]string{
		"rpc.response.status_code": "UNAVAILABLE",
	}
	serverError := map[string]string{
		"http.response.status_code": "503",
	}
	truth := make(map[oracleEdge]oracleCounts)
	calls := 1 + rng.IntN(30)
	spans := make([]span.Span, 0, 2*calls)
	for i := range calls {
		trace := byte(i + 1)
		caller := topologytest.RandomService(rng)
		callee := topologytest.RandomService(rng)
		var callerAttributes, calleeAttributes map[string]string
		switch rng.IntN(4) {
		case 0:
			callerAttributes = unavailable
		case 1:
			calleeAttributes = serverError
		}
		spans = append(
			spans,
			telemetrytest.Client(t, trace, 1, caller, callerAttributes),
			telemetrytest.Server(t, trace, 2, callee, calleeAttributes),
		)
		key := oracleEdge{
			Caller: graph.ServiceNodeID(telemetrytest.Service(caller)),
			Callee: graph.ServiceNodeID(telemetrytest.Service(callee)),
		}
		counts := truth[key]
		counts.Calls++
		if callerAttributes != nil || calleeAttributes != nil {
			counts.Failed++
		}
		truth[key] = counts
	}
	return spans, truth
}

func oracleBatches(rng *rand.Rand, spans []span.Span) []batch.Batch {
	rng.Shuffle(len(spans), func(i, j int) {
		spans[i], spans[j] = spans[j], spans[i]
	})
	var batches []batch.Batch
	var clock time.Duration
	for len(spans) > 0 {
		size := min(len(spans), 1+rng.IntN(5))
		clock += time.Duration(rng.Int64N(int64(oracleWindowStep)))
		batches = append(batches, batch.Batch{
			Spans:      spans[:size],
			ReceivedAt: telemetrytest.Epoch().Add(clock),
		})
		spans = spans[size:]
		for rng.IntN(3) == 0 {
			retried := batches[rng.IntN(len(batches))]
			batches = append(batches, batch.Batch{
				Spans:      retried.Spans,
				ReceivedAt: telemetrytest.Epoch().Add(clock),
			})
		}
	}
	return batches
}
