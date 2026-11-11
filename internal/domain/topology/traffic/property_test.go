package traffic_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	journals        = 300
	batchesEach     = 60
	eventsEach      = 6
	operationPool   = 6
	maxStepSeconds  = 15
	eventSpread     = 120
	heartbeatChance = 8
	fireChance      = 2
	untimedChance   = 10
	servedChance    = 3
	smallSeries     = 4
)

type generator struct {
	rng *rand.Rand
}

func (g generator) chance(oneIn int) bool {
	return g.rng.IntN(oneIn) == 0
}

func (g generator) event(h *harness, receivedAt time.Time) {
	at := receivedAt.Add(time.Duration(g.rng.IntN(2*eventSpread)-eventSpread) * time.Second)
	op := httpOperation(fmt.Sprintf("GET /%d", g.rng.IntN(operationPool)))
	if g.chance(servedChance) {
		node := handle.Node(g.rng.IntN(testNodes))
		served := servedBy(h.t, fmt.Sprintf("svc-%d", node), at, op)
		h.traffic.RecordServed(node, served)
		return
	}
	edge := handle.Edge(g.rng.IntN(testEdges))
	observed := frontendToCart(at, op, g.chance(fireChance))
	observed.Callee = telemetrytest.Service(fmt.Sprintf("svc-%d", int(edge)%testNodes))
	if g.chance(untimedChance) {
		observed.Interval = eventtime.Interval{}
	}
	h.traffic.RecordCall(edge, handle.Node(int(edge)%testNodes), observed)
}

func (g generator) batch(h *harness, receivedAt time.Time) {
	if g.chance(heartbeatChance) {
		h.heartbeat(receivedAt)
		return
	}
	latest := receivedAt.Add(time.Duration(g.rng.IntN(2*eventSpread)-eventSpread) * time.Second)
	h.batch(receivedAt, latest, func() {
		for range eventsEach {
			g.event(h, receivedAt)
		}
	})
}

func accounted(stats *stream.Stats) bool {
	return stats.Offered == stats.OnTime+stats.LateAccepted+stats.LateDropped+stats.NoWatermark
}

func checkAccounting(t *testing.T, seed uint64, stats *traffic.Stats) {
	t.Helper()
	perEngine := accounted(&stats.Subjects) && accounted(&stats.Operations) && accounted(&stats.Overflow)
	split := stats.Operations.Offered+stats.Overflow.Offered == stats.Subjects.Offered &&
		stats.Operations.LateDropped == stats.Subjects.LateDropped &&
		stats.Operations.NoWatermark == stats.Subjects.NoWatermark
	if !perEngine || !split {
		t.Fatalf("seed %d: I6 per engine %v, I11 split %v: %+v", seed, perEngine, split, stats)
	}
}

func play(t *testing.T, seed uint64, limits *traffic.Limits) *harness {
	t.Helper()
	h := newHarness(t, limits)
	g := generator{rng: rand.New(rand.NewPCG(seed, seed))}
	receivedAt := base
	for range batchesEach {
		receivedAt = receivedAt.Add(time.Duration(g.rng.IntN(maxStepSeconds)) * time.Second)
		g.batch(h, receivedAt)
		if g.chance(fireChance) {
			h.traffic.Fire()
		}
		stats := h.traffic.Stats()
		checkAccounting(t, seed, &stats)
	}
	h.flush()
	return h
}

func TestRandomJournalsKeepTheAccountingAndLeaveNothingBehind(t *testing.T) {
	for seed := range uint64(journals) {
		limits := testLimits()
		if seed%2 == 0 {
			limits.MaxSeries = smallSeries
		}
		h := play(t, seed, limits)
		stats := h.traffic.Stats()
		left := [4]int{stats.Cardinality.Operations, stats.Subjects.Series + stats.Operations.Series + stats.Overflow.Series,
			stats.Sketch.InUse, len(stats.OwnersOverLimit)}
		if left != [4]int{} {
			t.Fatalf("seed %d: [operations, series, sketch entries, owners over limit] left after a flush %v", seed, left)
		}
		if diff := cmp.Diff(h.sink.finals, play(t, seed, limits).sink.finals); diff != "" {
			t.Fatalf("seed %d: a replay of the journal gave other finals (-first +replay):\n%s", seed, diff)
		}
	}
}
