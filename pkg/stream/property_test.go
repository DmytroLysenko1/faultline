package stream_test

import (
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const scenarios = 10_000

func TestEveryRetiredWindowEqualsTheBatchRecomputation(t *testing.T) {
	random := rand.New(rand.NewPCG(7, 11))
	for range scenarios {
		s := generate(random)
		r := runScenario(t, s, false, noFiring)
		r.apply(heartbeat(flushAfter(s.last)))
		assertMatchesOracle(t, r, recompute(s.batches))
		if t.Failed() {
			return
		}
	}
}

func assertMatchesOracle(t *testing.T, r *run, want oracle) {
	t.Helper()
	assertRetiredCellsMatch(t, r, want)
	assertLastFinalPanesMatch(t, r.recorder.finals(), want)
	stats := r.windows.Stats()
	got := counts{LateDropped: stats.LateDropped, NoWatermark: stats.NoWatermark, Reassigned: stats.Reassigned}
	if diff := cmp.Diff(want.counts, got); diff != "" {
		t.Errorf("drop and reassignment counters mismatch (-oracle +engine):\n%s", diff)
	}
	r.recorder.releaseRetired()
	if diff := cmp.Diff([2]int{0, 0}, [2]int{stats.Series, r.recorder.arena.Stats.InUse}); diff != "" {
		t.Errorf("after the flush [open series, held sketch entries] (-want +got):\n%s", diff)
	}
}

func runScenario(t *testing.T, s scenario, early bool, fire func() int) *run {
	t.Helper()
	r := newRun(t, early)
	for _, b := range s.batches {
		r.apply(b)
		assertEveryEventAccounted(t, r)
		for range fire() {
			r.windows.FireEarly(everything())
		}
	}
	return r
}

func noFiring() int {
	return 0
}

func assertEveryEventAccounted(t *testing.T, r *run) {
	t.Helper()
	stats := r.windows.Stats()
	if stats.Offered != stats.OnTime+stats.LateAccepted+stats.LateDropped+stats.NoWatermark {
		t.Fatalf("offered %d != on time %d + late accepted %d + late dropped %d + no watermark %d", stats.Offered,
			stats.OnTime, stats.LateAccepted, stats.LateDropped, stats.NoWatermark)
	}
}

func assertRetiredCellsMatch(t *testing.T, r *run, want oracle) {
	t.Helper()
	var accepted uint64
	for key, got := range r.recorder.retired {
		accepted += got.requests
		cell, ok := want.cells[key]
		if !ok || cell.requests != got.requests {
			t.Fatalf("window %+v: retired %d requests, oracle %+v", key, got.requests, cell)
		}
		reference := want.sketchOf(r.recorder.arena, key)
		matches := r.recorder.arena.Equal(&got.latency, reference)
		r.recorder.arena.Release(reference)
		if !matches {
			t.Fatalf("window %+v: latency sketch differs from the batch recomputation", key)
		}
	}
	if len(r.recorder.retired) != len(want.cells) {
		t.Fatalf("retired %d windows, oracle has %d", len(r.recorder.retired), len(want.cells))
	}
	if stats := r.windows.Stats(); accepted != stats.OnTime+stats.LateAccepted {
		t.Fatalf("retired requests %d != on time %d + late accepted %d", accepted, stats.OnTime, stats.LateAccepted)
	}
}

func assertLastFinalPanesMatch(t *testing.T, finals []finalPane, want oracle) {
	t.Helper()
	last := make(map[cellKey]uint64, len(want.cells))
	for _, pane := range finals {
		last[cellKey{key: pane.Key, window: pane.Window}] = pane.Requests
	}
	for key, cell := range want.cells {
		if got, ok := last[key]; !ok || got != cell.requests {
			t.Fatalf("window %+v: last final pane reported %d requests (emitted %v), oracle %d", key, got, ok,
				cell.requests)
		}
	}
}

func TestFinalPanesAreAFunctionOfTheJournal(t *testing.T) {
	random := rand.New(rand.NewPCG(13, 17))
	for range scenarios {
		s := generate(random)
		quiet := runScenario(t, s, false, noFiring)
		quiet.apply(heartbeat(flushAfter(s.last)))
		assertPaneLog(t, quiet.recorder)
		want := quiet.recorder.finals()
		fireSeed := random.Uint64()
		for name, got := range map[string][]finalPane{
			"with FireEarly":             finalsWithFiring(t, s, fireSeed),
			"replayed in a fresh engine": finalsWithFiring(t, s, fireSeed),
			"flushed by heartbeats":      finalsFlushedStepByStep(t, s),
		} {
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("final panes %s differ (-one jump, quiet +%s):\n%s", name, name, diff)
			}
		}
	}
}

func finalsWithFiring(t *testing.T, s scenario, seed uint64) []finalPane {
	t.Helper()
	random := rand.New(rand.NewPCG(seed, seed))
	r := runScenario(t, s, true, func() int { return random.IntN(6) })
	r.apply(heartbeat(flushAfter(s.last)))
	assertPaneLog(t, r.recorder)
	return r.recorder.finals()
}

func finalsFlushedStepByStep(t *testing.T, s scenario) []finalPane {
	t.Helper()
	r := runScenario(t, s, false, noFiring)
	for at := s.last.Add(windowSize); !at.After(flushAfter(s.last)); at = at.Add(windowSize) {
		r.apply(heartbeat(at))
	}
	assertPaneLog(t, r.recorder)
	return r.recorder.finals()
}

func assertPaneLog(t *testing.T, r *recorder) {
	t.Helper()
	revisions := make(map[cellKey]uint32)
	for i, pane := range r.log {
		key := cellKey{key: pane.Key, window: pane.Window}
		assertPane(t, pane, revisions[key])
		if pane.Kind != stream.PaneEarly {
			revisions[key] = pane.Revision
		}
		if i > 0 && pane.Kind == stream.PaneOnTime && r.batchOf[i-1] == r.batchOf[i] &&
			r.log[i-1].Kind == stream.PaneOnTime && r.log[i-1].Window > pane.Window {
			t.Fatalf("one Begin emitted ON_TIME for window %d after window %d", pane.Window, r.log[i-1].Window)
		}
	}
}

func assertPane(t *testing.T, pane finalPane, previous uint32) {
	t.Helper()
	switch {
	case pane.Kind == stream.PaneEarly && (previous != 0 || pane.Revision != 0):
		t.Fatalf("%+v: EARLY after final revision %d", pane, previous)
	case pane.Kind != stream.PaneEarly && pane.Revision != previous+1:
		t.Fatalf("%+v: revision %d follows %d", pane, pane.Revision, previous)
	case pane.Kind == stream.PaneOnTime && pane.Revision != 1:
		t.Fatalf("%+v: ON_TIME must be revision 1", pane)
	}
}

func TestGeneratorReachesEveryPlacementRule(t *testing.T) {
	random := rand.New(rand.NewPCG(7, 11))
	var total stream.Stats
	for range scenarios / 10 {
		s := generate(random)
		stats := runScenario(t, s, false, noFiring).windows.Stats()
		total.LateAccepted += stats.LateAccepted
		total.LateDropped += stats.LateDropped
		total.NoWatermark += stats.NoWatermark
		total.Reassigned += stats.Reassigned
	}
	for name, value := range map[string]uint64{
		"late accepted": total.LateAccepted, "late dropped": total.LateDropped,
		"no watermark": total.NoWatermark, "reassigned": total.Reassigned,
	} {
		if value == 0 {
			t.Errorf("the generator never produced a %s event", name)
		}
	}
}
