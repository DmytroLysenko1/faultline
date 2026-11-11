package traffic_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

const allocRuns = 100

type summary struct {
	Start    time.Time
	Kind     red.PaneKind
	Revision uint32
	Requests uint64
	Observed time.Duration
}

func summaryOf(stats red.WindowStats) summary {
	if stats.IsZero() {
		return summary{}
	}
	return summary{time.Unix(0, stats.StartUnixNano), stats.Kind, stats.Revision, stats.Requests, stats.Observed}
}

func (h *harness) edgeRED() traffic.SubjectRED {
	h.t.Helper()
	edges := h.traffic.View().Edges
	if len(edges) <= int(frontendCart) {
		return traffic.SubjectRED{}
	}
	return edges[frontendCart]
}

func TestAnEarlyPaneBecomesLiveWithTheCoverageTheJournalReached(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(3*time.Second), base.Add(3*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /"), false)
	})
	h.traffic.Fire()

	changed := h.traffic.Changed()
	live := summaryOf(h.edgeRED().Live)
	afterView := h.traffic.Changed()

	want := summary{Start: base, Kind: red.PaneEarly, Requests: 1, Observed: 3 * time.Second}
	if diff := cmp.Diff(want, live); diff != "" {
		t.Errorf("live (-want +got):\n%s", diff)
	}
	if !changed || afterView {
		t.Errorf("Changed before View = %v, after = %v; want true then false", changed, afterView)
	}
}

func TestAFreshWindowIsCoveredByAtLeastOneFirePeriod(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(100*time.Millisecond), base.Add(100*time.Millisecond), func() {
		h.call(base.Add(50*time.Millisecond), httpOperation("GET /"), false)
	})
	h.traffic.Fire()

	if observed := h.edgeRED().Live.Observed; observed != earlyPeriod {
		t.Errorf("observed %s, want the fire period %s so 1 request is not 10/s", observed, earlyPeriod)
	}
}

func TestAnOlderWindowNeverReplacesTheLiveOne(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(15*time.Second), base.Add(15*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /"), false)
		h.call(base.Add(11*time.Second), httpOperation("GET /"), false)
	})
	h.traffic.Fire()
	h.batch(base.Add(16*time.Second), base.Add(16*time.Second), func() {
		h.call(base.Add(2*time.Second), httpOperation("GET /"), false)
	})
	h.traffic.Fire()

	if start := summaryOf(h.edgeRED().Live).Start; !start.Equal(base.Add(windowSize)) {
		t.Errorf("live window starts at %v, want the newer window at %v", start, base.Add(windowSize))
	}
}

func TestAWindowAheadOfTheJournalIsNotLive(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(3*time.Second), base.Add(3*time.Second), func() {
		h.call(base.Add(12*time.Second), httpOperation("GET /"), false)
	})
	h.traffic.Fire()

	if live := h.edgeRED().Live; !live.IsZero() {
		t.Errorf("live = %+v, want none: the window starts after every receive time", summaryOf(live))
	}
}

func TestAWindowAheadOfTheJournalBecomesLiveOnceTheJournalReachesIt(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(9*time.Second), base.Add(9*time.Second), func() {
		h.call(base.Add(11*time.Second), httpOperation("GET /"), false)
	})
	h.traffic.Fire()
	h.batch(base.Add(15*time.Second), base.Add(15*time.Second), func() {})
	h.traffic.Fire()

	if start := summaryOf(h.edgeRED().Live).Start; !start.Equal(base.Add(windowSize)) {
		t.Errorf("live window starts at %v, want %v once the journal reached it", start, base.Add(windowSize))
	}
}

func TestOnTimeEndsLiveAndBecomesFinal(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(3*time.Second), base.Add(3*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /"), false)
	})
	h.traffic.Fire()
	h.batch(base.Add(45*time.Second), base.Add(45*time.Second), func() {})

	subject := h.edgeRED()
	got := [2]summary{summaryOf(subject.Live), summaryOf(subject.Final)}
	want := [2]summary{{}, {Start: base, Kind: red.PaneOnTime, Revision: 1, Requests: 1, Observed: windowSize}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("[live, final] (-want +got):\n%s", diff)
	}
}

func TestALatePaneOfAnOlderWindowKeepsTheNewerFinal(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(15*time.Second), base.Add(15*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /"), false)
		h.call(base.Add(11*time.Second), httpOperation("GET /"), false)
	})
	h.batch(base.Add(60*time.Second), base.Add(60*time.Second), func() {})
	h.batch(base.Add(61*time.Second), base.Add(61*time.Second), func() {
		h.call(base.Add(5*time.Second), httpOperation("GET /"), false)
	})

	late := h.finalsOf(traffic.SeriesSubject)
	final := summaryOf(h.edgeRED().Final)
	if final.Start != base.Add(windowSize) || late[len(late)-1].Stats.Kind != red.PaneLate {
		t.Errorf("final %+v after a late pane of %v; want the newer window kept", final, late[len(late)-1].Pane.Window.Start)
	}
}

func TestFinalStaysAfterTheSeriesCloses(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /"), false)
	})
	h.flush()

	if h.traffic.Stats().Subjects.Series != 0 || h.edgeRED().Final.IsZero() {
		t.Errorf("series %d, final %+v; want the series closed and the final kept",
			h.traffic.Stats().Subjects.Series, summaryOf(h.edgeRED().Final))
	}
}

func TestAFinalWindowIsCoveredOnlyFromTheFirstAcceptedMoment(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.call(base.Add(-52*time.Second), httpOperation("GET /"), false)
	})
	h.flush()

	coverageStart := base.Add(5*time.Second - delay - lateness)
	want := base.Add(-50 * time.Second).Sub(coverageStart)
	if observed := h.finalsOf(traffic.SeriesSubject)[0].Stats.Observed; observed != want {
		t.Errorf("observed %s, want %s: no event before %v was accepted", observed, want, coverageStart)
	}
}

func journal(h *harness, firesAfter func(second int) int) {
	for second := range 30 {
		at := base.Add(time.Duration(second) * time.Second)
		h.batch(at, at, func() {
			h.call(at.Add(-time.Second), httpOperation("GET /"), second%3 == 0)
		})
		for range firesAfter(second) {
			h.traffic.Fire()
		}
	}
	h.flush()
}

func TestFinalsDependOnTheJournalAloneNotOnHowOftenItFires(t *testing.T) {
	first, second := newHarness(t, testLimits()), newHarness(t, testLimits())
	journal(first, func(int) int { return 1 })
	journal(second, func(second int) int { return second % 4 })

	if diff := cmp.Diff(first.sink.finals, second.sink.finals); diff != "" {
		t.Errorf("finals of two runs of one journal (-first +second):\n%s", diff)
	}
}

func TestRecordingAnEventIntoAnOpenWindowDoesNotAllocate(t *testing.T) {
	h := newHarness(t, testLimits())
	observed := frontendToCart(base.Add(time.Second), httpOperation("GET /"), false)
	h.begin(base.Add(5*time.Second), base.Add(5*time.Second))
	h.traffic.RecordCall(frontendCart, cartNode, observed)

	allocations := testing.AllocsPerRun(allocRuns, func() {
		h.traffic.RecordCall(frontendCart, cartNode, observed)
	})
	h.traffic.End()
	if allocations != 0 {
		t.Errorf("RecordCall allocates %.0f times per event, want 0", allocations)
	}
}
