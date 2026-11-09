package stream_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

func at(offset time.Duration) time.Time {
	return epoch.Add(offset)
}

func singleEvent(receivedAt, eventAt time.Time) batch {
	return batch{receivedAt: receivedAt, events: []event{{key: 1, at: eventAt}}}
}

type outcome struct {
	Placement stream.Placement
	Window    int64
	Counts    eventCounts
}

type eventCounts struct {
	Offered, OnTime, LateAccepted, LateDropped, NoWatermark, Reassigned uint64
}

func countsOf(windows *stream.Windows[int, cell]) eventCounts {
	stats := windows.Stats()
	return eventCounts{Offered: stats.Offered, OnTime: stats.OnTime, LateAccepted: stats.LateAccepted,
		LateDropped: stats.LateDropped, NoWatermark: stats.NoWatermark, Reassigned: stats.Reassigned}
}

func windowAt(offset time.Duration) int64 {
	return config.WindowOf(at(offset)).ID
}

func TestClassifyAndLocateApplyTheSamePlacementRules(t *testing.T) {
	mark := stream.Mark{Time: at(time.Minute), Known: true}
	anchor := at(time.Minute + delay)
	type args struct {
		mark stream.Mark
		t    time.Time
	}
	tests := []struct {
		name string
		args args
		want outcome
	}{
		{name: "no watermark yet", args: args{mark: stream.Mark{}, t: at(0)},
			want: outcome{Placement: stream.PlacementDroppedNoWatermark, Window: -1,
				Counts: eventCounts{Offered: 1, NoWatermark: 1}}},
		{name: "one nanosecond beyond allowed lateness", args: args{mark: mark, t: at(30*time.Second - 1)},
			want: outcome{Placement: stream.PlacementDroppedLate, Window: -1,
				Counts: eventCounts{Offered: 1, LateDropped: 1}}},
		{name: "exactly allowed lateness", args: args{mark: mark, t: at(30 * time.Second)},
			want: outcome{Placement: stream.PlacementLate, Window: windowAt(30 * time.Second),
				Counts: eventCounts{Offered: 1, LateAccepted: 1}}},
		{name: "last nanosecond of the window that ends at the watermark", args: args{mark: mark, t: at(time.Minute - 1)},
			want: outcome{Placement: stream.PlacementLate, Window: windowAt(50 * time.Second),
				Counts: eventCounts{Offered: 1, LateAccepted: 1}}},
		{name: "exactly at the watermark", args: args{mark: mark, t: at(time.Minute)},
			want: outcome{Placement: stream.PlacementOnTime, Window: windowAt(time.Minute),
				Counts: eventCounts{Offered: 1, OnTime: 1}}},
		{name: "last window of the horizon", args: args{mark: mark, t: at(110 * time.Second)},
			want: outcome{Placement: stream.PlacementOnTime, Window: windowAt(110 * time.Second),
				Counts: eventCounts{Offered: 1, OnTime: 1}}},
		{name: "first window past the horizon goes to the anchor", args: args{mark: mark, t: at(120 * time.Second)},
			want: outcome{Placement: stream.PlacementOnTime, Window: config.WindowOf(anchor).ID,
				Counts: eventCounts{Offered: 1, OnTime: 1, Reassigned: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := placeOnce(t, tt.args.mark, anchor, tt.args.t)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("placement mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func placeOnce(t *testing.T, mark stream.Mark, anchor, eventAt time.Time) outcome {
	t.Helper()
	r := newRun(t, false)
	r.windows.Begin(mark, anchor)
	classified := r.windows.Classify(eventAt)
	c, placement, _ := r.windows.Locate(1, eventAt)
	if c != nil {
		c.requests++
	}
	r.windows.End()
	got := outcome{Placement: placement, Window: -1, Counts: countsOf(r.windows)}
	farAhead := at(24 * time.Hour)
	r.windows.Begin(stream.Mark{Time: farAhead, Known: true}, farAhead)
	r.windows.End()
	for key := range r.recorder.retired {
		got.Window = key.window
	}
	if classified != placement {
		t.Fatalf("Classify = %v, Locate = %v", classified, placement)
	}
	return got
}

func TestWindowOfRoundsDownAndPutsTheEndIntoTheNextWindow(t *testing.T) {
	first := windowAt(0)
	tests := []struct {
		name string
		args time.Time
		want int64
	}{
		{name: "window start", args: at(0), want: first},
		{name: "one nanosecond before the end", args: at(windowSize - 1), want: first},
		{name: "exactly on the end", args: at(windowSize), want: first + 1},
		{name: "before 1970 rounds down", args: time.Unix(0, -1), want: -1},
		{name: "1970 epoch", args: time.Unix(0, 0), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, config.WindowOf(tt.args).ID); diff != "" {
				t.Errorf("window id mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWindowBecomesFinalExactlyWhenTheWatermarkReachesItsEnd(t *testing.T) {
	r := newRun(t, false)
	r.apply(singleEvent(at(delay), at(0)))
	end := config.WindowOf(at(0)).End

	r.apply(heartbeat(end.Add(delay - 1)))
	beforeEnd := len(r.recorder.finals())
	r.apply(heartbeat(end.Add(delay)))

	if diff := cmp.Diff([2]int{0, 1}, [2]int{beforeEnd, len(r.recorder.finals())}); diff != "" {
		t.Errorf("ON_TIME panes [1 ns before the end, at the end] (-want +got):\n%s", diff)
	}
}

func TestWindowRetiresExactlyAtEndPlusAllowedLateness(t *testing.T) {
	r := newRun(t, false)
	r.apply(singleEvent(at(delay), at(0)))
	retireAt := config.WindowOf(at(0)).End.Add(lateness + delay)

	r.apply(heartbeat(retireAt.Add(-1)))
	stillOpen := r.windows.Has(1)
	r.apply(heartbeat(retireAt))

	want := []bool{true, false}
	if diff := cmp.Diff(want, []bool{stillOpen, r.windows.Has(1)}); diff != "" {
		t.Errorf("series open [1 ns before retirement, at retirement] (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]int{1}, r.recorder.closed); diff != "" {
		t.Errorf("Closed calls mismatch (-want +got):\n%s", diff)
	}
}

func TestAcceptanceFRSTR002(t *testing.T) {
	r := newRun(t, false)
	window := config.WindowOf(time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC))
	eventAt := window.Start.Add(5 * time.Second)
	watermarkAt := func(offset time.Duration) time.Time { return window.Start.Add(offset + delay) }
	r.apply(singleEvent(eventAt, eventAt))

	r.apply(batch{receivedAt: watermarkAt(30 * time.Second), events: []event{
		{key: 2, at: watermarkAt(30 * time.Second)}, {key: 1, at: eventAt},
	}})
	r.apply(batch{receivedAt: watermarkAt(50 * time.Second), events: []event{
		{key: 2, at: watermarkAt(50 * time.Second)}, {key: 1, at: eventAt},
	}})
	r.apply(singleEvent(watermarkAt(50*time.Second), watermarkAt(50*time.Second).Add(time.Hour)))

	want := []finalPane{
		{Key: 1, Window: window.ID, Kind: stream.PaneOnTime, Revision: 1, Requests: 1},
		{Key: 1, Window: window.ID, Kind: stream.PaneLate, Revision: 2, Requests: 2},
	}
	if diff := cmp.Diff(want, panesOf(r.recorder.finals(), 1, window.ID)); diff != "" {
		t.Errorf("panes of window [12:00:00, 12:00:10) mismatch (-want +got):\n%s", diff)
	}
	wantStats := stream.Stats{Series: 2, Capacity: capacity, Offered: 6, OnTime: 4, LateAccepted: 1, LateDropped: 1,
		Reassigned: 1, PanesOnTime: 1, PanesLate: 1, Retired: 1}
	if diff := cmp.Diff(wantStats, r.windows.Stats()); diff != "" {
		t.Errorf("stats mismatch (-want +got):\n%s", diff)
	}
}

func panesOf(finals []finalPane, key int, window int64) []finalPane {
	selected := make([]finalPane, 0, len(finals))
	for _, pane := range finals {
		if pane.Key == key && pane.Window == window {
			selected = append(selected, pane)
		}
	}
	return selected
}

func TestUntimedSpansBeforeTheFirstWatermarkAreCountedNotPlaced(t *testing.T) {
	restart := time.Date(2026, 11, 9, 12, 10, 0, 0, time.UTC)
	untimed := func(receivedAt time.Time) batch {
		return batch{receivedAt: receivedAt, events: []event{{key: 9, untimed: true}}}
	}
	type want struct {
		Counts eventCounts
		Series []bool
	}
	tests := []struct {
		name string
		args []batch
		want want
	}{
		{name: "untimed batches 90 s apart", args: []batch{untimed(restart), untimed(restart.Add(90 * time.Second))},
			want: want{Counts: eventCounts{Offered: 2, NoWatermark: 2}, Series: []bool{false, false}}},
		{name: "untimed batch, then a Collector backlog within lateness", args: []batch{untimed(restart), {
			receivedAt: restart.Add(time.Second),
			events: []event{
				{key: 1, at: restart.Add(-2 * time.Minute)},
				{key: 2, at: restart.Add(-2*time.Minute - 55*time.Second)},
			},
		}}, want: want{Counts: eventCounts{Offered: 3, OnTime: 1, LateAccepted: 1, NoWatermark: 1},
			Series: []bool{false, true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRun(t, false)
			for _, b := range tt.args {
				r.apply(b)
			}
			got := want{Counts: countsOf(r.windows), Series: []bool{r.windows.Has(9), r.windows.Has(2)}}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestOneWatermarkJumpEmitsOnTimeInWindowOrderAcrossTheRingWrap(t *testing.T) {
	r := newRun(t, false)
	ring := int64(config.Slots())
	base := windowAt(0)
	for base%ring != ring-2 {
		base++
	}
	start := config.WindowOf(at(0)).Start.Add(time.Duration(base-windowAt(0)) * windowSize)
	events := make([]event, 0, 4)
	for i := range 4 {
		events = append(events, event{key: 1, at: start.Add(time.Duration(3-i) * windowSize)})
	}
	r.apply(batch{receivedAt: start.Add(4 * windowSize), events: events})

	r.apply(heartbeat(flushAfter(start.Add(4 * windowSize))))

	want := []int64{base, base + 1, base + 2, base + 3}
	got := make([]int64, 0, len(r.recorder.finals()))
	for _, pane := range r.recorder.finals() {
		got = append(got, pane.Window)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ON_TIME window order across the ring wrap (-want +got):\n%s", diff)
	}
}

func TestUntimedEventAfterABacklogBatchIsPlacedAtTheLatestCandidate(t *testing.T) {
	r := newRun(t, false)
	backlogAt := at(10 * time.Minute)
	r.apply(singleEvent(backlogAt, backlogAt.Add(-2*time.Minute)))

	r.apply(batch{receivedAt: backlogAt.Add(time.Second), events: []event{{key: 2, untimed: true}}})
	r.apply(heartbeat(flushAfter(backlogAt)))

	want := map[cellKey]uint64{
		{key: 1, window: config.WindowOf(backlogAt.Add(-2 * time.Minute)).ID}: 1,
		{key: 2, window: config.WindowOf(backlogAt.Add(-2 * time.Minute)).ID}: 1,
	}
	if diff := cmp.Diff(want, retiredRequests(r)); diff != "" {
		t.Errorf("retired requests mismatch (-want +got):\n%s", diff)
	}
}

func retiredRequests(r *run) map[cellKey]uint64 {
	got := make(map[cellKey]uint64, len(r.recorder.retired))
	for key, cell := range r.recorder.retired {
		got[key] = cell.requests
	}
	return got
}

func TestEventFromAFastClockIsReassignedToTheBatchAnchor(t *testing.T) {
	r := newRun(t, false)
	receivedAt := at(time.Minute)

	r.apply(batch{receivedAt: receivedAt, events: []event{{key: 1, at: receivedAt.Add(2 * time.Minute)}}})
	r.apply(heartbeat(flushAfter(receivedAt)))

	want := map[cellKey]uint64{{key: 1, window: config.WindowOf(receivedAt).ID}: 1}
	if diff := cmp.Diff(want, retiredRequests(r)); diff != "" {
		t.Errorf("retired requests mismatch (-want +got):\n%s", diff)
	}
}

func TestSeriesFirstSeenAfterItsWindowIsFinalGetsALateRevisionOne(t *testing.T) {
	r := newRun(t, false)
	r.apply(singleEvent(at(delay+time.Minute), at(time.Minute)))

	r.apply(batch{receivedAt: at(delay + time.Minute), events: []event{{key: 2, at: at(0)}}})

	want := []finalPane{{Key: 2, Window: windowAt(0), Kind: stream.PaneLate, Revision: 1, Requests: 1}}
	if diff := cmp.Diff(want, r.recorder.finals()); diff != "" {
		t.Errorf("panes mismatch (-want +got):\n%s", diff)
	}
}

func TestSeveralLateEventsInOneBatchEmitOneLatePane(t *testing.T) {
	r := newRun(t, false)
	r.apply(singleEvent(at(delay), at(0)))
	r.apply(heartbeat(at(delay + windowSize)))
	late := []event{{key: 1, at: at(time.Second)}, {key: 1, at: at(2 * time.Second)}, {key: 1, at: at(3 * time.Second)}}

	r.apply(batch{receivedAt: at(delay + windowSize), events: late})

	want := []finalPane{
		{Key: 1, Window: windowAt(0), Kind: stream.PaneOnTime, Revision: 1, Requests: 1},
		{Key: 1, Window: windowAt(0), Kind: stream.PaneLate, Revision: 2, Requests: 4},
	}
	if diff := cmp.Diff(want, r.recorder.finals()); diff != "" {
		t.Errorf("panes mismatch (-want +got):\n%s", diff)
	}
}

func TestEarlyPanesOnlyForChangedWindowsAndNeverAfterFinal(t *testing.T) {
	r := newRun(t, true)
	r.apply(singleEvent(at(delay), at(0)))
	r.windows.FireEarly()
	r.windows.FireEarly()
	r.apply(heartbeat(at(delay + windowSize)))
	r.windows.FireEarly()
	r.apply(singleEvent(at(delay+windowSize), at(time.Second)))
	r.windows.FireEarly()

	want := []finalPane{
		{Key: 1, Window: windowAt(0), Kind: stream.PaneEarly, Requests: 1},
		{Key: 1, Window: windowAt(0), Kind: stream.PaneOnTime, Revision: 1, Requests: 1},
		{Key: 1, Window: windowAt(0), Kind: stream.PaneLate, Revision: 2, Requests: 2},
	}
	if diff := cmp.Diff(want, r.recorder.log); diff != "" {
		t.Errorf("pane log mismatch (-want +got):\n%s", diff)
	}
	stats := r.windows.Stats()
	if diff := cmp.Diff([3]uint64{1, 1, 1}, [3]uint64{stats.PanesEarly, stats.PanesOnTime, stats.PanesLate}); diff != "" {
		t.Errorf("[PanesEarly, PanesOnTime, PanesLate] mismatch (-want +got):\n%s", diff)
	}
}

func TestEngineWithoutEarlyFiringEmitsNoEarlyPanes(t *testing.T) {
	r := newRun(t, false)
	r.apply(singleEvent(at(delay), at(0)))

	r.windows.FireEarly()

	if diff := cmp.Diff([]finalPane(nil), r.recorder.log); diff != "" {
		t.Errorf("panes mismatch (-want +got):\n%s", diff)
	}
}

func TestTimesOutsideTheUnixNanosecondRangeDoNotWrap(t *testing.T) {
	mark := stream.Mark{Time: at(time.Minute), Known: true}
	anchor := at(time.Minute + delay)
	tests := []struct {
		name string
		args time.Time
		want outcome
	}{
		{name: "year 1600 is too late", args: time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC),
			want: outcome{Placement: stream.PlacementDroppedLate, Window: -1,
				Counts: eventCounts{Offered: 1, LateDropped: 1}}},
		{name: "zero time is too late", args: time.Time{}, want: outcome{Placement: stream.PlacementDroppedLate,
			Window: -1, Counts: eventCounts{Offered: 1, LateDropped: 1}}},
		{name: "year 3000 goes to the anchor", args: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC),
			want: outcome{Placement: stream.PlacementOnTime, Window: config.WindowOf(anchor).ID,
				Counts: eventCounts{Offered: 1, OnTime: 1, Reassigned: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, placeOnce(t, mark, anchor, tt.args)); diff != "" {
				t.Errorf("placement mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAnchorOutsideTheHorizonIsClampedInsteadOfBreakingTheRing(t *testing.T) {
	mark := stream.Mark{Time: at(time.Minute), Known: true}
	tests := []struct {
		name string
		args time.Time
		want int64
	}{
		{name: "anchor far in the future", args: at(time.Hour), want: windowAt(110 * time.Second)},
		{name: "anchor far in the past", args: at(-time.Hour), want: windowAt(30 * time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := placeOnce(t, mark, tt.args, at(2*time.Hour))
			if diff := cmp.Diff(tt.want, got.Window); diff != "" {
				t.Errorf("window of an event reassigned to an out-of-horizon anchor (-want +got):\n%s", diff)
			}
		})
	}
}
