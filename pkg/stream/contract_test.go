package stream_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

func TestAcceptanceFRSTR001AtLeastNineEarlyResultsBeforeTheFinalOne(t *testing.T) {
	r := newRun(t, true)
	window := config.WindowOf(at(0))
	const eventsPerSecond = 1000
	for second := range windowSize / time.Second {
		events := make([]event, eventsPerSecond)
		for i := range events {
			events[i] = event{key: 1, at: window.Start.Add(second*time.Second + time.Duration(i)*time.Millisecond)}
		}
		r.apply(batch{receivedAt: window.Start.Add((second + 1) * time.Second), events: events})
		r.windows.FireEarly(everything())
	}
	r.apply(heartbeat(flushAfter(window.End)))

	type summary struct {
		NineEarlyBeforeFinal bool
		FinalRequests        uint64
	}
	early, final := 0, finalPane{}
	for _, pane := range r.recorder.log {
		if pane.Kind != stream.PaneEarly {
			final = pane
			break
		}
		early++
	}
	want := summary{NineEarlyBeforeFinal: true, FinalRequests: eventsPerSecond * uint64(windowSize/time.Second)}
	if diff := cmp.Diff(want, summary{NineEarlyBeforeFinal: early >= 9, FinalRequests: final.Requests}); diff != "" {
		t.Errorf("FR-STR-001 mismatch (-want +got):\n%s", diff)
	}
}

func TestProtocolViolationsPanic(t *testing.T) {
	tests := []struct {
		name string
		args func(r *run)
	}{
		{name: "Locate outside a batch", args: func(r *run) { r.windows.Locate(1, at(0)) }},
		{name: "Classify outside a batch", args: func(r *run) { r.windows.Classify(at(0)) }},
		{name: "Drop outside a batch", args: func(r *run) { r.windows.Drop(stream.PlacementDroppedLate) }},
		{name: "End outside a batch", args: func(r *run) { r.windows.End() }},
		{name: "Begin twice", args: func(r *run) {
			r.windows.Begin(stream.Mark{}, at(0))
			r.windows.Begin(stream.Mark{}, at(0))
		}},
		{name: "Drop of a placed event", args: func(r *run) {
			r.windows.Begin(stream.Mark{}, at(0))
			r.windows.Drop(stream.PlacementOnTime)
		}},
		{name: "new series beyond capacity", args: fillBeyondCapacity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRun(t, false)
			defer func() {
				if recover() == nil {
					t.Fatal("protocol violation did not panic")
				}
			}()
			tt.args(r)
		})
	}
}

func TestWatermarkThatMovesBackDoesNotMoveTheWindowsBack(t *testing.T) {
	r := newRun(t, false)
	r.windows.Begin(stream.Mark{Time: at(time.Minute), Known: true}, at(time.Minute))
	r.windows.End()

	r.windows.Begin(stream.Mark{Time: at(-time.Hour), Known: true}, at(-time.Hour))
	placement := r.windows.Classify(at(0))
	r.windows.End()
	r.windows.Begin(stream.Mark{}, at(0))
	afterUnknown := r.windows.Classify(at(0))
	r.windows.End()

	want := []stream.Placement{stream.PlacementDroppedLate, stream.PlacementDroppedLate}
	if diff := cmp.Diff(want, []stream.Placement{placement, afterUnknown}); diff != "" {
		t.Errorf("placement of an event 60 s behind the highest watermark (-want +got):\n%s", diff)
	}
}

func fillBeyondCapacity(r *run) {
	r.windows.Begin(stream.Mark{Time: at(0), Known: true}, at(0))
	for key := range capacity + 1 {
		r.windows.Locate(key, at(0))
	}
}

func TestSeriesLifecycle(t *testing.T) {
	r := newRun(t, false)
	r.windows.Begin(stream.Mark{Time: at(0), Known: true}, at(0))
	_, _, first := r.windows.Locate(1, at(0))
	_, _, second := r.windows.Locate(1, at(time.Second))
	r.windows.End()
	open, liveLen, liveCapacity := r.windows.Has(1), r.windows.Len(), r.windows.Capacity()
	r.apply(heartbeat(flushAfter(at(0))))

	type lifecycle struct {
		Created, CreatedAgain, OpenAfterPlacement, OpenAfterFlush bool
		LiveLen, LiveCapacity, Len, Capacity                      int
		Closed                                                    []int
	}
	want := lifecycle{Created: true, OpenAfterPlacement: true, LiveLen: 1, LiveCapacity: capacity, Capacity: capacity,
		Closed: []int{1}}
	got := lifecycle{Created: first, CreatedAgain: second, OpenAfterPlacement: open, OpenAfterFlush: r.windows.Has(1),
		LiveLen: liveLen, LiveCapacity: liveCapacity, Len: r.windows.Len(), Capacity: r.windows.Capacity(),
		Closed: r.recorder.closed}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("series lifecycle mismatch (-want +got):\n%s", diff)
	}
}

func TestClosedSeriesFreeCapacityForNewKeys(t *testing.T) {
	r := newRun(t, false)
	events := make([]event, capacity)
	for i := range events {
		events[i] = event{key: i, at: at(0)}
	}
	r.apply(batch{receivedAt: at(0), events: events})
	r.apply(heartbeat(flushAfter(at(0))))

	r.apply(singleEvent(at(10*time.Minute), at(10*time.Minute)))

	if diff := cmp.Diff([2]int{1, capacity}, [2]int{r.windows.Len(), len(r.recorder.closed)}); diff != "" {
		t.Errorf("[Len, Closed calls] mismatch (-want +got):\n%s", diff)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    stream.Config
		wantErr error
	}{
		{name: "defaults", args: config},
		{name: "zero window", args: stream.Config{Delay: delay, SlackWindows: 1}, wantErr: stream.ErrInvalidConfig},
		{name: "negative delay", args: stream.Config{Size: windowSize, Delay: -1, SlackWindows: 1},
			wantErr: stream.ErrInvalidConfig},
		{name: "negative lateness", args: stream.Config{Size: windowSize, AllowedLateness: -1, SlackWindows: 1},
			wantErr: stream.ErrInvalidConfig},
		{name: "no slack", args: stream.Config{Size: windowSize}, wantErr: stream.ErrInvalidConfig},
		{name: "too many live windows", args: stream.Config{Size: time.Second, Delay: time.Hour, SlackWindows: 1},
			wantErr: stream.ErrInvalidConfig},
		{name: "delay plus lateness overflows", args: stream.Config{Size: time.Second, Delay: math.MaxInt64,
			AllowedLateness: time.Second, SlackWindows: 1}, wantErr: stream.ErrInvalidConfig},
		{name: "huge delay and lateness", args: stream.Config{Size: time.Second, Delay: 1 << 62, AllowedLateness: 1 << 62,
			SlackWindows: 1}, wantErr: stream.ErrInvalidConfig},
		{name: "delay plus lateness overflows with a huge window", args: stream.Config{Size: math.MaxInt64,
			Delay: math.MaxInt64 / 2, AllowedLateness: math.MaxInt64/2 + 10, SlackWindows: 1},
			wantErr: stream.ErrInvalidConfig},
		{name: "huge slack", args: stream.Config{Size: windowSize, SlackWindows: math.MaxInt},
			wantErr: stream.ErrInvalidConfig},
		{name: "huge window with a tiny delay", args: stream.Config{Size: math.MaxInt64, Delay: 2, SlackWindows: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.args.Validate(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestSlotsCoverDelayLatenessTheCurrentWindowAndSlack(t *testing.T) {
	tests := []struct {
		name string
		args stream.Config
		want int
	}{
		{name: "defaults: 6 for delay + lateness, 1 current, 2 slack", args: config, want: 9},
		{name: "partial window rounds up", args: stream.Config{Size: windowSize, Delay: time.Second, SlackWindows: 1},
			want: 3},
		{name: "window wider than the delay", args: stream.Config{Size: math.MaxInt64, Delay: 2, SlackWindows: 1},
			want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.args.Slots()); diff != "" {
				t.Errorf("Slots() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	recorder := newRecorder(t)
	tests := []struct {
		name    string
		args    stream.Options
		sink    stream.Sink[int, cell]
		wantErr error
	}{
		{name: "zero capacity", args: stream.Options{}, sink: recorder, wantErr: stream.ErrInvalidOptions},
		{name: "huge capacity", args: stream.Options{Capacity: math.MaxInt32}, sink: recorder,
			wantErr: stream.ErrInvalidOptions},
		{name: "no sink", args: stream.Options{Capacity: 1}, wantErr: stream.ErrInvalidOptions},
		{name: "invalid config", args: stream.Options{Capacity: 1}, sink: recorder, wantErr: stream.ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config
			if errors.Is(tt.wantErr, stream.ErrInvalidConfig) {
				cfg.SlackWindows = 0
			}
			if _, err := stream.New(cfg, tt.args, tt.sink); !errors.Is(err, tt.wantErr) {
				t.Fatalf("New() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestWatermarkFollowsTheLatestCandidateAndNeverPassesReceiveTime(t *testing.T) {
	watermark, err := stream.NewWatermark(config)
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		receivedAt time.Time
		candidate  stream.Mark
	}{
		{receivedAt: at(time.Minute), candidate: stream.Mark{Time: at(time.Minute), Known: true}},
		{receivedAt: at(2 * time.Minute), candidate: stream.Mark{Time: at(90 * time.Second), Known: true}},
		{receivedAt: at(3 * time.Minute), candidate: stream.Mark{Time: at(0), Known: true}},
		{receivedAt: at(4 * time.Minute), candidate: stream.Mark{}},
		{receivedAt: at(5 * time.Minute), candidate: stream.Mark{Time: at(time.Hour), Known: true}},
		{receivedAt: at(4 * time.Minute), candidate: stream.Mark{}},
	}
	got := make([]time.Time, 0, len(steps))
	for _, step := range steps {
		got = append(got, watermark.Advance(step.receivedAt, step.candidate).Time)
	}

	want := []time.Time{
		at(30 * time.Second), at(time.Minute), at(time.Minute), at(time.Minute), at(4*time.Minute + 30*time.Second),
		at(4*time.Minute + 30*time.Second),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("watermark after each batch mismatch (-want +got):\n%s", diff)
	}
	wantMarks := []time.Time{at(0), at(5 * time.Minute), want[len(want)-1]}
	gotMarks := []time.Time{watermark.CoverageStart.Time, watermark.MaxReceivedAt.Time, watermark.Current.Time}
	if diff := cmp.Diff(wantMarks, gotMarks); diff != "" {
		t.Errorf("[CoverageStart, MaxReceivedAt, Current] mismatch (-want +got):\n%s", diff)
	}
}

func TestAnchorPrefersTheBatchCandidateThenTheLatestCandidateThenReceiveTime(t *testing.T) {
	watermark, err := stream.NewWatermark(config)
	if err != nil {
		t.Fatal(err)
	}
	beforeAnyCandidate := watermark.Anchor(stream.Mark{}, at(time.Minute))
	watermark.Advance(at(2*time.Minute), stream.Mark{Time: at(90 * time.Second), Known: true})

	want := []time.Time{at(time.Minute), at(90 * time.Second), at(80 * time.Second), at(3 * time.Minute)}
	got := []time.Time{
		beforeAnyCandidate,
		watermark.Anchor(stream.Mark{}, at(3*time.Minute)),
		watermark.Anchor(stream.Mark{Time: at(80 * time.Second), Known: true}, at(3*time.Minute)),
		watermark.Anchor(stream.Mark{Time: at(time.Hour), Known: true}, at(3*time.Minute)),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("anchors [no candidate yet, batch without candidate, batch with candidate] (-want +got):\n%s", diff)
	}
}

func TestWindowOfStaysCorrectAtTheEdgesOfUnixNanoseconds(t *testing.T) {
	for _, nanos := range []int64{math.MinInt64, math.MaxInt64} {
		instant := time.Unix(0, nanos)
		window := config.WindowOf(instant)
		if instant.Before(window.Start) || !instant.Before(window.End) {
			t.Errorf("WindowOf(%d) = [%s, %s), does not contain the instant %s", nanos, window.Start, window.End, instant)
		}
	}
}

func TestNewWatermarkRejectsAnInvalidConfig(t *testing.T) {
	if _, err := stream.NewWatermark(stream.Config{}); !errors.Is(err, stream.ErrInvalidConfig) {
		t.Fatalf("NewWatermark(zero config) = %v, want ErrInvalidConfig", err)
	}
}
