package eventtime_test

import (
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
)

type observed struct {
	End          time.Time
	EndKnown     bool
	Latency      time.Duration
	LatencyKnown bool
}

func observe(interval eventtime.Interval) observed {
	end, endKnown := interval.End()
	latency, latencyKnown := interval.Latency()
	return observed{End: end, EndKnown: endKnown, Latency: latency, LatencyKnown: latencyKnown}
}

func TestFromUnixNanos(t *testing.T) {
	const second = uint64(time.Second)
	type args struct {
		start, end uint64
	}
	tests := []struct {
		name string
		args args
		want observed
	}{
		{name: "end zero is untimed", args: args{start: second, end: 0}, want: observed{}},
		{name: "end beyond int64 is untimed", args: args{start: second, end: math.MaxInt64 + 1}, want: observed{}},
		{name: "start zero keeps the end, latency unknown", args: args{start: 0, end: 5 * second},
			want: observed{End: time.Unix(5, 0), EndKnown: true}},
		{name: "start after end keeps the end, latency unknown", args: args{start: 6 * second, end: 5 * second},
			want: observed{End: time.Unix(5, 0), EndKnown: true}},
		{name: "start equals end is zero latency", args: args{start: 5 * second, end: 5 * second},
			want: observed{End: time.Unix(5, 0), EndKnown: true, LatencyKnown: true}},
		{name: "ordinary span", args: args{start: 4 * second, end: 5 * second},
			want: observed{End: time.Unix(5, 0), EndKnown: true, Latency: time.Second, LatencyKnown: true}},
		{name: "largest representable span", args: args{start: 1, end: math.MaxInt64},
			want: observed{End: time.Unix(0, math.MaxInt64), EndKnown: true, Latency: math.MaxInt64 - 1,
				LatencyKnown: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := observe(eventtime.FromUnixNanos(tt.args.start, tt.args.end))
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("interval mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestZeroIntervalIsUntimedAndSixteenBytes(t *testing.T) {
	type layout struct {
		Observed observed
		Bytes    uintptr
	}
	want := layout{Bytes: 16}
	got := layout{Observed: observe(eventtime.Interval{}), Bytes: unsafe.Sizeof(eventtime.Interval{})}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("zero interval mismatch (-want +got):\n%s", diff)
	}
}

func TestIntervalEqualDistinguishesEndAndLatency(t *testing.T) {
	const second = uint64(time.Second)
	base := eventtime.FromUnixNanos(second, 3*second)
	tests := []struct {
		Name string
		Args eventtime.Interval
		Want bool
	}{
		{Name: "same interval", Args: eventtime.FromUnixNanos(second, 3*second), Want: true},
		{Name: "another end", Args: eventtime.FromUnixNanos(2*second, 4*second)},
		{Name: "another latency", Args: eventtime.FromUnixNanos(2*second, 3*second)},
		{Name: "unknown latency", Args: eventtime.FromUnixNanos(0, 3*second)},
		{Name: "untimed", Args: eventtime.Interval{}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := [2]bool{base.Equal(tt.Args), cmp.Diff(base, tt.Args) == ""}
			if diff := cmp.Diff([2]bool{tt.Want, tt.Want}, got); diff != "" {
				t.Errorf("[Equal, cmp.Diff empty] mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEndsAfterRanksUntimedBeforeEveryTimedInterval(t *testing.T) {
	untimed := eventtime.Interval{}
	early := eventtime.FromUnixNanos(1, 10)
	late := eventtime.FromUnixNanos(1, 20)
	sameEndLongerLatency := eventtime.FromUnixNanos(0, 20)
	tests := []struct {
		Name string
		Args [2]eventtime.Interval
		Want bool
	}{
		{Name: "later end", Args: [2]eventtime.Interval{late, early}, Want: true},
		{Name: "earlier end", Args: [2]eventtime.Interval{early, late}},
		{Name: "same end, whatever the latency", Args: [2]eventtime.Interval{sameEndLongerLatency, late}},
		{Name: "timed after untimed", Args: [2]eventtime.Interval{early, untimed}, Want: true},
		{Name: "untimed after timed", Args: [2]eventtime.Interval{untimed, early}},
		{Name: "untimed after untimed", Args: [2]eventtime.Interval{untimed, untimed}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if got := tt.Args[0].EndsAfter(tt.Args[1]); got != tt.Want {
				t.Errorf("EndsAfter = %v, want %v", got, tt.Want)
			}
		})
	}
}
