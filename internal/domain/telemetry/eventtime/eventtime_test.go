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
