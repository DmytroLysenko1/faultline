package eventtime

import (
	"math"
	"time"
)

// Interval keeps zero as "unknown" for both fields, so the zero value is an
// untimed span: end 0 is unknown by the OTLP rules, and latency is stored + 1.
type Interval struct {
	end            int64
	latencyPlusOne int64
}

func FromUnixNanos(start, end uint64) Interval {
	if end == 0 || end > math.MaxInt64 {
		return Interval{}
	}
	interval := Interval{end: int64(end)}
	if start != 0 && start <= end {
		interval.latencyPlusOne = int64(end-start) + 1
	}
	return interval
}

func (i Interval) End() (time.Time, bool) {
	if i.end == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, i.end), true
}

func (i Interval) Latency() (time.Duration, bool) {
	if i.latencyPlusOne == 0 {
		return 0, false
	}
	return time.Duration(i.latencyPlusOne - 1), true
}

func (i Interval) Equal(other Interval) bool {
	return i == other
}
