package red

import (
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type WindowStats struct {
	StartUnixNano  int64
	Kind           PaneKind
	Revision       uint32
	Requests       uint64
	Failed         uint64
	LatencySamples uint64
	P50            time.Duration
	P95            time.Duration
	P99            time.Duration
	Observed       time.Duration
}

func Summarize(arena *sketch.Arena, pane stream.Pane, cell *Cell, observed time.Duration) WindowStats {
	samples := cell.Latency.Count
	if samples > cell.Requests {
		panic(fmt.Sprintf("red: %d latency samples for %d requests", samples, cell.Requests))
	}
	stats := SummarizeCounts(pane, &cell.Counts, observed)
	percentiles, known := arena.Percentiles(&cell.Latency)
	if !known {
		return stats
	}
	stats.LatencySamples = samples
	stats.P50 = time.Duration(percentiles.P50)
	stats.P95 = time.Duration(percentiles.P95)
	stats.P99 = time.Duration(percentiles.P99)
	return stats
}

func SummarizeCounts(pane stream.Pane, counts *Counts, observed time.Duration) WindowStats {
	if counts.Failed > counts.Requests {
		panic(fmt.Sprintf("red: %d failed of %d requests", counts.Failed, counts.Requests))
	}
	if size := pane.Window.End.Sub(pane.Window.Start); observed <= 0 || observed > size {
		panic(fmt.Sprintf("red: observed %s outside the window of %s", observed, size))
	}
	return WindowStats{
		StartUnixNano: pane.Window.Start.UnixNano(),
		Kind:          kindOf(pane.Kind),
		Revision:      pane.Revision,
		Requests:      counts.Requests,
		Failed:        counts.Failed,
		Observed:      observed,
	}
}

func (s WindowStats) Rate() float64 {
	if s.IsZero() {
		return 0
	}
	return float64(s.Requests) / s.Observed.Seconds()
}

func (s WindowStats) ErrorRatio() float64 {
	if s.Requests == 0 {
		return 0
	}
	return float64(s.Failed) / float64(s.Requests)
}

func (s WindowStats) HasLatency() bool {
	return s.LatencySamples > 0
}

func (s WindowStats) IsZero() bool {
	return s.Kind == 0
}

func (s WindowStats) Equal(other WindowStats) bool {
	return s == other
}
