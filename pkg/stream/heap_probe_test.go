//go:build !race

package stream_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type subjectCell [5]uint64

type subjectSink struct{}

func (subjectSink) Emit(int, stream.Pane, *subjectCell)     {}
func (subjectSink) Retire(int, stream.Window, *subjectCell) {}
func (subjectSink) Closed(int)                              {}

const overestimateSlack = 1.25

func TestWorstCaseHeapBytesBoundsTheMeasuredHeap(t *testing.T) {
	for _, series := range []int{70_000, 100_000} {
		t.Run(fmt.Sprintf("%d series", series), func(t *testing.T) {
			measured := measureHeap(t, series)
			t.Logf("measured %d B, estimate %d B", measured, stream.WorstCaseHeapBytes[int, subjectCell](config, series))
			estimate := stream.WorstCaseHeapBytes[int, subjectCell](config, series)
			if measured > estimate || float64(estimate) > float64(measured)*overestimateSlack {
				t.Fatalf("measured %d B, estimate %d B: want measured ≤ estimate ≤ %.2f × measured", measured, estimate,
					overestimateSlack)
			}
		})
	}
}

func measureHeap(t *testing.T, series int) int64 {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	windows, err := stream.New[int, subjectCell](config, stream.Options{Capacity: series}, subjectSink{})
	if err != nil {
		t.Fatal(err)
	}
	fillEverySeries(windows, series)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(windows)
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

func fillEverySeries(windows *stream.Windows[int, subjectCell], series int) {
	now := at(0)
	windows.Begin(stream.Mark{Time: now, Known: true}, now)
	for key := range series {
		windows.Locate(key, now)
	}
	windows.End()
}
