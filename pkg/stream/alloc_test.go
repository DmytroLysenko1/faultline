package stream_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const measureAttempts = 3

type countCell struct {
	requests uint64
}

type discard struct{}

func (discard) Emit(int, stream.Pane, *countCell)     {}
func (discard) Retire(int, stream.Window, *countCell) {}
func (discard) Closed(int)                            {}

func newCountingWindows(tb testing.TB, series int, early bool) *stream.Windows[int, countCell] {
	tb.Helper()
	windows, err := stream.New[int, countCell](config, stream.Options{Capacity: series, Early: early}, discard{})
	if err != nil {
		tb.Fatal(err)
	}
	return windows
}

func TestSteadyStateBatchesDoNotAllocate(t *testing.T) {
	windows := newCountingWindows(t, capacity, true)
	now := at(time.Hour)
	cycle := func() {
		now = now.Add(windowSize)
		windows.Begin(stream.Mark{Time: now, Known: true}, now)
		for key := range capacity {
			windows.Locate(key, now)
			for late := range 3 {
				windows.Locate(key, now.Add(-time.Duration(late+1)*windowSize))
			}
		}
		windows.End()
		windows.FireEarly(everything())
	}
	for range 10 {
		cycle()
	}

	if allocs := testing.AllocsPerRun(100, cycle); allocs != 0 {
		t.Fatalf("a batch with on-time and late events in every final window allocated %v times, want 0", allocs)
	}
}

// TestFirstBatchWithLateEventsInEveryFinalWindowDoesNotAllocate takes the fewest mallocs of a few attempts: the
// count is the whole process's, so a GC cycle or a runtime goroutine now and then adds one that is not the batch's,
// while an allocation of the batch itself shows in every attempt.
func TestFirstBatchWithLateEventsInEveryFinalWindowDoesNotAllocate(t *testing.T) {
	allocs := firstBatchMallocs(t)
	for range measureAttempts - 1 {
		allocs = min(allocs, firstBatchMallocs(t))
	}

	if allocs != 0 {
		t.Fatalf("late events in the 3 final, unretired windows of every series allocated %d times, want 0", allocs)
	}
}

func firstBatchMallocs(t *testing.T) uint64 {
	t.Helper()
	windows := newCountingWindows(t, capacity, false)
	start := at(time.Hour)
	windows.Begin(stream.Mark{Time: start, Known: true}, start)
	for key := range capacity {
		for window := range 4 {
			windows.Locate(key, start.Add(time.Duration(window)*windowSize))
		}
	}
	windows.End()
	mark := start.Add(4 * windowSize)
	windows.Begin(stream.Mark{Time: mark, Known: true}, mark)

	return mallocsDuring(func() {
		for key := range capacity {
			for window := range 4 {
				windows.Locate(key, start.Add(time.Duration(window)*windowSize))
			}
		}
		windows.End()
	})
}

// mallocsDuring first finishes the GC cycle the setup may have started, so it does not end inside run.
func mallocsDuring(run func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.Mallocs - before.Mallocs
}
