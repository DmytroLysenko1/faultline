package processing

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const recordedIterations = 10_000

func uniform(duration time.Duration) Timings {
	return Timings{
		Iteration: duration,
		Apply:     duration,
		Fire:      duration,
		Publish:   duration,
	}
}

func TestTakeReturnsTheMaximaSinceTheLastTakeAndStartsOver(t *testing.T) {
	var measured timings
	measured.record(Timings{
		Iteration: 5 * time.Millisecond,
		Apply:     2 * time.Millisecond,
		Fire:      3 * time.Millisecond,
	})
	measured.record(Timings{
		Iteration: 3 * time.Millisecond,
		Apply:     time.Millisecond,
		Fire:      time.Millisecond,
		Publish:   4 * time.Millisecond,
	})
	got := []Timings{measured.take(), measured.take()}
	measured.record(uniform(time.Microsecond))
	got = append(got, measured.take())
	want := []Timings{
		{
			Iteration: 5 * time.Millisecond,
			Apply:     2 * time.Millisecond,
			Fire:      3 * time.Millisecond,
			Publish:   4 * time.Millisecond,
		},
		{},
		uniform(time.Microsecond),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("takes (-want +got):\n%s", diff)
	}
}

func spread(iteration int) Timings {
	return Timings{
		Iteration: time.Duration(iteration),
		Publish:   time.Duration(recordedIterations - iteration),
	}
}

// TestTakesCoverEveryRecordOnceInOrder: record i has Iteration i and Publish N−i, so a take that
// folds records first…last reads back as Iteration last and Publish N−first. Takes must tile
// 1…N: no record lost, none taken twice, none mixed into a take out of order.
func TestTakesCoverEveryRecordOnceInOrder(t *testing.T) {
	var measured timings
	var writer sync.WaitGroup
	done := make(chan struct{})
	writer.Go(func() {
		defer close(done)
		for iteration := 1; iteration <= recordedIterations; iteration++ {
			measured.record(spread(iteration))
		}
	})
	var taken []Timings
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
		}
		if take := measured.take(); take != (Timings{}) {
			taken = append(taken, take)
		}
	}
	writer.Wait()
	next := 1
	for _, take := range taken {
		first, last := recordedIterations-int(take.Publish), int(take.Iteration)
		if first != next || last < first {
			t.Fatalf("take covers records %d…%d, want it to start at %d", first, last, next)
		}
		next = last + 1
	}
	if next != recordedIterations+1 {
		t.Errorf("takes end at record %d, want %d", next-1, recordedIterations)
	}
}
