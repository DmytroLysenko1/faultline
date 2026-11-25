package feed

import (
	"math"
	"sync"
	"testing"
)

func TestUnreleasedNeverWrapsWhileTheFeedStillHands(t *testing.T) {
	var c counters
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			c.handed(false)
			c.released.Add(1)
		}
	})
	worst := uint64(0)
	overReleased := 0
	for range 2_000_000 {
		stats := c.snapshot()
		worst = max(worst, stats.Unreleased())
		if stats.Released > stats.DeliveredBatches+stats.Heartbeats {
			overReleased++
		}
	}
	close(done)
	wg.Wait()
	if worst > math.MaxUint64/2 {
		t.Fatalf("Unreleased wrapped to %d", worst)
	}
	if overReleased > 0 {
		t.Fatalf("%d snapshots released more batches than the feed handed", overReleased)
	}
}
