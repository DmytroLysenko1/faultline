package checkpoint

import "sync/atomic"

type Stats struct {
	Epoch          uint64
	SweptObjects   uint64
	SweptOrphans   uint64
	SweepFailures  uint64
	UnreadableMeta uint64
	LastSweepError error
}

type counters struct {
	sweptObjects  atomic.Uint64
	sweptOrphans  atomic.Uint64
	sweepFailures atomic.Uint64
	unreadable    atomic.Uint64
	lastSweep     atomic.Pointer[error]
}

func (c *counters) snapshot(epoch uint64) Stats {
	stats := Stats{
		Epoch:          epoch,
		SweptObjects:   c.sweptObjects.Load(),
		SweptOrphans:   c.sweptOrphans.Load(),
		SweepFailures:  c.sweepFailures.Load(),
		UnreadableMeta: c.unreadable.Load(),
	}
	if last := c.lastSweep.Load(); last != nil {
		stats.LastSweepError = *last
	}
	return stats
}

func (c *counters) sweepFailed(err error) {
	c.sweepFailures.Add(1)
	c.lastSweep.Store(&err)
}
