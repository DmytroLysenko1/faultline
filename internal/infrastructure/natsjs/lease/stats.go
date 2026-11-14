package lease

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	Held            bool
	Epoch           uint64
	Renewals        uint64
	RenewFailures   uint64
	LostWrites      uint64
	ReleaseFailures uint64
	RenewAge        time.Duration
}

type counters struct {
	renewals        atomic.Uint64
	renewFailures   atomic.Uint64
	lostWrites      atomic.Uint64
	releaseFailures atomic.Uint64
}

func (c *counters) snapshot(epoch uint64, held bool, age time.Duration) Stats {
	return Stats{
		Held:            held,
		Epoch:           epoch,
		Renewals:        c.renewals.Load(),
		RenewFailures:   c.renewFailures.Load(),
		LostWrites:      c.lostWrites.Load(),
		ReleaseFailures: c.releaseFailures.Load(),
		RenewAge:        age,
	}
}
