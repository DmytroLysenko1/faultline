package clock

import "time"

type Monotonic struct {
	Epoch   time.Time
	Started bool
	Elapsed time.Duration
}

func (c *Monotonic) Advance(now time.Time) time.Duration {
	if !c.Started {
		c.Epoch = now
		c.Started = true
	}
	c.Elapsed = max(c.Elapsed, now.Sub(c.Epoch))
	return c.Elapsed
}

func (c *Monotonic) Now() time.Time {
	return c.Epoch.Add(c.Elapsed)
}
