package throttle

import (
	"sync"
	"time"
)

type clock interface {
	Now() time.Time
}

type Suppressed struct {
	Events int64
	Weight int64
}

func (s Suppressed) with(weight int64) Suppressed {
	return Suppressed{
		Events: s.Events + 1,
		Weight: s.Weight + weight,
	}
}

type Throttle struct {
	interval time.Duration
	clock    clock
	lock     sync.Mutex
	written  bool
	last     time.Time
	pending  Suppressed
}

func New(interval time.Duration, clock clock) *Throttle {
	return &Throttle{
		interval: interval,
		clock:    clock,
	}
}

func (t *Throttle) Admit(weight int64) (Suppressed, bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	now := t.clock.Now()
	if t.written && now.Sub(t.last) < t.interval {
		t.pending = t.pending.with(weight)
		return Suppressed{}, false
	}
	suppressed := t.pending
	t.pending = Suppressed{}
	t.written = true
	t.last = now
	return suppressed, true
}
