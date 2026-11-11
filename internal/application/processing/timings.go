package processing

import (
	"sync"
	"time"
)

type Timings struct {
	Iteration time.Duration
	Apply     time.Duration
	Fire      time.Duration
	Publish   time.Duration
}

func (t Timings) widest(other Timings) Timings {
	return Timings{
		Iteration: max(t.Iteration, other.Iteration),
		Apply:     max(t.Apply, other.Apply),
		Fire:      max(t.Fire, other.Fire),
		Publish:   max(t.Publish, other.Publish),
	}
}

type timings struct {
	mu      sync.Mutex
	maximum Timings
}

func (t *timings) record(iteration Timings) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maximum = t.maximum.widest(iteration)
}

func (t *timings) take() Timings {
	t.mu.Lock()
	defer t.mu.Unlock()
	taken := t.maximum
	t.maximum = Timings{}
	return taken
}
