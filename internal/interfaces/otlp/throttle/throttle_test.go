package throttle_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/throttle"
)

const (
	interval          = 10 * time.Second
	halfInterval      = interval / 2
	concurrentCallers = 8
	callsPerCaller    = 500
)

type fakeClock struct {
	lock sync.Mutex
	now  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{
		now: time.Unix(0, 0),
	}
}

func (c *fakeClock) Now() time.Time {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.now
}

func (c *fakeClock) Advance(step time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.now = c.now.Add(step)
}

type admission struct {
	Suppressed throttle.Suppressed
	Admitted   bool
}

func admit(gate *throttle.Throttle, weight int64) admission {
	suppressed, admitted := gate.Admit(weight)
	return admission{
		Suppressed: suppressed,
		Admitted:   admitted,
	}
}

func TestFirstEventIsWrittenImmediately(t *testing.T) {
	gate := throttle.New(interval, newClock())
	want := admission{
		Admitted: true,
	}
	if diff := cmp.Diff(want, admit(gate, 3)); diff != "" {
		t.Errorf("first admission (-want +got):\n%s", diff)
	}
}

func TestEventsWithinTheIntervalAreSuppressedAndReportedOnce(t *testing.T) {
	clock := newClock()
	gate := throttle.New(interval, clock)
	admit(gate, 1)
	clock.Advance(halfInterval)
	got := []admission{
		admit(gate, 2),
		admit(gate, 5),
	}
	clock.Advance(halfInterval)
	got = append(got, admit(gate, 7), admit(gate, 11))
	want := []admission{
		{},
		{},
		{
			Suppressed: throttle.Suppressed{
				Events: 2,
				Weight: 7,
			},
			Admitted: true,
		},
		{},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("admissions (-want +got):\n%s", diff)
	}
}

func TestWrittenEventIsNotCountedAsSuppressed(t *testing.T) {
	clock := newClock()
	gate := throttle.New(interval, clock)
	admit(gate, 1)
	clock.Advance(interval)
	admit(gate, 4)
	clock.Advance(interval)
	if diff := cmp.Diff(throttle.Suppressed{}, admit(gate, 9).Suppressed); diff != "" {
		t.Errorf("nothing was suppressed between two written events (-want +got):\n%s", diff)
	}
}

func TestConcurrentAdmitsLoseNoEvent(t *testing.T) {
	clock := newClock()
	gate := throttle.New(interval, clock)
	var admitted atomic.Int64
	var callers sync.WaitGroup
	for range concurrentCallers {
		callers.Go(func() {
			for range callsPerCaller {
				if _, ok := gate.Admit(1); ok {
					admitted.Add(1)
				}
			}
		})
	}
	callers.Wait()
	clock.Advance(interval)
	final := admit(gate, 1)
	total := admitted.Load() + final.Suppressed.Events
	if want := int64(concurrentCallers * callsPerCaller); total != want {
		t.Errorf("written %d + suppressed %d = %d events, want %d", admitted.Load(), final.Suppressed.Events, total, want)
	}
}

func TestSuppressedCountRestartsAfterEachWrittenRecord(t *testing.T) {
	clock := newClock()
	gate := throttle.New(interval, clock)
	admit(gate, 1)
	admit(gate, 2)
	clock.Advance(interval)
	admit(gate, 3)
	clock.Advance(interval)
	want := admission{
		Admitted: true,
	}
	if diff := cmp.Diff(want, admit(gate, 4)); diff != "" {
		t.Errorf("record after a quiet interval reports old suppressions (-want +got):\n%s", diff)
	}
}
