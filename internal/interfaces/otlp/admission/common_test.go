package admission_test

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const (
	admitted       = "admitted"
	refusedByShare = "share"
	refusedBySlots = "slots"
	refusedByRate  = "rate"
	neverRefused   = "never refused"
	attemptCap     = 100_000
)

type fakeClock struct {
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{
		now: time.Unix(1_000_000, 0),
	}
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(step time.Duration) {
	c.now = c.now.Add(step)
}

func (c *fakeClock) Set(at time.Time) {
	c.now = at
}

type mark struct {
	Remote string
	Token  credential.ID
}

type recordingMarks struct {
	marks []mark
}

func (r *recordingMarks) Authenticated(remote net.Addr, id credential.ID) {
	r.marks = append(r.marks, mark{
		Remote: remote.String(),
		Token:  id,
	})
}

type noMarks struct{}

func (noMarks) Authenticated(net.Addr, credential.ID) {}

func defaultLimits() admission.Limits {
	return admission.Limits{
		Slots:             10,
		MinShare:          2,
		ActiveWindow:      10 * time.Second,
		WaitingWindow:     60 * time.Second,
		RequestsPerSecond: 2000,
	}
}

func unlimitedRate() admission.Limits {
	limits := defaultLimits()
	limits.RequestsPerSecond = 1_000_000_000
	return limits
}

func newGate(t testing.TB, limits admission.Limits, clock *fakeClock) admission.Gate {
	t.Helper()
	books, err := admission.New(limits, clock)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return admission.Gate{
		Admission: books,
		Marks:     noMarks{},
	}
}

func token(name string) credential.ID {
	return credential.ID(name)
}

func numbered(index int) credential.ID {
	return credential.ID(fmt.Sprintf("tok%09d", index))
}

func enter(gate admission.Gate, id credential.ID) (func(), string) {
	release, err := gate.Enter(id, nil)
	return release, outcomeOf(err)
}

func outcomeOf(err error) string {
	switch {
	case err == nil:
		return admitted
	case errors.Is(err, admission.ErrShareExceeded):
		return refusedByShare
	case errors.Is(err, admission.ErrSlotsFull):
		return refusedBySlots
	case errors.Is(err, admission.ErrRateExceeded):
		return refusedByRate
	}
	return err.Error()
}

func enterAndLeave(gate admission.Gate, id credential.ID) string {
	release, outcome := enter(gate, id)
	if release != nil {
		release()
	}
	return outcome
}

func admitUntilRefused(gate admission.Gate, id credential.ID) (int, string) {
	for count := range attemptCap {
		_, outcome := enter(gate, id)
		if outcome != admitted {
			return count, outcome
		}
	}
	return attemptCap, neverRefused
}

func hold(t *testing.T, gate admission.Gate, id credential.ID, count int) []func() {
	t.Helper()
	releases := make([]func(), 0, count)
	for range count {
		release, outcome := enter(gate, id)
		if outcome != admitted {
			t.Fatalf("%s hold %d: %s", id, len(releases)+1, outcome)
		}
		releases = append(releases, release)
	}
	return releases
}

func releaseAll(releases []func()) {
	for _, release := range releases {
		release()
	}
}
