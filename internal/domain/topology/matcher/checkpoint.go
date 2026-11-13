package matcher

import (
	"fmt"
	"math"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func (m *Matcher) WriteCheckpoint(w *checkpoint.Writer) {
	for _, counter := range m.Stats.counters() {
		w.Uint(*counter)
	}
	m.Clock.WriteCheckpoint(w)
	w.Count(m.Pending.Len())
	for slot, found := m.Pending.Front(); found; slot, found = m.Pending.Next(slot) {
		m.Pending.At(slot).write(w)
	}
	m.Tombstones.WriteCheckpoint(w)
}

func Restore(limits Limits, r *checkpoint.Reader) (*Matcher, error) {
	restored, err := New(limits)
	if err != nil {
		return nil, err
	}
	for _, counter := range restored.Stats.counters() {
		*counter = r.Uint()
	}
	restored.Clock = clock.Restore(r)
	restored.readQueue(r)
	if restored.Tombstones, err = tombstone.Restore(limits.MaxTombstones, r); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	return restored, nil
}

func (s *Stats) counters() []*uint64 {
	return []*uint64{
		&s.Observed, &s.Matched,
		&s.Duplicate, &s.Unpairable,
		&s.Evicted, &s.Expired,
		&s.Pending, &s.ExpiredCaller,
		&s.EvictedCaller, &s.TombstoneEvicted,
	}
}

func (m *Matcher) readQueue(r *checkpoint.Reader) {
	pending, previous := r.Count(m.Limits.MaxPending), time.Duration(math.MinInt64)
	for range pending {
		previous = m.readPending(r, previous)
	}
	if m.Stats.Pending != nonNegative(int64(pending)) {
		r.Fail(fmt.Sprintf("%d halves wait, stats count %d", pending, m.Stats.Pending))
	}
}

func (m *Matcher) readPending(r *checkpoint.Reader, previous time.Duration) time.Duration {
	half := readHalf(r)
	if _, repeated := m.PendingByKey[half.Key]; repeated || !half.Side.Pairable() || half.Deadline < previous {
		r.Fail("a waiting half is repeated, cannot pair or is out of deadline order")
		return previous
	}
	m.PendingByKey[half.Key] = m.Pending.PushBack(&half)
	return half.Deadline
}

func (h *pendingHalf) write(w *checkpoint.Writer) {
	w.Fixed(h.Key.TraceID[:])
	w.Fixed(h.Key.ID[:])
	h.Service.WriteCheckpoint(w)
	w.Duration(h.Deadline)
	w.Uint(uint64(h.Side))
	w.Bool(h.Failed)
	writeInterval(w, h.Interval)
	h.Operation.WriteCheckpoint(w)
}

func readHalf(r *checkpoint.Reader) pendingHalf {
	var half pendingHalf
	r.Fixed(half.Key.TraceID[:])
	r.Fixed(half.Key.ID[:])
	half.Service = resource.ReadServiceRef(r)
	half.Deadline = r.Duration()
	half.Side = role.Role(r.Uint8(uint8(role.Entry)))
	half.Failed = r.Bool()
	half.Interval = readInterval(r)
	half.Operation = operation.ReadOperation(r)
	return half
}

func writeInterval(w *checkpoint.Writer, interval eventtime.Interval) {
	var endNanos, latencyPlusOne uint64
	if end, timed := interval.End(); timed {
		endNanos = nonNegative(end.UnixNano())
	}
	if latency, measured := interval.Latency(); measured {
		latencyPlusOne = nonNegative(int64(latency)) + 1
	}
	w.Uint(endNanos)
	w.Uint(latencyPlusOne)
}

func readInterval(r *checkpoint.Reader) eventtime.Interval {
	end, latencyPlusOne := r.Uint(), r.Uint()
	if end > math.MaxInt64 || latencyPlusOne > end {
		r.Fail("interval latency outside its end")
		return eventtime.Interval{}
	}
	if latencyPlusOne == 0 {
		return eventtime.FromUnixNanos(0, end)
	}
	return eventtime.FromUnixNanos(end-(latencyPlusOne-1), end)
}

func nonNegative(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
