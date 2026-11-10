package matcher

import (
	"container/list"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
)

var ErrInvalidLimits = errors.New("invalid matcher limits")

type Limits struct {
	TTL           time.Duration
	MaxPending    int
	TombstoneTTL  time.Duration
	MaxTombstones int
}

func (l Limits) validate() error {
	switch {
	case l.TTL <= 0:
		return fmt.Errorf("%w: pairing ttl %s must be positive", ErrInvalidLimits, l.TTL)
	case l.MaxPending <= 0:
		return fmt.Errorf("%w: max pending %d must be positive", ErrInvalidLimits, l.MaxPending)
	case l.TombstoneTTL < l.TTL:
		return fmt.Errorf("%w: tombstone ttl %s is below pairing ttl %s", ErrInvalidLimits, l.TombstoneTTL, l.TTL)
	}
	return nil
}

type pendingHalf struct {
	Key      ids.CallKey
	Service  resource.ServiceRef
	Deadline time.Duration
	Side     role.Role
	Failed   bool
}

type Matcher struct {
	Limits       Limits
	PendingByKey map[ids.CallKey]*list.Element
	Pending      *list.List
	Tombstones   tombstone.Set
	Stats        Stats
	Clock        clock.Monotonic
}

func New(limits Limits) (*Matcher, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	tombstones, err := tombstone.New(limits.MaxTombstones)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	return &Matcher{
		Limits:       limits,
		PendingByKey: make(map[ids.CallKey]*list.Element),
		Pending:      list.New(),
		Tombstones:   tombstones,
	}, nil
}

func (m *Matcher) Match(observed *span.Span, now time.Time) (call.Call, bool) {
	key, side := role.Of(observed)
	if !m.Stats.Observe(side) {
		return call.Call{}, false
	}
	at := m.Clock.Advance(now)
	arrived := pendingHalf{
		Key:      key,
		Side:     side,
		Service:  observed.Service,
		Failed:   observed.Failed,
		Deadline: at + m.Limits.TTL,
	}
	waiting := m.PendingByKey[key]
	switch {
	case m.isDuplicate(arrived, waiting):
		m.Stats.RecordDuplicate()
		return call.Call{}, false
	case waiting == nil:
		m.wait(arrived)
		return call.Call{}, false
	}
	return m.complete(waiting, arrived, at), true
}

func (m *Matcher) Expire(now time.Time) []call.Unanswered {
	at := m.Clock.Advance(now)
	m.Tombstones.Expire(at)
	var callers []call.Unanswered
	for {
		half, expired := m.popExpired(at)
		if !expired {
			return callers
		}
		if caller, unanswered := m.settleExpired(half, at); unanswered {
			callers = append(callers, caller)
		}
	}
}

func (m *Matcher) isDuplicate(arrived pendingHalf, waiting *list.Element) bool {
	if m.Tombstones.Suppresses(arrived.Key, arrived.Side) {
		return true
	}
	return waiting != nil && halfOf(waiting).sameSideAs(arrived)
}

func (m *Matcher) complete(waiting *list.Element, arrived pendingHalf, at time.Duration) call.Call {
	other := m.remove(waiting)
	m.Stats.RecordMatch()
	m.addTombstone(arrived.Key, tombstone.BothSides, at)
	return other.callWith(arrived)
}

func (m *Matcher) popExpired(at time.Duration) (pendingHalf, bool) {
	front := m.Pending.Front()
	if front == nil || halfOf(front).Deadline > at {
		return pendingHalf{}, false
	}
	half := m.remove(front)
	m.Stats.RecordExpiry(half.Side)
	return half, true
}

func (m *Matcher) settleExpired(half pendingHalf, at time.Duration) (call.Unanswered, bool) {
	unanswered, isCaller := half.unanswered()
	if isCaller {
		m.addTombstone(half.Key, tombstone.CallerOnly, at)
	}
	return unanswered, isCaller
}

func (m *Matcher) wait(half pendingHalf) {
	if m.Pending.Len() >= m.Limits.MaxPending {
		m.evictOldest()
	}
	m.PendingByKey[half.Key] = m.Pending.PushBack(half)
	m.Stats.RecordWait()
}

func (m *Matcher) evictOldest() {
	oldest := m.Pending.Front()
	half := m.remove(oldest)
	m.Stats.RecordEviction(half.Side)
}

func (m *Matcher) remove(element *list.Element) pendingHalf {
	half := halfOf(element)
	m.Pending.Remove(element)
	delete(m.PendingByKey, half.Key)
	return half
}

func (m *Matcher) addTombstone(key ids.CallKey, scope tombstone.Scope, at time.Duration) {
	if m.Tombstones.Add(key, scope, at+m.Limits.TombstoneTTL) {
		m.Stats.RecordTombstoneEviction()
	}
}

func (h pendingHalf) sameSideAs(other pendingHalf) bool {
	return h.Side == other.Side
}

func (h pendingHalf) callWith(other pendingHalf) call.Call {
	caller, callee := h, other
	if h.Side == role.Callee {
		caller, callee = other, h
	}
	return call.Between(caller.party(), callee.party())
}

func (h pendingHalf) unanswered() (call.Unanswered, bool) {
	if h.Side != role.Caller {
		return call.Unanswered{}, false
	}
	return call.Unanswered{
		Service: h.Service,
		Failed:  h.Failed,
	}, true
}

func (h pendingHalf) party() call.Party {
	return call.Party{
		Service: h.Service,
		Failed:  h.Failed,
	}
}

func halfOf(element *list.Element) pendingHalf {
	return element.Value.(pendingHalf)
}
