package admission

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const EntryHeapBytes = 224

var (
	ErrShareExceeded   = errors.New("ingest token holds its share of the request slots")
	ErrSlotsFull       = errors.New("ingest request slots are full")
	ErrRateExceeded    = errors.New("ingest request rate budget is spent")
	ErrSlotHeldTooLong = errors.New("ingest request body was not received within the slot hold limit")
	ErrInvalidLimits   = errors.New("invalid admission limits")
	ErrUnknownReason   = errors.New("ingest request refused for an unknown reason")
)

type Reason uint8

const (
	ReasonShare Reason = iota
	ReasonSlots
	ReasonRate
	ReasonCount
)

const unknownReason = "unknown"

var reasonNames = [ReasonCount]string{
	ReasonShare: "share",
	ReasonSlots: "slots",
	ReasonRate:  "rate",
}

var reasonSentinels = [ReasonCount]error{
	ReasonShare: ErrShareExceeded,
	ReasonSlots: ErrSlotsFull,
	ReasonRate:  ErrRateExceeded,
}

func (r Reason) String() string {
	if r >= ReasonCount {
		return unknownReason
	}
	return reasonNames[r]
}

func (r Reason) sentinel() error {
	if r >= ReasonCount {
		return ErrUnknownReason
	}
	return reasonSentinels[r]
}

type clock interface {
	Now() time.Time
}

type connectionMarks interface {
	Authenticated(remote net.Addr, id credential.ID)
}

type RejectionError struct {
	Reason        Reason
	TokenID       credential.ID
	TopHolderID   credential.ID
	TopHolderHeld int
}

func (r RejectionError) Error() string {
	return r.Unwrap().Error()
}

func (r RejectionError) Unwrap() error {
	return r.Reason.sentinel()
}

type Limits struct {
	Slots             int
	MinShare          int
	ActiveWindow      time.Duration
	WaitingWindow     time.Duration
	RequestsPerSecond int
}

func (l Limits) validate() error {
	if min(l.Slots, l.MinShare, l.RequestsPerSecond) < 1 {
		return fmt.Errorf("slots %d, min share %d, requests per second %d: %w",
			l.Slots, l.MinShare, l.RequestsPerSecond, ErrInvalidLimits)
	}
	if l.ActiveWindow <= 0 || l.WaitingWindow < l.ActiveWindow {
		return fmt.Errorf("active window %s, waiting window %s: %w", l.ActiveWindow, l.WaitingWindow, ErrInvalidLimits)
	}
	return nil
}

type Stats struct {
	InUse        int
	ActiveTokens int
	Admitted     uint64
	Rejected     [ReasonCount]uint64
	TooSlow      uint64
}

type Admission struct {
	Limits Limits
	Clock  clock
	lock   sync.Mutex
	book   book
	stats  Stats
}

func New(limits Limits, clock clock) (*Admission, error) {
	if clock == nil {
		return nil, fmt.Errorf("no clock: %w", ErrInvalidLimits)
	}
	if err := limits.validate(); err != nil {
		return nil, err
	}
	rate := float64(limits.RequestsPerSecond)
	return &Admission{
		Limits: limits,
		Clock:  clock,
		book: book{
			entries: map[credential.ID]*entry{},
			holders: map[*entry]credential.ID{},
			instance: bucket{
				tokens: rate,
				last:   clock.Now(),
			},
		},
	}, nil
}

func (a *Admission) Stats() Stats {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.book.prune(a.Clock.Now(), a.Limits.ActiveWindow)
	stats := a.stats
	stats.InUse = a.book.inUse
	stats.ActiveTokens = len(a.book.entries)
	return stats
}

func (a *Admission) SlotHeldTooLong() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.stats.TooSlow++
}

func (a *Admission) admit(id credential.ID) (func(), error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	now := a.Clock.Now()
	a.book.prune(now, a.Limits.ActiveWindow)
	held := a.book.touch(id, now, a.rate())
	if reason, refused := a.refusal(held, now); refused {
		return nil, a.reject(id, held, reason, now)
	}
	a.book.take(held)
	a.stats.Admitted++
	return sync.OnceFunc(func() {
		a.release(held)
	}), nil
}

func (a *Admission) refusal(held *entry, now time.Time) (Reason, bool) {
	if held.held >= a.share() {
		return ReasonShare, true
	}
	if a.book.inUse >= a.Limits.Slots {
		return ReasonSlots, true
	}
	if !a.book.allowBoth(held, now, a.rate()) {
		return ReasonRate, true
	}
	return ReasonCount, false
}

func (a *Admission) share() int {
	waiting := len(a.book.entries)
	return max(a.Limits.MinShare, (a.Limits.Slots+waiting-1)/waiting)
}

func (a *Admission) rate() float64 {
	return float64(a.Limits.RequestsPerSecond)
}

func (a *Admission) reject(id credential.ID, held *entry, reason Reason, now time.Time) error {
	a.stats.Rejected[reason]++
	rejection := RejectionError{
		Reason:  reason,
		TokenID: id,
	}
	if reason == ReasonRate {
		return rejection
	}
	held.waitingUntil = now.Add(a.Limits.WaitingWindow)
	rejection.TopHolderID, rejection.TopHolderHeld = a.book.topHolder()
	return rejection
}

func (a *Admission) release(held *entry) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.book.give(held, a.Clock.Now())
}

type Gate struct {
	Admission *Admission
	Marks     connectionMarks
}

func (g Gate) Enter(id credential.ID, remote net.Addr) (func(), error) {
	g.Marks.Authenticated(remote, id)
	return g.Admission.admit(id)
}
