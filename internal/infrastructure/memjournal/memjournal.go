package memjournal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/idlerule"
)

var (
	ErrInvalidLimits = errors.New("invalid journal limits")
	ErrInvalidMax    = errors.New("receive limit must be positive")
)

type Limits struct {
	MaxBytes   int64
	MaxBatches int
	IdleAfter  time.Duration
}

func (l Limits) valid() bool {
	return l.MaxBytes >= 1 && l.MaxBatches >= 1 && l.IdleAfter > 0
}

type Clock interface {
	Now() time.Time
}

type Journal struct {
	limits    Limits
	source    Clock
	lock      sync.Mutex
	monotonic clock.Monotonic
	idle      idlerule.Rule
	queue     chan entry
	closed    bool
	held      Stats
	appended  uint64
}

func New(limits Limits, source Clock) (*Journal, error) {
	if source == nil {
		panic("memjournal: New needs a clock")
	}
	if !limits.valid() {
		return nil, fmt.Errorf("%w: %+v", ErrInvalidLimits, limits)
	}
	return &Journal{
		limits: limits,
		source: source,
		idle: idlerule.Rule{
			After: limits.IdleAfter,
		},
		queue: make(chan entry, limits.MaxBatches),
	}, nil
}

func (j *Journal) Append(ctx context.Context, spans []span.Span) error {
	next, err := j.measure(spans)
	if err != nil {
		return err
	}
	j.lock.Lock()
	defer j.lock.Unlock()
	if j.closed {
		return errors.Join(ingest.ErrUnavailable, ingest.ErrNotAttempted)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	return j.enqueueLocked(&next)
}

func (j *Journal) Close() {
	j.lock.Lock()
	defer j.lock.Unlock()
	if j.closed {
		return
	}
	j.closed = true
	close(j.queue)
}

func (j *Journal) Stats() Stats {
	j.lock.Lock()
	defer j.lock.Unlock()
	return j.held
}

func (j *Journal) measure(spans []span.Span) (entry, error) {
	unstamped, err := batch.NewOwning(spans)
	if err != nil {
		return entry{}, fmt.Errorf("append: %w", err)
	}
	measured := entry{
		batch: unstamped,
		size:  unstamped.Footprint(),
	}
	if capacity := j.limits.MaxBytes; measured.size > capacity {
		return entry{}, fmt.Errorf("append %d bytes, capacity %d: %w", measured.size, capacity, ingest.ErrBatchTooLarge)
	}
	return measured, nil
}

func (j *Journal) enqueueLocked(next *entry) error {
	if err := j.roomLocked(next); err != nil {
		return err
	}
	next.batch.ReceivedAt = j.stampLocked()
	if j.idle.DueBefore(next.batch) && j.roomForHeartbeatLocked() {
		j.pushLocked(new(heartbeatAt(next.batch.ReceivedAt)))
	}
	j.pushLocked(next)
	return nil
}

func (j *Journal) roomLocked(next *entry) error {
	if held, capacity := j.held.Bytes, j.limits.MaxBytes; held+next.size > capacity {
		return fmt.Errorf("append %d bytes, %d of %d held: %w", next.size, held, capacity, ingest.ErrOverloaded)
	}
	if j.freeSlotsLocked() == 0 {
		return fmt.Errorf("append: queue of %d batches is full: %w", j.limits.MaxBatches, ingest.ErrOverloaded)
	}
	return nil
}

func (j *Journal) pulseIfIdle() {
	j.lock.Lock()
	defer j.lock.Unlock()
	if j.closed || !j.roomForHeartbeatLocked() {
		return
	}
	now := j.stampLocked()
	if j.idle.Due(now) {
		j.pushLocked(new(heartbeatAt(now)))
	}
}

func (j *Journal) freeSlotsLocked() int {
	return cap(j.queue) - len(j.queue)
}

func (j *Journal) roomForHeartbeatLocked() bool {
	return j.freeSlotsLocked() > 1
}

func (j *Journal) pushLocked(next *entry) {
	j.appended++
	next.sequence = j.appended
	j.queue <- *next
	j.idle.Record(next.batch)
	j.held = j.held.with(next)
}

func (j *Journal) stampLocked() time.Time {
	j.monotonic.Advance(j.source.Now())
	return j.monotonic.Now()
}
