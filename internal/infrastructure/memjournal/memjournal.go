package memjournal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

var (
	ErrInvalidLimits = errors.New("invalid journal limits")
	ErrInvalidMax    = errors.New("receive limit must be positive")
)

type Limits struct {
	MaxBytes   int64
	MaxBatches int
}

type Journal struct {
	limits Limits
	lock   sync.Mutex
	queue  chan entry
	closed bool
	held   Stats
}

func New(limits Limits) (*Journal, error) {
	if limits.MaxBytes < 1 || limits.MaxBatches < 1 {
		return nil, fmt.Errorf("%w: %+v", ErrInvalidLimits, limits)
	}
	return &Journal{
		limits: limits,
		queue:  make(chan entry, limits.MaxBatches),
	}, nil
}

func (j *Journal) Append(ctx context.Context, received batch.Batch) error {
	next := newEntry(received)
	if next.size > j.limits.MaxBytes {
		return fmt.Errorf("append %d bytes, capacity %d: %w", next.size, j.limits.MaxBytes, ingest.ErrBatchTooLarge)
	}
	j.lock.Lock()
	defer j.lock.Unlock()
	if j.closed {
		return ingest.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	return j.enqueueLocked(next)
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

func (j *Journal) enqueueLocked(next entry) error {
	if j.held.Bytes+next.size > j.limits.MaxBytes {
		return fmt.Errorf(
			"append %d bytes, %d of %d held: %w",
			next.size,
			j.held.Bytes,
			j.limits.MaxBytes,
			ingest.ErrOverloaded,
		)
	}
	select {
	case j.queue <- next:
		j.held = j.held.with(next)
		return nil
	default:
		return fmt.Errorf("append: queue of %d batches is full: %w", j.limits.MaxBatches, ingest.ErrOverloaded)
	}
}
