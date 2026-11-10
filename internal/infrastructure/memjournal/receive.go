package memjournal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

type delivery struct {
	entry   entry
	release func()
}

func (j *Journal) Receive(ctx context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error) {
	if limit < 1 {
		return nil, ErrInvalidMax
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("receive: %w", err)
	}
	j.pulseIfIdle()
	first, found, err := j.await(ctx, maxWait)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return j.collect(&first, limit), nil
}

func (j *Journal) await(ctx context.Context, maxWait time.Duration) (entry, bool, error) {
	first, found, err := j.poll()
	if found || err != nil || maxWait <= 0 {
		return first, found, err
	}
	return j.wait(ctx, maxWait)
}

func (j *Journal) wait(ctx context.Context, maxWait time.Duration) (entry, bool, error) {
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return entry{}, false, fmt.Errorf("receive: %w", ctx.Err())
	case first, open := <-j.queue:
		return first, open, closedFeed(open)
	case <-timer.C:
		j.pulseIfIdle()
		return j.poll()
	}
}

func (j *Journal) poll() (entry, bool, error) {
	select {
	case first, open := <-j.queue:
		return first, open, closedFeed(open)
	default:
		return entry{}, false, nil
	}
}

func closedFeed(open bool) error {
	if open {
		return nil
	}
	return processing.ErrFeedClosed
}

func (j *Journal) collect(first *entry, limit int) []processing.Delivery {
	deliveries := make([]processing.Delivery, 0, min(limit, len(j.queue)+1))
	deliveries = append(deliveries, j.deliver(first))
	for len(deliveries) < limit {
		next, ready := j.ready()
		if !ready {
			break
		}
		deliveries = append(deliveries, j.deliver(&next))
	}
	return deliveries
}

func (j *Journal) ready() (entry, bool) {
	select {
	case next, open := <-j.queue:
		return next, open
	default:
		return entry{}, false
	}
}

func (j *Journal) deliver(delivered *entry) *delivery {
	created := &delivery{entry: *delivered}
	created.release = sync.OnceFunc(func() {
		j.release(&created.entry)
	})
	return created
}

func (j *Journal) release(released *entry) {
	j.lock.Lock()
	defer j.lock.Unlock()
	j.held = j.held.without(released)
}

func (d *delivery) Batch() batch.Batch {
	return d.entry.batch
}

func (d *delivery) Bytes() int64 {
	return d.entry.size
}

func (d *delivery) Release() {
	d.release()
}
