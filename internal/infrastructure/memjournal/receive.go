package memjournal

import (
	"context"
	"fmt"
	"sync"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

type delivery struct {
	entry   entry
	release func()
}

func (j *Journal) Receive(ctx context.Context, limit int) ([]processing.Delivery, error) {
	if limit < 1 {
		return nil, ErrInvalidMax
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("receive: %w", ctx.Err())
	case first, open := <-j.queue:
		if !open {
			return nil, processing.ErrFeedClosed
		}
		return j.collect(first, limit), nil
	}
}

func (j *Journal) collect(first entry, limit int) []processing.Delivery {
	deliveries := make([]processing.Delivery, 0, min(limit, len(j.queue)+1))
	deliveries = append(deliveries, j.deliver(first))
	for len(deliveries) < limit {
		next, ready := j.ready()
		if !ready {
			break
		}
		deliveries = append(deliveries, j.deliver(next))
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

func (j *Journal) deliver(delivered entry) *delivery {
	return &delivery{
		entry: delivered,
		release: sync.OnceFunc(func() {
			j.release(delivered)
		}),
	}
}

func (j *Journal) release(released entry) {
	j.lock.Lock()
	defer j.lock.Unlock()
	j.held = j.held.without(released)
}

func (d *delivery) Batch() batch.Batch {
	return d.entry.batch
}

func (d *delivery) Release() {
	d.release()
}
