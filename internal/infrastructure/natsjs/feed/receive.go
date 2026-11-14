package feed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

func (f *Feed) Receive(ctx context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error) {
	if limit < 1 {
		return nil, ErrInvalidLimit
	}
	if err := f.refusal(ctx); err != nil {
		return nil, err
	}
	first, err := f.await(ctx, maxWait)
	if err != nil || first == nil {
		return nil, err
	}
	return f.handOut(first, limit)
}

func (f *Feed) handOut(first *delivery, limit int) ([]processing.Delivery, error) {
	deliveries := f.collect(first, limit)
	if err := f.failure(); err != nil {
		for _, dropped := range deliveries {
			dropped.Release()
		}
		return nil, err
	}
	return deliveries, nil
}

func (f *Feed) refusal(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("receive: %w", err)
	}
	return f.failure()
}

func (f *Feed) afterHalt() (*delivery, error) {
	if err := f.failure(); err != nil {
		return nil, err
	}
	if next, found := f.ready(); found {
		return next, nil
	}
	return nil, f.closed()
}

func (f *Feed) failure() error {
	lost := f.leaseLost()
	if lost {
		f.halt(ErrLeaseLost)
	}
	if !f.isHalted() {
		return nil
	}
	if !errors.Is(f.cause, processing.ErrFeedClosed) {
		return f.cause
	}
	if lost {
		return ErrLeaseLost
	}
	return nil
}

func (f *Feed) await(ctx context.Context, maxWait time.Duration) (*delivery, error) {
	if f.drained.Load() {
		return nil, f.cause
	}
	if next, found := f.ready(); found {
		return next, nil
	}
	if f.isHalted() {
		return nil, f.closed()
	}
	if maxWait <= 0 {
		return nil, nil
	}
	return f.wait(ctx, maxWait)
}

func (f *Feed) wait(ctx context.Context, maxWait time.Duration) (*delivery, error) {
	f.timer.Reset(maxWait)
	defer f.timer.Stop()
	select {
	case next := <-f.deliveries:
		return next, nil
	case <-f.halted:
		return f.afterHalt()
	case <-f.timer.C:
		return nil, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("receive: %w", ctx.Err())
	}
}

func (f *Feed) closed() error {
	f.drained.Store(true)
	return f.cause
}

func (f *Feed) leaseLost() bool {
	select {
	case <-f.lease.Lost():
		return true
	default:
		return false
	}
}

func (f *Feed) isHalted() bool {
	select {
	case <-f.halted:
		return true
	default:
		return false
	}
}

func (f *Feed) collect(first *delivery, limit int) []processing.Delivery {
	deliveries := make([]processing.Delivery, 0, min(limit, len(f.deliveries)+1))
	deliveries = append(deliveries, first)
	for len(deliveries) < limit {
		next, found := f.ready()
		if !found {
			break
		}
		deliveries = append(deliveries, next)
	}
	return deliveries
}

func (f *Feed) ready() (*delivery, bool) {
	select {
	case next := <-f.deliveries:
		return next, true
	default:
		return nil, false
	}
}
