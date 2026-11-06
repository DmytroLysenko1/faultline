package processing

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const MaxBatchesPerIteration = 64

type spanFeed interface {
	Receive(ctx context.Context, limit int) ([]Delivery, error)
}

type graphPublisher interface {
	Publish(ctx context.Context, snapshot *topology.Snapshot) error
}

type Pipeline struct {
	feed      spanFeed
	builder   *topology.Builder
	publisher graphPublisher
	started   atomic.Bool
}

func NewPipeline(feed spanFeed, builder *topology.Builder, publisher graphPublisher) *Pipeline {
	return &Pipeline{
		feed:      feed,
		builder:   builder,
		publisher: publisher,
	}
}

func (p *Pipeline) Run(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	for {
		if err := p.iterate(ctx); err != nil {
			return stopReason(ctx, err)
		}
	}
}

func (p *Pipeline) iterate(ctx context.Context) error {
	deliveries, err := p.receive(ctx)
	if err != nil {
		return err
	}
	p.apply(deliveries)
	if err := p.publish(ctx, len(deliveries)); err != nil {
		return err
	}
	release(deliveries)
	return nil
}

func (p *Pipeline) receive(ctx context.Context) ([]Delivery, error) {
	deliveries, err := p.feed.Receive(ctx, MaxBatchesPerIteration)
	if err != nil {
		return nil, fmt.Errorf("receive batches: %w", err)
	}
	if len(deliveries) == 0 {
		return nil, ErrEmptyReceive
	}
	return deliveries, nil
}

func (p *Pipeline) apply(deliveries []Delivery) {
	for _, delivery := range deliveries {
		p.builder.Apply(delivery.Batch())
	}
}

func (p *Pipeline) publish(ctx context.Context, batches int) error {
	if err := p.publisher.Publish(ctx, p.builder.Snapshot()); err != nil {
		return fmt.Errorf("publish iteration of %d batches: %w", batches, err)
	}
	return nil
}

func release(deliveries []Delivery) {
	for _, delivery := range deliveries {
		delivery.Release()
	}
}

func stopReason(ctx context.Context, err error) error {
	if errors.Is(err, ErrFeedClosed) || canceledByCaller(ctx, err) {
		return nil
	}
	return err
}

func canceledByCaller(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled)
}
