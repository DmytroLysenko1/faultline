package processing

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	MaxBatchesPerIteration = 64
	HeldSnapshots          = 1
)

type spanFeed interface {
	Receive(ctx context.Context, limit int, maxWait time.Duration) ([]Delivery, error)
}

type graphPublisher interface {
	Publish(ctx context.Context, snapshot *topology.Snapshot) error
}

type Pipeline struct {
	feed        spanFeed
	builder     *topology.Builder
	publisher   graphPublisher
	clock       clock
	cadence     Cadence
	held        []Delivery
	heldBytes   int64
	nextFire    time.Time
	checkpoints checkpointer
	started     atomic.Bool
	measured    Timings
	timings     timings
}

func NewPipeline(feed spanFeed, builder *topology.Builder, publisher graphPublisher, sink checkpointSink,
	clock clock, cadence Cadence,
) (*Pipeline, error) {
	if err := cadence.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, fmt.Errorf("%w: nil clock", ErrInvalidCadence)
	}
	if cadence.CheckpointInterval > 0 && sink == nil {
		return nil, fmt.Errorf("%w: checkpoint interval %s without a checkpoint store", ErrInvalidCadence,
			cadence.CheckpointInterval)
	}
	return &Pipeline{
		feed:      feed,
		builder:   builder,
		publisher: publisher,
		clock:     clock,
		cadence:   cadence,
		checkpoints: checkpointer{
			sink:     sink,
			interval: cadence.CheckpointInterval,
		},
	}, nil
}

func (p *Pipeline) Run(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	now := p.clock.Now()
	p.nextFire = now.Add(p.cadence.FirePeriod)
	p.checkpoints.start(now)
	for {
		if err := p.iterate(ctx); err != nil {
			return p.stop(ctx, err)
		}
	}
}

func (p *Pipeline) TakeTimings() Timings {
	return p.timings.take()
}

func (p *Pipeline) CheckpointStats() CheckpointStats {
	return p.checkpoints.counters.read()
}

func (p *Pipeline) iterate(ctx context.Context) error {
	deliveries, err := p.feed.Receive(ctx, MaxBatchesPerIteration, p.untilFire())
	if err != nil {
		return fmt.Errorf("receive batches: %w", err)
	}
	started := p.clock.Now()
	defer p.recordIteration(started)
	p.apply(deliveries)
	if !p.due() {
		return nil
	}
	if err := p.publish(ctx); err != nil {
		return err
	}
	return p.checkpointIfDue(ctx)
}

func (p *Pipeline) checkpointIfDue(ctx context.Context) error {
	if !p.checkpoints.due(p.clock.Now()) {
		return nil
	}
	if err := p.checkpoint(ctx); canceledByCaller(ctx, err) {
		return err
	}
	return nil
}

func (p *Pipeline) recordIteration(started time.Time) {
	p.measured.Iteration = p.since(started)
	p.timings.record(p.measured)
	p.measured = Timings{}
}

func (p *Pipeline) since(started time.Time) time.Duration {
	return max(p.clock.Now().Sub(started), 0)
}

func (p *Pipeline) untilFire() time.Duration {
	return min(max(p.nextFire.Sub(p.clock.Now()), 0), p.cadence.FirePeriod)
}

func (p *Pipeline) apply(deliveries []Delivery) {
	for _, delivery := range deliveries {
		started := p.clock.Now()
		p.builder.Apply(delivery.Batch())
		p.measured.Apply = max(p.measured.Apply, p.since(started))
		p.held = append(p.held, delivery)
		p.heldBytes += delivery.Bytes()
		p.checkpoints.applied(delivery)
	}
}

func (p *Pipeline) due() bool {
	if now := p.clock.Now(); reached(now, p.nextFire, p.cadence.FirePeriod) {
		p.builder.Fire()
		p.measured.Fire = p.since(now)
		p.nextFire = now.Add(p.cadence.FirePeriod)
		return true
	}
	return p.builder.StructureChanged() || p.heldBytes >= p.cadence.MaxHeldBytes
}

func (p *Pipeline) publish(ctx context.Context) error {
	started := p.clock.Now()
	err := p.publisher.Publish(ctx, p.builder.Snapshot())
	p.measured.Publish = p.since(started)
	if err != nil {
		return fmt.Errorf("publish after %d held batches: %w", len(p.held), err)
	}
	for _, delivery := range p.held {
		delivery.Release()
	}
	clear(p.held)
	p.held, p.heldBytes = p.held[:0], 0
	return nil
}

func (p *Pipeline) checkpoint(ctx context.Context) error {
	started := p.clock.Now()
	err := p.checkpoints.write(ctx, p.builder, started)
	p.measured.Checkpoint = p.since(started)
	return err
}

func (p *Pipeline) stop(ctx context.Context, err error) error {
	if errors.Is(err, ErrFeedClosed) {
		return p.drain(ctx)
	}
	if canceledByCaller(ctx, err) {
		return nil
	}
	return err
}

func (p *Pipeline) drain(ctx context.Context) error {
	defer p.recordIteration(p.clock.Now())
	err := p.publish(ctx)
	if canceledByCaller(ctx, err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	if !p.checkpoints.pendingAtStop() {
		return nil
	}
	if err := p.checkpoint(ctx); lostAtStop(ctx, err) {
		return fmt.Errorf("drain: final checkpoint: %w", err)
	}
	return nil
}

func lostAtStop(ctx context.Context, err error) bool {
	return err != nil && !errors.Is(err, ErrCheckpointBusy) && !canceledByCaller(ctx, err)
}

func canceledByCaller(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled)
}
