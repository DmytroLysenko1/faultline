package feed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/semaphore"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	pullHeartbeat    = 5 * time.Second
	nextWait         = 3 * pullHeartbeat
	identityAttempt  = 2 * time.Second
	identityRetryGap = time.Second
	identityBudget   = 35 * time.Second
)

type pump struct {
	reader       reader
	stream       partitionStream
	letters      deadLetters
	pulses       pulses
	lease        partitionLease
	created      time.Time
	lastAtOpen   uint64
	partition    uint16
	maxFootprint int64
	credit       *semaphore.Weighted
	out          chan<- *delivery
	counters     *counters
	checked      string
	last         uint64
	receivedAt   time.Time
	window       window
	seeded       bool
	idle         idle
}

func newPump(source *source, start *Start, options *Options, feed *Feed) *pump {
	return &pump{
		reader:       source.reader,
		stream:       source.stream,
		letters:      source.letters,
		pulses:       source.pulses,
		lease:        feed.lease,
		created:      source.identity.Created,
		lastAtOpen:   source.identity.Last,
		partition:    options.Partition,
		maxFootprint: options.MaxFootprint,
		credit:       semaphore.NewWeighted(options.MaxBytes),
		out:          feed.deliveries,
		counters:     feed.counters,
		checked:      source.reader.Consumer(),
		last:         start.After.Sequence,
		receivedAt:   start.ReceivedAt,
		idle:         newIdle(start, options.IdleAfter),
	}
}

func (p *pump) run(ctx context.Context) error {
	for {
		if err := p.step(ctx); err != nil {
			return err
		}
	}
}

func (p *pump) step(ctx context.Context) error {
	next, found, err := p.next(ctx)
	if err != nil {
		return err
	}
	if err := p.confirm(ctx); err != nil {
		return err
	}
	if err := p.take(ctx, &next, found); err != nil {
		return err
	}
	return p.beat(ctx)
}

func (p *pump) take(ctx context.Context, next *message, found bool) error {
	p.idle.observed(found, next.pending)
	if !found {
		return nil
	}
	return p.deliver(ctx, next)
}

func (p *pump) next(ctx context.Context) (message, bool, error) {
	next, err := p.reader.Next(nextWait)
	if err == nil {
		p.counters.read(&next)
		return next, true, nil
	}
	if ctx.Err() != nil {
		return message{}, false, ctx.Err()
	}
	if errors.Is(err, nats.ErrTimeout) {
		return message{}, false, nil
	}
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return message{}, false, fmt.Errorf("%w: the stream is gone: %w", ErrStreamIdentity, err)
	}
	return message{}, false, fmt.Errorf("%w: %w", ErrConsumerLost, err)
}

func (p *pump) confirm(ctx context.Context) error {
	name := p.reader.Consumer()
	if name == p.checked {
		return nil
	}
	p.counters.resets.Add(1)
	if err := p.verify(ctx); err != nil {
		return err
	}
	p.checked = name
	return nil
}

func (p *pump) verify(ctx context.Context) error {
	giveUp := time.Now().Add(identityBudget)
	for {
		again, err := p.verifyOnce(ctx, giveUp)
		if !again {
			return err
		}
	}
}

func (p *pump) verifyOnce(ctx context.Context, giveUp time.Time) (bool, error) {
	err := p.identify(ctx)
	if !retryable(ctx, err) {
		return false, settled(ctx, err)
	}
	p.counters.identityFailures.Add(1)
	if time.Now().Add(identityRetryGap).After(giveUp) {
		return false, fmt.Errorf("%w: the stream did not answer for %s: %w", ErrConsumerLost, identityBudget, err)
	}
	if err := pause(ctx, identityRetryGap); err != nil {
		return false, err
	}
	return true, nil
}

func (p *pump) identify(ctx context.Context) error {
	attempt, cancel := context.WithTimeout(ctx, identityAttempt)
	defer cancel()
	found, err := p.stream.Describe(attempt)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("%w: the stream is gone: %w", ErrStreamIdentity, err)
	}
	if err != nil {
		return err
	}
	if !found.Created.Equal(p.created) {
		return fmt.Errorf("%w: created %s, read from %s", ErrStreamIdentity, found.Created, p.created)
	}
	return nil
}

func retryable(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() == nil && natsjs.Temporary(err)
}

func settled(ctx context.Context, err error) error {
	if err == nil || errors.Is(err, ErrStreamIdentity) {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%w: stream info: %w", ErrConsumerLost, err)
}

func (p *pump) deliver(ctx context.Context, next *message) error {
	if err := p.advance(next.sequence); err != nil {
		return err
	}
	header, err := record.ReadHeader(next.data, p.partition, p.maxFootprint)
	if err != nil {
		return p.reject(ctx, next, err)
	}
	if err := p.credit.Acquire(ctx, header.Weight); err != nil {
		return err
	}
	made, err := p.decode(next, &header)
	if err != nil {
		p.credit.Release(header.Weight)
		return p.reject(ctx, next, err)
	}
	p.idle.delivered(made.batch)
	return p.send(ctx, made)
}

func (p *pump) advance(sequence uint64) error {
	if sequence <= p.last {
		return fmt.Errorf("%w: position %d after %d", ErrConsumerLost, sequence, p.last)
	}
	if p.last > 0 {
		p.counters.gaps.Add(sequence - p.last - 1)
	}
	p.last = sequence
	return nil
}

func (p *pump) decode(next *message, header *record.Header) (*delivery, error) {
	position := processing.Position{
		Partition: p.partition,
		Sequence:  next.sequence,
	}
	if header.Kind == record.KindHeartbeat {
		return p.delivery(batch.Heartbeat(p.stamp(next.stamped, header.WrittenAt)), position, 0), nil
	}
	p.counters.skewed(next.stamped.Sub(header.WrittenAt).Abs())
	spans, err := record.Decode(next.data, *header)
	if err != nil {
		return nil, err
	}
	decoded, err := batch.NewOwning(spans)
	if err != nil {
		return nil, err
	}
	decoded.ReceivedAt = p.stamp(next.stamped, header.WrittenAt)
	return p.delivery(decoded, position, header.Weight), nil
}

func (p *pump) stamp(leader, writer time.Time) time.Time {
	observed := leader
	if writer.Before(leader) {
		observed = writer
	}
	if observed.Before(p.receivedAt) {
		p.counters.clamped.Add(1)
		return p.receivedAt
	}
	p.receivedAt = observed
	return observed
}

func (p *pump) delivery(made batch.Batch, position processing.Position, weight int64) *delivery {
	return &delivery{
		batch:    made,
		bytes:    weight,
		position: position,
		credit:   p.credit,
		counters: p.counters,
	}
}

func (p *pump) send(ctx context.Context, made *delivery) error {
	select {
	case p.out <- made:
		p.counters.handed(made.batch.Heartbeat)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func pause(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
