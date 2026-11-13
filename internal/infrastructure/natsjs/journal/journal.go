package journal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var (
	ErrInvalidOptions = errors.New("invalid journal options")
	errClosing        = errors.New("journal is closing")
	errDoomed         = errors.New("too little of the deadline left to publish")
	errPublishTimeout = errors.New("publish ran out of its budget")
)

const (
	publishReserve = 100 * time.Millisecond
	minAttempt     = 50 * time.Millisecond
)

type recordPublisher interface {
	Publish(ctx context.Context, partition uint16, data []byte, msgID string) (bool, error)
}

type Clock interface {
	Now() time.Time
}

type Options struct {
	Tenant         string
	Topology       natsjs.Topology
	RulesVersion   uint32
	PublishTimeout time.Duration
	Clock          Clock
}

func (o *Options) validate() error {
	if o == nil {
		return fmt.Errorf("%w: no options", ErrInvalidOptions)
	}
	for _, check := range []func() error{o.validateIdentity, o.validateTopology, o.validateTimeout} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (o *Options) validateIdentity() error {
	if o.Clock == nil {
		return fmt.Errorf("%w: no clock", ErrInvalidOptions)
	}
	if err := record.ValidateTenant(o.Tenant); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	return nil
}

func (o *Options) validateTopology() error {
	if err := o.Topology.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	return nil
}

func (o *Options) validateTimeout() error {
	if o.PublishTimeout <= publishReserve+minAttempt {
		return fmt.Errorf("%w: publish timeout %s must exceed %s", ErrInvalidOptions, o.PublishTimeout,
			publishReserve+minAttempt)
	}
	return nil
}

type Journal struct {
	publisher recordPublisher
	options   Options
	encoder   record.Encoder
	lock      sync.Mutex
	closed    bool
	inflight  sync.WaitGroup
	drained   func() <-chan struct{}
	counters  counters
}

func New(publisher recordPublisher, options *Options) (*Journal, error) {
	if publisher == nil {
		return nil, fmt.Errorf("%w: no publisher", ErrInvalidOptions)
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	journal := &Journal{
		publisher: publisher,
		options:   *options,
	}
	journal.drained = sync.OnceValue(journal.waitForAppends)
	return journal, nil
}

func (j *Journal) Append(ctx context.Context, spans []span.Span) error {
	if len(spans) == 0 {
		return nil
	}
	if err := j.enter(); err != nil {
		return err
	}
	defer j.inflight.Done()
	budget, err := j.budget(ctx)
	if err != nil {
		return err
	}
	started := j.options.Clock.Now()
	defer func() { j.counters.observeAppend(j.options.Clock.Now().Sub(started)) }()
	publishCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return j.outcome(ctx, j.publishAll(publishCtx, groupByPartition(spans, j.options.Topology.Partitions)))
}

func (j *Journal) Close(ctx context.Context) error {
	j.lock.Lock()
	j.closed = true
	j.lock.Unlock()
	drained := j.drained()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
	}
	select {
	case <-drained:
		return nil
	default:
		return fmt.Errorf("close journal: %w", ctx.Err())
	}
}

func (j *Journal) waitForAppends() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		j.inflight.Wait()
	}()
	return done
}

func (j *Journal) Stats() Stats {
	return j.counters.snapshot()
}

func (j *Journal) enter() error {
	j.lock.Lock()
	defer j.lock.Unlock()
	if j.closed {
		j.counters.closing.Add(1)
		return fmt.Errorf("append: %w", errors.Join(ingest.ErrUnavailable, errClosing))
	}
	j.inflight.Add(1)
	return nil
}

func (j *Journal) budget(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		j.counters.interrupted.Add(1)
		return 0, fmt.Errorf("append: %w", err)
	}
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return j.options.PublishTimeout, nil
	}
	remaining := time.Until(deadline)
	if remaining <= publishReserve+minAttempt {
		j.counters.doomed.Add(1)
		return 0, fmt.Errorf("append with %s left: %w", remaining, errors.Join(ingest.ErrUnavailable, errDoomed))
	}
	return min(j.options.PublishTimeout, remaining-publishReserve), nil
}

func (j *Journal) outcome(ctx context.Context, errs []error) error {
	failure := firstFailure(errs)
	if failure == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		j.counters.interrupted.Add(1)
		return fmt.Errorf("append: %w", err)
	}
	for _, sentinel := range []error{ingest.ErrBatchTooLarge, ingest.ErrOverloaded} {
		if err := firstMatching(errs, sentinel); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}
	return fmt.Errorf("append: %w", withoutDeadline(failure))
}

func withoutDeadline(failure error) error {
	if !errors.Is(failure, context.DeadlineExceeded) {
		return failure
	}
	return fmt.Errorf("%w: %s", errors.Join(ingest.ErrUnavailable, errPublishTimeout), failure.Error())
}

func firstMatching(errs []error, target error) error {
	for _, err := range errs {
		if errors.Is(err, target) {
			return err
		}
	}
	return nil
}

func firstFailure(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
