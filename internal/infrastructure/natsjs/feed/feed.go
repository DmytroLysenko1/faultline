package feed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

var (
	ErrStreamIdentity       = errors.New("journal stream identity changed")
	ErrPositionRange        = errors.New("checkpoint position is outside the stream")
	ErrUnknownRecordVersion = errors.New("journal record of an unknown version")
	ErrSystemicCorruption   = errors.New("journal records corrupt one after another")
	ErrDeadLetterRejected   = errors.New("dead letter queue refused a corrupt record")
	ErrConsumerLost         = errors.New("journal consumer could not be recreated")
	ErrLeaseLost            = errors.New("partition lease lost")
	ErrLeaseSuspended       = errors.New("partition lease not valid for retention")
	ErrForeignPosition      = errors.New("position of another partition")
	ErrInvalidOptions       = errors.New("invalid feed options")
	ErrInvalidLimit         = errors.New("receive limit must be positive")
)

const (
	maxResetAttempts = 7
	pullWindows      = 2
	discardTimeout   = 500 * time.Millisecond
	consumerTimeout  = 10 * time.Second
	infoTimeout      = 2 * time.Second
	queuedDeliveries = 64
	namePrefixFormat = "fl-p%d-e%d-%s"
)

type partitionLease interface {
	Valid() bool
	Lost() <-chan struct{}
}

type Options struct {
	Topology     natsjs.Topology
	Partition    uint16
	Epoch        uint64
	MaxBytes     int64
	MaxFootprint int64
	PullMaxBytes int
	IdleAfter    time.Duration
}

func (o *Options) validate(start *Start) error {
	if o == nil {
		return fmt.Errorf("%w: no options", ErrInvalidOptions)
	}
	for _, check := range []func() error{o.validatePartition, o.validateCredit, o.validateIdle} {
		if err := check(); err != nil {
			return err
		}
	}
	return o.validateStart(start)
}

func (o *Options) validateStart(start *Start) error {
	if start.After.Partition != o.Partition {
		return fmt.Errorf("%w: start of partition %d for partition %d", ErrInvalidOptions, start.After.Partition,
			o.Partition)
	}
	if start.After.Sequence > 0 && start.ReceivedAt.IsZero() {
		return fmt.Errorf("%w: a start after position %d without its ReceivedAt", ErrInvalidOptions,
			start.After.Sequence)
	}
	return nil
}

func (o *Options) validatePartition() error {
	if err := o.Topology.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	if int(o.Partition) >= o.Topology.Partitions || o.Epoch == 0 {
		return fmt.Errorf("%w: partition %d of %d at epoch %d", ErrInvalidOptions, o.Partition,
			o.Topology.Partitions, o.Epoch)
	}
	return nil
}

func (o *Options) validateCredit() error {
	if o.MaxFootprint < 1 || o.MaxBytes < o.MaxFootprint {
		return fmt.Errorf("%w: max bytes %d must hold the largest batch footprint %d", ErrInvalidOptions, o.MaxBytes,
			o.MaxFootprint)
	}
	if o.PullMaxBytes < pullWindows*natsjs.MaxMessageBytes {
		return fmt.Errorf("%w: pull max bytes %d, at least %d", ErrInvalidOptions, o.PullMaxBytes,
			pullWindows*natsjs.MaxMessageBytes)
	}
	return nil
}

func (o *Options) validateIdle() error {
	if o.IdleAfter <= 0 {
		return fmt.Errorf("%w: idle after %s must be positive", ErrInvalidOptions, o.IdleAfter)
	}
	return nil
}

type Start struct {
	After      processing.Position
	ReceivedAt time.Time
	Created    time.Time
}

type Feed struct {
	created    time.Time
	partition  uint16
	stream     partitionStream
	lease      partitionLease
	deliveries chan *delivery
	halted     chan struct{}
	cause      error
	haltOnce   sync.Once
	drained    atomic.Bool
	timer      *time.Timer
	cancel     context.CancelFunc
	reader     reader
	pumped     chan struct{}
	watched    chan struct{}
	done       chan struct{}
	counters   *counters
}

func Open(
	ctx context.Context,
	conn *natsjs.Conn,
	lease partitionLease,
	start Start,
	options *Options,
) (*Feed, error) {
	if err := options.validate(&start); err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("%w: no lease", ErrInvalidOptions)
	}
	js, err := jetstream.New(conn.Feed.NATS)
	if err != nil {
		return nil, fmt.Errorf("jetstream on the feed connection: %w", err)
	}
	source, err := openSource(ctx, js, &start, options)
	if err != nil {
		return nil, fmt.Errorf("open feed of %s: %w", options.Topology.Stream(options.Partition), err)
	}
	return newFeed(ctx, lease, source, &start, options), nil
}

func newFeed(
	ctx context.Context,
	lease partitionLease,
	source *source,
	start *Start,
	options *Options,
) *Feed {
	pumping, cancel := context.WithCancel(context.WithoutCancel(ctx))
	feed := &Feed{
		created:    source.identity.Created,
		partition:  options.Partition,
		stream:     source.stream,
		lease:      lease,
		deliveries: make(chan *delivery, queuedDeliveries),
		halted:     make(chan struct{}),
		cancel:     cancel,
		reader:     source.reader,
		pumped:     make(chan struct{}),
		watched:    make(chan struct{}),
		done:       make(chan struct{}),
		counters:   &counters{},
		timer:      stoppedTimer(),
	}
	feed.counters.purged.Store(purgedBefore(source.identity.First))
	run := newPump(source, start, options, feed)
	go feed.pump(pumping, run)
	go feed.watch()
	return feed
}

func (f *Feed) Created() time.Time {
	return f.created
}

func (f *Feed) Stats() Stats {
	return f.counters.snapshot()
}

func stoppedTimer() *time.Timer {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return timer
}

func (f *Feed) pump(ctx context.Context, run *pump) {
	defer close(f.pumped)
	if err := run.run(ctx); ctx.Err() == nil {
		f.halt(err)
	}
}

func (f *Feed) watch() {
	defer close(f.watched)
	select {
	case <-f.lease.Lost():
		f.halt(ErrLeaseLost)
	case <-f.halted:
	}
}

func (f *Feed) halt(cause error) {
	f.haltOnce.Do(func() {
		f.cause = cause
		f.counters.stop.CompareAndSwap(uint32(StopNone), uint32(stopReasonOf(cause)))
		close(f.halted)
		f.cancel()
		go f.stop()
	})
}

func (f *Feed) stop() {
	f.reader.Stop()
	<-f.pumped
	<-f.watched
	close(f.done)
}

func (f *Feed) Close() {
	f.halt(processing.ErrFeedClosed)
}

func (f *Feed) Wait(ctx context.Context) error {
	select {
	case <-f.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for the feed to stop: %w", ctx.Err())
	}
}
