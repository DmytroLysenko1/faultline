package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

type partitionLease interface {
	Valid() bool
}

type lifecycle uint8

const (
	stateOpened lifecycle = iota
	stateStaked
	stateStarted
	stateClosed
)

type outcome uint8

const (
	outcomeRefused outcome = iota
	outcomeEmpty
	outcomeUnusable
	outcomeFound
	outcomeRestorable
)

type address struct {
	read     bool
	verdict  outcome
	revision uint64
	current  *objectRef
	firstSeq uint64
	meta     processing.CheckpointMeta
	created  time.Time
}

type staking struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

type Store struct {
	options    Options
	lease      partitionLease
	buckets    buckets
	now        func() time.Time
	reading    chan struct{}
	mu         sync.Mutex
	state      lifecycle
	latest     address
	generation uint64
	stats      counters
	up         *uploader
	staking    *staking
	done       chan struct{}
	failed     error
	chunkBytes uint32
}

func Open(
	ctx context.Context,
	conn *natsjs.Conn,
	lease partitionLease,
	options *Options,
) (*Store, error) {
	store, err := open(ctx, conn, lease, options, dedupWindow)
	return store, translate(err)
}

func open(
	ctx context.Context,
	conn *natsjs.Conn,
	lease partitionLease,
	options *Options,
	window time.Duration,
) (*Store, error) {
	if err := options.validate(window); err != nil {
		return nil, err
	}
	control, data, err := jetStreams(conn)
	if err != nil {
		return nil, err
	}
	if err := removeSpools(options.SpoolDir); err != nil {
		return nil, err
	}
	bound, err := openBuckets(ctx, control, data, options, window)
	if err != nil {
		return nil, err
	}
	return newStore(options, lease, &bound, time.Now), nil
}

func jetStreams(conn *natsjs.Conn) (jetstream.JetStream, jetstream.JetStream, error) {
	control, err := jetstream.New(conn.Control.NATS)
	if err != nil {
		return nil, nil, fmt.Errorf("checkpoint control jetstream: %w", err)
	}
	data, err := jetstream.New(conn.Publish.NATS)
	if err != nil {
		return nil, nil, fmt.Errorf("checkpoint data jetstream: %w", err)
	}
	return control, data, nil
}

func newStore(options *Options, lease partitionLease, bound *buckets, now func() time.Time) *Store {
	return &Store{
		options:    *options,
		lease:      lease,
		buckets:    *bound,
		now:        now,
		reading:    make(chan struct{}, 1),
		done:       make(chan struct{}),
		chunkBytes: chunkSize,
	}
}

func (s *Store) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.reading <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) release() {
	<-s.reading
}

func (s *Store) Close(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateClosed {
		return
	}
	s.state = stateClosed
	if s.staking != nil {
		s.staking.cancel(errStopStake)
	}
	if s.up != nil {
		s.up.stop(stopDeadline(ctx, time.Now()))
	}
}

func (s *Store) Wait(ctx context.Context) error {
	done := s.running()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return translate(fmt.Errorf("wait for the checkpoint store to stop: %w", ctx.Err()))
	}
}

func (s *Store) running() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.up != nil:
		return s.up.exited
	case s.staking != nil:
		return s.staking.done
	default:
		return nil
	}
}

func (s *Store) Done() <-chan struct{} {
	return s.done
}

func (s *Store) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return translate(s.failed)
}

func (s *Store) Stats() Stats {
	return s.stats.snapshot(s.options.Epoch, s.options.MaxBytes)
}

func stopDeadline(ctx context.Context, now time.Time) time.Time {
	deadline, bounded := ctx.Deadline()
	if !bounded {
		deadline = now.Add(defaultStop)
	}
	return deadline.Add(-stopMargin)
}

func (s *Store) open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateClosed {
		return errStoreClosed
	}
	return nil
}

func (s *Store) remember(found *address) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = *found
	s.latest.read = true
}

func (s *Store) settle(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest.verdict = verdictOf(err)
}

func verdictOf(err error) outcome {
	switch {
	case err == nil:
		return outcomeFound
	case errors.Is(err, processing.ErrNoCheckpoint):
		return outcomeEmpty
	case unusable(err):
		return outcomeUnusable
	}
	return outcomeRefused
}

func unusable(err error) bool {
	causes := []error{errCorrupt, errCheckpointMissing, errStreamRecreated, errReadIncomplete}
	return slices.ContainsFunc(causes, func(cause error) bool {
		return errors.Is(err, cause)
	})
}

func (v outcome) discardable() bool {
	return v == outcomeUnusable || v == outcomeRestorable
}

func (s *Store) nextGeneration() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
}

func (s *Store) currentGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

func (s *Store) settleRead(generation uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != s.generation {
		return
	}
	s.latest.verdict = readVerdict(err)
}

func readVerdict(err error) outcome {
	if errors.Is(err, errReadDone) {
		return outcomeRestorable
	}
	return verdictOf(err)
}

func (s *Store) remembered() address {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

func (s *Store) enter(ctx context.Context) (context.Context, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateClosed {
		return nil, errStoreClosed
	}
	if s.state != stateOpened || s.staking != nil {
		return nil, fmt.Errorf("%w: partition %d is already staked", errStoreState, s.options.Partition)
	}
	s.state = stateStaked
	staked, cancel := context.WithCancelCause(ctx)
	s.staking = &staking{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	return staked, nil
}

func (s *Store) exitStake(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staking.cancel(nil)
	close(s.staking.done)
	s.staking = nil
	if err != nil && s.state == stateStaked {
		s.state = stateOpened
	}
	if err != nil && s.state == stateClosed {
		return fmt.Errorf("%w: %w", errStoreClosed, err)
	}
	return err
}

func (s *Store) notStaking() error {
	if s.staking != nil {
		return fmt.Errorf("%w: start while partition %d is staked", errStoreState, s.options.Partition)
	}
	return nil
}
