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

type Store struct {
	options    Options
	lease      partitionLease
	buckets    buckets
	now        func() time.Time
	reading    chan struct{}
	mu         sync.Mutex
	state      lifecycle
	latest     address
	stats      counters
	up         *uploader
	done       chan struct{}
	failed     error
	chunkBytes uint32
}

func Open(ctx context.Context, conn *natsjs.Conn, lease partitionLease, options *Options) (*Store, error) {
	store, err := open(ctx, conn, lease, options)
	return store, translate(err)
}

func open(ctx context.Context, conn *natsjs.Conn, lease partitionLease, options *Options) (*Store, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	control, data, err := jetStreams(conn)
	if err != nil {
		return nil, err
	}
	if err := removeSpools(options.SpoolDir); err != nil {
		return nil, err
	}
	bound, err := openBuckets(ctx, control, data, options)
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
	if s.up != nil {
		s.up.stop(stopDeadline(ctx, time.Now()))
	}
}

func (s *Store) Wait(ctx context.Context) error {
	up := s.uploader()
	if up == nil {
		return nil
	}
	select {
	case <-up.exited:
		return nil
	case <-ctx.Done():
		return translate(fmt.Errorf("wait for the checkpoint upload to stop: %w", ctx.Err()))
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

func (s *Store) uploader() *uploader {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.up
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
		return outcomeRestorable
	case errors.Is(err, processing.ErrNoCheckpoint):
		return outcomeEmpty
	case unusable(err):
		return outcomeUnusable
	}
	return outcomeRefused
}

func unusable(err error) bool {
	return slices.ContainsFunc([]error{errCorrupt, errCheckpointMissing, errStreamRecreated}, func(cause error) bool {
		return errors.Is(err, cause)
	})
}

func (v outcome) discardable() bool {
	return v == outcomeUnusable || v == outcomeRestorable
}

func (s *Store) remembered() address {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

func (s *Store) staked() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case stateClosed:
		return errStoreClosed
	case stateStaked, stateStarted:
		return fmt.Errorf("%w: partition %d is already staked", errStoreState, s.options.Partition)
	case stateOpened:
		s.state = stateStaked
	}
	return nil
}
