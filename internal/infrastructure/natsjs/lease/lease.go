package lease

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

var (
	ErrLeaseLost      = errors.New("partition lease lost")
	ErrInvalidOptions = errors.New("invalid lease options")
	ErrBucketDrift    = errors.New("lease bucket configuration drifted")
	ErrAlreadyRunning = errors.New("lease already renewed by Run")
)

const (
	Bucket           = "faultline_leases"
	MeasuredElection = 15 * time.Second
	keyPrefix        = "processor.p"
	clockMargin      = 5 * time.Second
	tokenSeparator   = "/"
)

type keyStore interface {
	Create(ctx context.Context, key string, value []byte) (uint64, error)
	Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)
	Get(ctx context.Context, key string) (entry, error)
	Delete(ctx context.Context, key string, revision uint64) error
}

type entry struct {
	value    []byte
	revision uint64
}

type Options struct {
	Partition      uint16
	Topology       natsjs.Topology
	Instance       string
	TTL            time.Duration
	RenewEvery     time.Duration
	RetryEvery     time.Duration
	AttemptTime    time.Duration
	LostAfter      time.Duration
	Logger         *slog.Logger
	electionBudget time.Duration
}

func (o *Options) validate() error {
	if o == nil {
		return fmt.Errorf("%w: no options", ErrInvalidOptions)
	}
	for _, check := range []func() error{o.validateOwner, o.validatePositive, o.validateMargins} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (o *Options) validateOwner() error {
	if o.Instance == "" || o.Logger == nil {
		return fmt.Errorf("%w: an instance and a logger are required", ErrInvalidOptions)
	}
	if err := o.Topology.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	if int(o.Partition) >= o.Topology.Partitions {
		return fmt.Errorf("%w: partition %d of %d", ErrInvalidOptions, o.Partition, o.Topology.Partitions)
	}
	return nil
}

func (o *Options) validatePositive() error {
	timing := []time.Duration{o.TTL, o.RenewEvery, o.RetryEvery, o.AttemptTime, o.LostAfter}
	if lo.SomeBy(timing, func(timing time.Duration) bool {
		return timing <= 0
	}) {
		return fmt.Errorf("%w: every timing must be positive", ErrInvalidOptions)
	}
	return nil
}

func (o *Options) validateMargins() error {
	if o.AttemptTime > o.RetryEvery {
		return fmt.Errorf("%w: attempt %s longer than retry %s", ErrInvalidOptions, o.AttemptTime, o.RetryEvery)
	}
	if o.TTL-o.LostAfter < clockMargin {
		return fmt.Errorf("%w: ttl %s must exceed lost after %s by %s", ErrInvalidOptions, o.TTL, o.LostAfter,
			clockMargin)
	}
	if o.LostAfter-o.RenewEvery-o.RetryEvery-o.AttemptTime < o.election() {
		return fmt.Errorf("%w: lost after %s rides out less than %s of election", ErrInvalidOptions, o.LostAfter,
			o.election())
	}
	return nil
}

func (o *Options) election() time.Duration {
	return cmp.Or(o.electionBudget, MeasuredElection)
}

type Lease struct {
	Partition uint16
	Epoch     uint64
	store     keyStore
	key       string
	token     []byte
	options   Options
	maxAge    time.Duration
	origin    time.Time
	revision  uint64
	suspended bool
	running   atomic.Bool
	renewed   atomic.Int64
	released  atomic.Bool
	lost      chan struct{}
	markLost  func()
	counters  counters
}

func newLease(store keyStore, maxAge time.Duration, options *Options, nonce string) *Lease {
	lost := make(chan struct{})
	return &Lease{
		Partition: options.Partition,
		store:     store,
		key:       keyPrefix + strconv.Itoa(int(options.Partition)),
		token:     []byte(options.Instance + tokenSeparator + nonce),
		options:   *options,
		maxAge:    maxAge,
		origin:    time.Now(),
		lost:      lost,
		markLost: sync.OnceFunc(func() {
			close(lost)
		}),
	}
}

func (l *Lease) Valid() bool {
	return l != nil && !l.released.Load() && l.sinceRenewal() < l.options.LostAfter
}

func (l *Lease) Lost() <-chan struct{} {
	return l.lost
}

func (l *Lease) Stats() Stats {
	return l.counters.snapshot(l.Epoch, l.Valid(), l.sinceRenewal())
}

func (l *Lease) now() time.Duration {
	return time.Since(l.origin)
}

func (l *Lease) sinceRenewal() time.Duration {
	return l.now() - time.Duration(l.renewed.Load())
}

func (l *Lease) expiry() time.Duration {
	return time.Duration(l.renewed.Load()) + l.maxAge
}

func (l *Lease) wrote(sent time.Duration, revision uint64) {
	l.revision = revision
	l.renewed.Store(int64(sent))
	l.counters.renewals.Add(1)
}

func (l *Lease) adoptOwnWrite(ctx context.Context) bool {
	revision, own := l.ownRevision(ctx)
	if !own || revision <= l.revision {
		return false
	}
	l.revision = revision
	l.counters.lostWrites.Add(1)
	return true
}

func (l *Lease) reclaimOwnWrite(ctx context.Context) bool {
	revision, own := l.ownRevision(ctx)
	if !own {
		return false
	}
	if revision > l.revision {
		l.counters.lostWrites.Add(1)
	}
	l.revision = revision
	return true
}

func (l *Lease) ownRevision(ctx context.Context) (uint64, bool) {
	found, err := l.store.Get(ctx, l.key)
	if err != nil || !bytes.Equal(found.value, l.token) {
		return 0, false
	}
	return found.revision, true
}

func waitUntil(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
