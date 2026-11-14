package lease

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

func Acquire(ctx context.Context, conn *natsjs.Conn, options *Options) (*Lease, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(conn.Control.NATS)
	if err != nil {
		return nil, fmt.Errorf("jetstream on the control connection: %w", err)
	}
	kv, maxAge, err := openBucket(ctx, js, options)
	if err != nil {
		return nil, err
	}
	return claimConfirmed(ctx, kv, maxAge, options)
}

func claimConfirmed(
	ctx context.Context,
	kv jetstream.KeyValue,
	maxAge time.Duration,
	options *Options,
) (*Lease, error) {
	held, err := claim(ctx, bucket{
		kv: kv,
	}, maxAge, options)
	if err != nil {
		return nil, err
	}
	status, err := checkedStatus(ctx, kv, options)
	if err == nil && status.TTL() < maxAge {
		err = fmt.Errorf("%w: max age fell to %s below %s while acquiring", ErrBucketDrift, status.TTL(), maxAge)
	}
	if err != nil {
		held.abandon(ctx)
		return nil, err
	}
	return held, nil
}

func claim(ctx context.Context, store keyStore, maxAge time.Duration, options *Options) (*Lease, error) {
	lease := newLease(store, maxAge, options, rand.Text())
	for {
		started := lease.now()
		if lease.claimOnce(ctx, started) {
			return lease, nil
		}
		if err := waitUntil(ctx, started+options.RetryEvery-lease.now()); err != nil {
			lease.abandon(ctx)
			return nil, fmt.Errorf("acquire lease %s: %w", lease.key, err)
		}
	}
}

func (l *Lease) abandon(ctx context.Context) {
	l.released.Store(true)
	releasing, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if !l.reclaimOwnWrite(releasing) {
		return
	}
	if err := l.store.Delete(releasing, l.key, l.revision); err != nil {
		l.counters.releaseFailures.Add(1)
	}
}

func (l *Lease) claimOnce(ctx context.Context, sent time.Duration) bool {
	attempt, cancel := context.WithTimeout(ctx, l.options.AttemptTime)
	defer cancel()
	revision, err := l.store.Create(attempt, l.key, l.token)
	if err == nil {
		l.claimed(sent, revision)
		return true
	}
	if !errors.Is(err, jetstream.ErrKeyExists) || !l.reclaimOwnWrite(attempt) {
		return false
	}
	sent = l.now()
	revision, err = l.store.Update(attempt, l.key, l.token, l.revision)
	if err != nil {
		return false
	}
	l.claimed(sent, revision)
	return true
}

func (l *Lease) claimed(sent time.Duration, revision uint64) {
	l.wrote(sent, revision)
	l.Epoch = revision
}
