package lease

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const releaseTimeout = 500 * time.Millisecond

func (l *Lease) Run(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return fmt.Errorf("lease %s: %w", l.key, ErrAlreadyRunning)
	}
	defer l.released.Store(true)
	return l.renew(ctx)
}

func (l *Lease) renew(ctx context.Context) error {
	due := time.Duration(l.renewed.Load()) + l.options.RenewEvery
	for l.reached(ctx, min(due, l.expiry())) {
		if l.now() >= l.expiry() {
			l.markLost()
			return fmt.Errorf("lease %s: %w", l.key, ErrLeaseLost)
		}
		due = l.renewOnce(ctx)
		l.noteSuspension(ctx)
	}
	l.release(ctx)
	return nil
}

func (l *Lease) reached(ctx context.Context, at time.Duration) bool {
	return waitUntil(ctx, at-l.now()) == nil
}

func (l *Lease) noteSuspension(ctx context.Context) {
	suspended := l.sinceRenewal() >= l.options.LostAfter
	if suspended == l.suspended {
		return
	}
	l.suspended = suspended
	level, message := slog.LevelInfo, "lease.resumed"
	if suspended {
		level, message = slog.LevelWarn, "lease.suspended"
	}
	l.options.Logger.LogAttrs(ctx, level, message,
		slog.String("key", l.key),
		slog.Uint64("epoch", l.Epoch),
		slog.Duration("renew_age", l.sinceRenewal()),
	)
}

func (l *Lease) renewOnce(ctx context.Context) time.Duration {
	sent := l.now()
	attempt, cancel := context.WithTimeout(ctx, min(l.options.AttemptTime, l.expiry()-sent))
	defer cancel()
	revision, err := l.store.Update(attempt, l.key, l.token, l.revision)
	if err == nil {
		l.wrote(sent, revision)
		return sent + l.options.RenewEvery
	}
	l.counters.renewFailures.Add(1)
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		l.adoptOwnWrite(attempt)
	}
	return sent + l.options.RetryEvery
}

func (l *Lease) release(ctx context.Context) {
	l.released.Store(true)
	releasing, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	err := l.store.Delete(releasing, l.key, l.revision)
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) && l.adoptOwnWrite(releasing) {
		err = l.store.Delete(releasing, l.key, l.revision)
	}
	if err != nil {
		l.counters.releaseFailures.Add(1)
	}
}
