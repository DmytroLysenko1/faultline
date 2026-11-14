package retry

import (
	"context"
	"errors"
	"time"
)

const backoff = 2

type Policy struct {
	Attempt   time.Duration
	FirstWait time.Duration
	MaxWait   time.Duration
	Retryable func(err error) bool
}

func Do(ctx context.Context, policy *Policy, call func(attempt context.Context) error) error {
	wait := policy.FirstWait
	err := Attempt(ctx, policy.Attempt, call)
	for policy.retries(err) {
		if waitErr := Sleep(ctx, wait); waitErr != nil {
			return errors.Join(err, waitErr)
		}
		wait = min(backoff*wait, policy.MaxWait)
		err = Attempt(ctx, policy.Attempt, call)
	}
	return err
}

func (p *Policy) retries(err error) bool {
	return err != nil && p.Retryable(err)
}

func Attempt(ctx context.Context, budget time.Duration, call func(attempt context.Context) error) error {
	attempt, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return call(attempt)
}

func Sleep(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
