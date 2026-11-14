package feed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

type verdict func(err error) error
type DeadLetterClass string

type letterReason string

const (
	systemicSpan                       = 100
	deadLetterAttempt                  = 2 * time.Second
	deadLetterRetryGap                 = time.Second
	batchReason        letterReason    = "batch"
	stallLeaseInvalid  DeadLetterClass = "lease_suspended"
	seedMalformed      DeadLetterClass = "dlq_seed_malformed"
)

var permanentRefusals = []natsjs.Reason{
	natsjs.ReasonSealed,
	natsjs.ReasonStreamMismatch,
	natsjs.ReasonTooLarge,
}

type deadLetters interface {
	Publish(ctx context.Context, letter *deadLetter) error
	Last(ctx context.Context, partition uint16) (lastLetter, error)
}

type window struct {
	low  uint64
	high uint64
}

func (w window) systemic(sequence uint64) bool {
	return recent(w.low, sequence) && recent(w.high, sequence)
}

func recent(position, sequence uint64) bool {
	return position != 0 && position < sequence && position+systemicSpan > sequence
}

func (w window) after(sequence uint64) window {
	if sequence <= w.high {
		return w
	}
	return window{
		low:  w.high,
		high: sequence,
	}
}

func (w window) valid() bool {
	return w.low == 0 || w.low < w.high
}

func (w window) String() string {
	return strconv.FormatUint(w.low, decimal) + windowSeparator + strconv.FormatUint(w.high, decimal)
}

func (p *pump) reject(ctx context.Context, next *message, cause error) error {
	if errors.Is(cause, record.ErrUnknownVersion) {
		return fmt.Errorf("%w: position %d: %w", ErrUnknownRecordVersion, next.sequence, cause)
	}
	if err := p.seed(ctx); err != nil {
		return err
	}
	if p.window.systemic(next.sequence) {
		return fmt.Errorf("%w: position %d after %s: %w", ErrSystemicCorruption, next.sequence, p.window, cause)
	}
	return p.deadLetter(ctx, next, cause)
}

func (p *pump) deadLetter(ctx context.Context, next *message, cause error) error {
	letter := &deadLetter{
		partition: p.partition,
		sequence:  next.sequence,
		created:   p.created,
		reason:    reasonOf(cause),
		data:      next.data,
		window:    p.window.after(next.sequence),
	}
	publish := func(attempt context.Context) error {
		return p.letters.Publish(attempt, letter)
	}
	if err := p.hold(ctx, publish, publishVerdict); err != nil {
		return err
	}
	p.window = letter.window
	p.counters.deadLettered()
	return nil
}

func reasonOf(cause error) letterReason {
	if corrupt, found := errors.AsType[*record.CorruptError](cause); found {
		return letterReason(corrupt.Reason.String())
	}
	return batchReason
}

func (p *pump) seed(ctx context.Context) error {
	if p.seeded {
		return nil
	}
	if err := p.hold(ctx, p.readWindow, readVerdict); err != nil {
		return err
	}
	p.seeded = true
	return nil
}

func (p *pump) readWindow(ctx context.Context) error {
	last, err := p.letters.Last(ctx, p.partition)
	switch {
	case errors.Is(err, errNoLetter):
		return nil
	case errors.Is(err, errMalformedLetter):
		p.counters.noteDeadLetterError(seedMalformed)
		return nil
	case err != nil:
		return err
	}
	p.adopt(last)
	return nil
}

func (p *pump) adopt(last lastLetter) {
	if !last.created.Equal(p.created) {
		return
	}
	if !last.window.valid() || last.window.high > p.lastAtOpen {
		p.counters.noteDeadLetterError(seedMalformed)
		return
	}
	p.window = last.window
}

func (p *pump) hold(ctx context.Context, attempt func(context.Context) error, final verdict) error {
	for {
		done, err := p.holdOnce(ctx, attempt, final)
		if done {
			return err
		}
		if err := retry.Sleep(ctx, deadLetterRetryGap); err != nil {
			return err
		}
	}
}

func (p *pump) holdOnce(
	ctx context.Context,
	attempt func(context.Context) error,
	final verdict,
) (bool, error) {
	if !p.lease.Valid() {
		p.counters.stalled(stallLeaseInvalid)
		return false, nil
	}
	err := retry.Attempt(ctx, deadLetterAttempt, attempt)
	if err == nil {
		p.counters.unstalled()
		return true, nil
	}
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if stop := final(err); stop != nil {
		return true, stop
	}
	p.counters.stalled(DeadLetterClass(natsjs.Classify(err).Reason.String()))
	return false, nil
}

func publishVerdict(err error) error {
	if slices.Contains(permanentRefusals, natsjs.Classify(err).Reason) {
		return fmt.Errorf("%w: %w", ErrDeadLetterRejected, err)
	}
	return readVerdict(err)
}

func readVerdict(err error) error {
	if natsjs.Classify(err).Reason == natsjs.ReasonClosed {
		return fmt.Errorf("%w: the feed connection is closed: %w", ErrConsumerLost, err)
	}
	return nil
}
