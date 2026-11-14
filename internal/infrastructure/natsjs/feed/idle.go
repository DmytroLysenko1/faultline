package feed

import (
	"context"
	"errors"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/idlerule"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	pulseAttempt     = 2 * time.Second
	starvedConflicts = 3
)

type pulses interface {
	Pulse(ctx context.Context, beat *pulse) error
	PulseUnconditionally(ctx context.Context, beat *pulse) error
}

type idle struct {
	rule      idlerule.Rule
	stamped   time.Time
	since     time.Time
	pulsed    time.Time
	caughtUp  bool
	conflicts int
	reads     uint64
	readSince bool
}

func newIdle(start *Start, after time.Duration) idle {
	rule := idlerule.Rule{
		After: after,
	}
	if start.After.Sequence > 0 {
		rule.Started = true
		rule.Since = start.ReceivedAt
	}
	return idle{
		rule:    rule,
		stamped: start.ReceivedAt,
		since:   time.Now(),
	}
}

func (i *idle) now() time.Time {
	return i.stamped.Add(time.Since(i.since))
}

func (i *idle) delivered(made batch.Batch) {
	i.rule.Record(made)
	i.stamped = made.ReceivedAt
	i.since = time.Now()
}

func (i *idle) observed(found bool, pending uint64) {
	i.caughtUp = !found || pending == 0
}

func (i *idle) due() bool {
	if !i.rule.Due(i.now()) {
		i.conflicts = 0
		return false
	}
	return i.caughtUp && (i.pulsed.IsZero() || time.Since(i.pulsed) >= i.rule.After)
}

func (i *idle) attempted(reads uint64) {
	i.readSince = reads != i.reads
	i.reads = reads
}

func (i *idle) written() {
	i.pulsed = time.Now()
	i.conflicts = 0
}

func (p *pump) beat(ctx context.Context) error {
	if !p.idle.due() || !p.lease.Valid() {
		return nil
	}
	beat := &pulse{
		after:     p.last,
		writtenAt: p.idle.now(),
	}
	p.idle.attempted(p.counters.reads.Load())
	err := retry.Attempt(ctx, pulseAttempt, func(attempt context.Context) error {
		return p.pulses.Pulse(attempt, beat)
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return p.settle(ctx, beat, err)
}

func (p *pump) settle(ctx context.Context, beat *pulse, err error) error {
	switch {
	case err == nil:
		p.written()
	case errors.Is(err, errPulseConflict):
		return p.conflicted(ctx, beat)
	default:
		p.counters.heartbeatFailures.Add(1)
	}
	return nil
}

func (p *pump) conflicted(ctx context.Context, beat *pulse) error {
	if !p.idle.readSince {
		p.counters.heartbeatConflictsUnread.Add(1)
		return nil
	}
	p.counters.heartbeatConflicts.Add(1)
	p.idle.conflicts++
	if p.idle.conflicts < starvedConflicts {
		return nil
	}
	return p.pulseUnconditionally(ctx, beat)
}

func (p *pump) pulseUnconditionally(ctx context.Context, beat *pulse) error {
	err := retry.Attempt(ctx, pulseAttempt, func(attempt context.Context) error {
		return p.pulses.PulseUnconditionally(attempt, beat)
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		p.written()
		p.counters.heartbeatsUnconditional.Add(1)
		return nil
	}
	p.counters.heartbeatFailures.Add(1)
	return nil
}

func (p *pump) written() {
	p.idle.written()
	p.counters.heartbeatsWritten.Add(1)
}
