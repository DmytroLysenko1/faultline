package tokens

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	massRevokeFloor   = 16
	massRevokeDivisor = 2
	slowRereadDivisor = 2
)

type epoch struct {
	covered   bucketState
	regressed bool
	next      time.Time
}

type failureCounter struct {
	target  error
	counter func(*counters) *atomic.Uint64
}

var failureCounters = []failureCounter{
	{target: ErrNoLeader, counter: func(c *counters) *atomic.Uint64 { return &c.leaderlessChecks }},
	{target: ErrNoQuorum, counter: func(c *counters) *atomic.Uint64 { return &c.quorumlessChecks }},
	{target: ErrBucketDrift, counter: func(c *counters) *atomic.Uint64 { return &c.driftedChecks }},
}

func (d *Directory) Run(ctx context.Context) error {
	if !d.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	for {
		if err := retry.Sleep(ctx, time.Until(d.epoch.next)); err != nil {
			return fmt.Errorf("token directory stopped: %w", err)
		}
		d.epoch.next = d.check(ctx)
	}
}

func (d *Directory) load(ctx context.Context) error {
	sent := time.Now()
	entries, err := d.reread(ctx)
	if err != nil {
		return err
	}
	d.publish(&entries, sent)
	d.epoch.next = sent.Add(d.options.ReloadInterval)
	if entries.Invalid > 0 && entries.Invalid >= uint64(len(entries.Grants)) {
		d.logger.Error("tokens.mostly_invalid_entries", "invalid", entries.Invalid, "grants", len(entries.Grants))
	}
	return nil
}

func (d *Directory) noteDifferences(verdict *configVerdict) {
	if notes := verdict.notes(); len(notes) > 0 {
		d.logger.Warn("tokens.bucket_config_differs", "fields", notes)
	}
}

func (d *Directory) check(ctx context.Context) time.Time {
	sent := time.Now()
	d.stats.checks.Add(1)
	state, err := d.readState(ctx)
	if err != nil {
		d.failed(ctx, err)
		return d.retryAt()
	}
	d.followEpoch(state)
	if d.regressedBy(state) {
		return d.retryAt()
	}
	if d.epoch.covered.equal(state) {
		d.refresh(sent)
		return sent.Add(d.options.ReloadInterval)
	}
	return d.renew(ctx, sent)
}

func (d *Directory) renew(ctx context.Context, sent time.Time) time.Time {
	entries, err := d.reread(ctx)
	if err != nil {
		d.failedReread(ctx, err)
		return d.retryAt()
	}
	d.followEpoch(entries.State)
	if d.regressedBy(entries.State) {
		return d.retryAt()
	}
	d.publish(&entries, sent)
	return sent.Add(d.options.ReloadInterval)
}

func (d *Directory) readState(ctx context.Context) (bucketState, error) {
	checkCtx, cancel := context.WithTimeout(ctx, d.options.CheckTime)
	defer cancel()
	state, verdict, err := d.state(checkCtx)
	if err != nil {
		return bucketState{}, err
	}
	return state, d.accept(&verdict)
}

func (d *Directory) reread(ctx context.Context) (bucketEntries, error) {
	rereadCtx, cancel := context.WithTimeout(ctx, d.options.RereadTime)
	defer cancel()
	started := time.Now()
	entries, verdict, err := d.entries(rereadCtx)
	d.timeReread(time.Since(started))
	if err != nil {
		return bucketEntries{}, err
	}
	if d.epoch.covered.Created.IsZero() {
		d.noteDifferences(&verdict)
	}
	return entries, d.accept(&verdict)
}

func (d *Directory) timeReread(took time.Duration) {
	d.stats.rereadDuration.Store(int64(took))
	if took > d.options.RereadTime/slowRereadDivisor {
		d.logger.Warn("tokens.slow_reread", "took", took, "budget", d.options.RereadTime)
	}
}

func (d *Directory) accept(verdict *configVerdict) error {
	d.stats.revokeBlocked.Store(verdict.irreversible())
	drifted := len(verdict.byClass[classStrict]) > 0
	if d.stats.configDrift.Swap(drifted) != drifted && drifted {
		d.logger.Error("tokens.bucket_drift", "fields", verdict.notes())
	}
	return refuseDrift(verdict)
}

func (d *Directory) failed(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	d.stats.checkFailures.Add(1)
	for _, rule := range failureCounters {
		if errors.Is(err, rule.target) {
			rule.counter(&d.stats).Add(1)
		}
	}
	d.logger.Warn("tokens.check_failed", "error", err)
}

func (d *Directory) failedReread(ctx context.Context, err error) {
	if ctx.Err() == nil {
		d.stats.rereadFailures.Add(1)
	}
	d.failed(ctx, err)
}

func (d *Directory) followEpoch(state bucketState) {
	covered := d.epoch.covered
	if covered.Created.IsZero() || state.Created.Equal(covered.Created) {
		return
	}
	d.stats.epochChanges.Add(1)
	d.logger.Error("tokens.bucket_epoch_changed", "was", covered.Created, "now", state.Created)
	d.epoch = epoch{next: d.epoch.next}
	d.stats.regressed.Store(false)
}

func (d *Directory) regressedBy(state bucketState) bool {
	if !d.epoch.regressed && rolledBack(state, d.epoch.covered) {
		d.epoch.regressed = true
		d.stats.regressed.Store(true)
		d.logger.Error("tokens.sequence_regressed",
			"last_seq", state.LastSeq,
			"covered_last_seq", d.epoch.covered.LastSeq,
		)
	}
	if d.epoch.regressed {
		d.stats.regressedChecks.Add(1)
		d.stats.checkFailures.Add(1)
	}
	return d.epoch.regressed
}

func (d *Directory) refresh(sent time.Time) {
	d.current.Store(&snapshot{
		grants:    d.current.Load().grants,
		checkedAt: sent,
	})
}

func (d *Directory) publish(entries *bucketEntries, sent time.Time) {
	d.reportMassRevoke(entries)
	d.current.Store(&snapshot{
		grants:    entries.Grants,
		checkedAt: sent,
	})
	d.epoch.covered = entries.State
	d.stats.rereads.Add(1)
	d.stats.lastSequence.Store(entries.State.LastSeq)
	d.stats.invalidEntries.Store(entries.Invalid)
}

func (d *Directory) reportMassRevoke(entries *bucketEntries) {
	previous := d.current.Load().grants
	vanished := 0
	for id := range previous {
		if _, kept := entries.Grants[id]; !kept {
			vanished++
		}
	}
	if len(previous) == 0 || !massive(vanished, len(previous), entries.State.Msgs) {
		return
	}
	d.stats.massRevokes.Add(1)
	d.logger.Error("tokens.mass_revoke", "vanished", vanished, "of", len(previous))
}

func (d *Directory) retryAt() time.Time {
	return time.Now().Add(d.options.RetryEvery)
}

func massive(vanished, previous int, messages uint64) bool {
	return (messages == 0 && vanished > 1) || vanished > max(massRevokeFloor, previous/massRevokeDivisor)
}

func rolledBack(state, covered bucketState) bool {
	if covered.Created.IsZero() {
		return false
	}
	if state.LastSeq < covered.LastSeq || state.FirstSeq < covered.FirstSeq {
		return true
	}
	return state.Msgs > covered.Msgs && state.Msgs-covered.Msgs > state.LastSeq-covered.LastSeq
}

func (s bucketState) equal(other bucketState) bool {
	return s.LastSeq == other.LastSeq &&
		s.FirstSeq == other.FirstSeq &&
		s.Msgs == other.Msgs &&
		s.Created.Equal(other.Created)
}
