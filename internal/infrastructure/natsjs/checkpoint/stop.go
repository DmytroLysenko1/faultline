package checkpoint

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"
	"time"
)

const minCallBudget = 100 * time.Millisecond

var (
	errStopUpload  = errors.New("checkpoint work before the pointer write cut by the stop")
	errStopControl = errors.New("checkpoint call cut by the stop")
	errStopPlan    = errors.New("checkpoint upload dropped by the stop plan")
	errStopSweep   = errors.New("checkpoint sweep cut by the stop")
	errStopStake   = errors.New("checkpoint stake cut by the stop")
	errNoTimeLeft  = errors.New("too little time left before the stop deadline for a checkpoint call")
)

type edge uint8

const (
	edgeUpload edge = iota
	edgeControl
	edgeRoot
	edges
)

var edgeLeads = [edges]time.Duration{
	edgeUpload:  stopReserve,
	edgeControl: stopMargin,
	edgeRoot:    0,
}

type progress struct {
	started time.Time
	sent    int64
	acked   int
}

func (u *uploader) stop(deadline time.Time) {
	u.stopAt = deadline
	close(u.stopping)
	if u.cancelSweep != nil {
		u.cancelSweep(errStopSweep)
	}
	if !u.accepting {
		return
	}
	for each := range edges {
		wait := time.Until(deadline.Add(-edgeLeads[each]))
		if wait <= 0 {
			u.cancelThrough(each)
			continue
		}
		u.timers[each] = time.AfterFunc(wait, func() {
			u.cancelThrough(each)
		})
	}
}

func (u *uploader) cancelThrough(last edge) {
	for each := range last + 1 {
		u.cancels[each]()
	}
}

func (u *uploader) stopTimers() {
	for _, timer := range u.timers {
		if timer != nil {
			timer.Stop()
		}
	}
}

func (u *uploader) controlWithin(
	ctx context.Context,
	own time.Duration,
	call func(ctx context.Context) error,
) error {
	return u.within(ctx, own, edgeControl, call)
}

func (u *uploader) uploadWithin(
	ctx context.Context,
	own time.Duration,
	call func(ctx context.Context) error,
) error {
	return u.within(ctx, own, edgeUpload, call)
}

func (u *uploader) within(
	parent context.Context,
	own time.Duration,
	limit edge,
	call func(ctx context.Context) error,
) error {
	ctx, cancel, err := u.bounded(parent, own, limit)
	if err != nil {
		return err
	}
	defer cancel()
	return stopMarked(ctx, call(ctx))
}

func (u *uploader) bounded(
	parent context.Context,
	own time.Duration,
	limit edge,
) (context.Context, context.CancelFunc, error) {
	if err := deadParent(parent); err != nil {
		return nil, nil, err
	}
	stopAt, stopping := u.stopDeadline()
	if !stopping {
		ctx, cancel := context.WithTimeout(parent, own)
		return ctx, cancel, nil
	}
	end := stopAt.Add(-edgeLeads[limit])
	if !time.Now().Add(own).After(end) {
		ctx, cancel := context.WithTimeout(parent, own)
		return ctx, cancel, nil
	}
	if time.Until(end) < minCallBudget {
		return nil, nil, errNoTimeLeft
	}
	ctx, cancel := context.WithDeadlineCause(parent, end, limitCause(limit))
	return ctx, cancel, nil
}

func (u *uploader) boundary(phase uploadPhase, left int64) error {
	u.phase = phase
	u.left = left
	if !u.stopRequested() {
		return nil
	}
	u.takeQueued()
	if planAt(u.stopState()) != dropCurrent {
		return nil
	}
	return errStopPlan
}

func (u *uploader) stopState() stopState {
	stopAt, _ := u.stopDeadline()
	return stopState{
		phase:     u.phase,
		left:      u.left,
		size:      u.size,
		queued:    u.queued != nil,
		pending:   len(u.pending) > 0,
		final:     u.queuedSize(),
		remaining: time.Until(stopAt),
		speed:     u.speed(),
	}
}

func (u *uploader) queuedSize() int64 {
	if u.queued == nil {
		return 0
	}
	return u.queued.size
}

func (u *uploader) speed() rates {
	return rates{
		upload: lowest(u.store.options.UploadRate, u.measured.upload, u.observedUpload()),
		verify: u.verifySpeed(),
	}
}

func (u *uploader) verifySpeed() int64 {
	if u.store.options.VerifyRate > 0 {
		return lowest(u.store.options.VerifyRate, u.measured.verify)
	}
	return cmp.Or(u.measured.verify, verifyFloorRate)
}

func (u *uploader) observedUpload() int64 {
	if u.phase != phaseUpload || u.progress.acked < burstChunks {
		return 0
	}
	return rate(u.progress.sent, time.Since(u.progress.started))
}

func (u *uploader) takeQueued() {
	for {
		select {
		case next := <-u.spools:
			u.queueAtStop(new(next))
		default:
			return
		}
	}
}

func (u *uploader) queueAtStop(next *spool) {
	if next.final {
		u.queued = next
		return
	}
	u.release(next)
	u.store.stats.skip(skipNotStarted)
}

func (u *uploader) leave(upload, control context.Context) {
	if !u.stopRequested() || u.ended() {
		return
	}
	u.takeQueued()
	u.startQueued(upload, control)
	if u.ended() {
		return
	}
	u.settleAtStop(control)
}

func (u *uploader) settleAtStop(ctx context.Context) {
	err := u.resolve(ctx)
	u.publishCommits()
	if outcome, known := errors.AsType[*cycleError](err); known && outcome.kind == kindTerminal {
		outcome.apply(u)
		return
	}
	u.store.stats.leftAtStop.Add(uint64(len(u.pending)))
}

func (u *uploader) startQueued(upload, control context.Context) {
	if u.queued == nil {
		return
	}
	u.phase = phaseIdle
	action := planAt(u.stopState())
	queued := u.queued
	u.queued = nil
	if action != startFinal {
		u.release(queued)
		return
	}
	u.handle(upload, control, queued)
}

func (u *uploader) finalUnsettled() bool {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	return u.finalHandedOff && !u.finalCommitted
}

func lowest(first int64, more ...int64) int64 {
	result := first
	for _, each := range more {
		if each > 0 && each < result {
			result = each
		}
	}
	return result
}

func deadParent(parent context.Context) error {
	if parent.Err() == nil {
		return nil
	}
	if stopCaused(context.Cause(parent)) {
		return errNoTimeLeft
	}
	return parent.Err()
}

func limitCause(limit edge) error {
	if limit == edgeUpload {
		return errStopUpload
	}
	return errStopControl
}

func signed(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

func stopCauses() []error {
	return []error{errStopUpload, errStopControl, errStopSweep, errStopStake, errStopPlan, errNoTimeLeft}
}

func stopCaused(err error) bool {
	return slices.ContainsFunc(stopCauses(), func(cause error) bool {
		return errors.Is(err, cause)
	})
}

func stopMarked(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
		return err
	}
	return errors.Join(err, context.Cause(ctx))
}
