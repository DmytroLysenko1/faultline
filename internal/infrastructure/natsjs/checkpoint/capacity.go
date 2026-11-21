package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const spaceSweepRead = "the pointer read for the space sweep"

var (
	errBucketFull   = errors.New("checkpoint image does not fit the object bucket")
	errBucketUnread = errors.New("checkpoint object bucket could not be read")
)

type sweepSchedule struct {
	due         bool
	pausedUntil time.Time
}

func (s *sweepSchedule) wanted(now time.Time, overflows bool) bool {
	return s.due || (overflows && now.After(s.pausedUntil))
}

func (s *sweepSchedule) bucketFull() {
	s.due = true
}

func (s *sweepSchedule) swept(outcome *sweepOutcome, now time.Time, orphanAge time.Duration) {
	s.pausedUntil = outcome.pausedUntil(now, orphanAge)
	s.due = s.due && !outcome.completed
}

func (u *uploader) ready(upload, control context.Context, size int64) error {
	if !u.store.lease.Valid() {
		return skipped(skipLeaseSuspended, errLeaseLost)
	}
	if err := u.resolve(control); err != nil {
		return u.classified(err)
	}
	u.countImage(size)
	return u.fits(upload, control, size)
}

func (u *uploader) countImage(size int64) {
	u.store.stats.lastImage.Store(size)
	if size > u.store.options.WorstImage {
		u.store.stats.overWorst.Add(1)
	}
}

func (u *uploader) fits(upload, control context.Context, size int64) error {
	used, err := u.bucketUsed(upload)
	if err != nil {
		return err
	}
	if u.sweepWanted(used, size) {
		used, err = u.usedAfterSweep(upload, control, used)
		if err != nil {
			return err
		}
	}
	return u.roomFor(used, size)
}

func (u *uploader) usedAfterSweep(upload, control context.Context, used uint64) (uint64, error) {
	removed, err := u.spaceSweep(control)
	if err != nil || !removed {
		return used, err
	}
	return u.bucketUsed(upload)
}

func (u *uploader) bucketUsed(ctx context.Context) (uint64, error) {
	used, err := u.bucketBytes(ctx)
	if err != nil {
		return 0, u.classified(failed(failBucketRead, fmt.Errorf("%w: %w", errBucketUnread, err)))
	}
	u.store.stats.bucketBytes.Store(used)
	return used, nil
}

func (u *uploader) overflows(used uint64, size int64) bool {
	return used+unsigned(size) > unsigned(u.store.options.MaxBytes)
}

func (u *uploader) roomFor(used uint64, size int64) error {
	if !u.overflows(used, size) {
		return nil
	}
	return skipped(skipBucketFull, fmt.Errorf("%w: %d bytes, the bucket holds %d of %d", errBucketFull, size, used,
		u.store.options.MaxBytes))
}

func (u *uploader) sweepWanted(used uint64, size int64) bool {
	return !u.stopRequested() && u.schedule.wanted(u.store.now(), u.overflows(used, size))
}

func (u *uploader) spaceSweep(control context.Context) (bool, error) {
	ctx, cancel := u.stoppableSweep(control)
	defer cancel()
	if err := u.sweepGoesOn(); err != nil {
		return false, sweepCut(err)
	}
	u.store.stats.spaceSweeps.Add(1)
	found, err := u.pointerNow(ctx)
	if err != nil {
		return false, u.sweepFinished(ctx, &sweepOutcome{}, fmt.Errorf("%s: %w", spaceSweepRead, err))
	}
	if decided, settled := judgeEpoch(found, u.store.options.Epoch); settled {
		return false, epochEnded(decided, spaceSweepRead)
	}
	outcome, err := u.store.sweepBucket(ctx, spaceRule(found, u.store.options.Epoch, u.sweepGoesOn))
	return outcome.removed > 0, u.sweepFinished(ctx, &outcome, err)
}

func (u *uploader) stoppableSweep(control context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(control)
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	u.cancelSweep = cancel
	return ctx, func() {
		u.forgetSweep()
		cancel(nil)
	}
}

func (u *uploader) forgetSweep() {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	u.cancelSweep = nil
}

func (u *uploader) sweepGoesOn() error {
	if u.stopRequested() {
		return errStopSweep
	}
	return u.store.leaseKept()
}

func (u *uploader) sweepFinished(ctx context.Context, outcome *sweepOutcome, err error) error {
	err = stopMarked(ctx, err)
	u.store.stats.sweepEnded(err)
	u.schedule.swept(outcome, u.store.now(), u.store.options.orphanAge())
	return sweepCut(err)
}

func sweepCut(err error) error {
	switch {
	case stopCaused(err):
		return stopped(err)
	case errors.Is(err, errSweepLeaseLost):
		return skipped(skipLeaseSuspended, err)
	default:
		return nil
	}
}

func (u *uploader) uploadFailed(class uploadClass) {
	u.store.stats.failUpload(class)
	if class == classBucketFull {
		u.schedule.bucketFull()
	}
}
