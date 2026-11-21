package checkpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const feedCommitBudget = 5 * time.Second

var (
	errPositionRegressed = errors.New("checkpoint pointer is past this checkpoint on the same stream")
	errFenced            = errors.New("checkpoint pointer write fenced off after no known outcome")
)

type casRun struct {
	written  *object
	sent     bool
	revision uint64
	left     []objectRef
}

func (r *casRun) basedOn(found *pointer) {
	r.sent = true
	r.revision = found.revision
	left := found.value.Previous
	if left != nil && !slices.ContainsFunc(r.left, func(known objectRef) bool {
		return sameObject(&known, left)
	}) {
		r.left = append(r.left, *left)
	}
}

func (r *casRun) waiting() pendingObject {
	return pendingObject{
		ref:      *r.written.ref(),
		updated:  true,
		revision: r.revision,
		left:     r.left,
		metaSent: true,
		final:    r.written.final,
	}
}

func (u *uploader) commit(ctx context.Context, written *object) error {
	u.phase = phaseCAS
	if !u.store.lease.Valid() {
		return u.dropped(ctx, written, skipped(skipLeaseSuspended, errLeaseLost))
	}
	found, err := u.pointerNow(ctx)
	if err != nil {
		return u.dropped(ctx, written, failed(failCAS, fmt.Errorf("read the pointer before %s: %w", written.name,
			err)))
	}
	return u.judged(ctx, written, found)
}

func (u *uploader) judged(ctx context.Context, written *object, found *pointer) error {
	switch judgeBefore(found, u.store.options.Epoch, written.ref(), u.feed.Created()) {
	case verdictForeign:
		return terminal(fmt.Errorf("%w: before %s", errPointerForeign, written.name))
	case verdictLeaseLost:
		return terminal(fmt.Errorf("%w: a later epoch holds the pointer before %s", errLeaseLost, written.name))
	case verdictRegressed:
		return u.dropped(ctx, written, failed(failRegressed, fmt.Errorf("%w: %s", errPositionRegressed, written.name)))
	default:
		return u.update(ctx, &casRun{
			written: written,
		}, found, stepUpdate)
	}
}

func (u *uploader) update(ctx context.Context, run *casRun, found *pointer, step casStep) error {
	data, err := encodePointer(u.nextPointer(found, run.written))
	if err != nil && !run.sent {
		return u.dropped(ctx, run.written, failed(failCAS, err))
	}
	if err != nil {
		return u.settle(ctx, run, step, err)
	}
	err = u.controlWithin(ctx, pointerWriteBudget, func(ctx context.Context) error {
		run.basedOn(found)
		return u.putPointer(ctx, data, found.revision)
	})
	return u.updated(ctx, run, found, step, err)
}

func (u *uploader) updated(
	ctx context.Context,
	run *casRun,
	found *pointer,
	step casStep, err error,
) error {
	switch {
	case err == nil:
		return u.committed(ctx, run, run.written.ref(), found.value.Current)
	case errors.Is(err, errNoTimeLeft) && !run.sent:
		return u.dropped(ctx, run.written, err)
	default:
		return u.settle(ctx, run, step, err)
	}
}

func (u *uploader) putPointer(ctx context.Context, data []byte, revision uint64) error {
	_, err := u.store.buckets.pointers.Update(ctx, u.store.options.pointerKey(), data, revision)
	return err
}

func (u *uploader) settle(ctx context.Context, run *casRun, step casStep, cause error) error {
	reread, err := u.pointerNow(ctx)
	if err != nil {
		return u.unknown(run, errors.Join(cause, err))
	}
	decided := judgeAfter(reread, u.store.options.Epoch, run.written.ref(), step)
	if ended := epochEnded(decided, run.written.name); ended != nil {
		return ended
	}
	return u.proceed(ctx, run, reread, decided, cause)
}

func (u *uploader) proceed(ctx context.Context, run *casRun, reread *pointer, decided verdict, cause error) error {
	switch decided {
	case verdictAccepted:
		return u.committed(ctx, run, reread.value.Current, reread.value.Previous)
	case verdictRetry:
		return u.retry(ctx, run, reread)
	case verdictFence:
		return u.fenceOwn(ctx, run, reread)
	default:
		return u.unknown(run, cause)
	}
}

func epochEnded(decided verdict, name string) error {
	switch decided {
	case verdictLeaseLost:
		return terminal(fmt.Errorf("%w: a later epoch holds the pointer after %s", errLeaseLost, name))
	case verdictForeign:
		return terminal(fmt.Errorf("%w: after %s", errPointerForeign, name))
	default:
		return nil
	}
}

func (u *uploader) retry(ctx context.Context, run *casRun, reread *pointer) error {
	if !u.store.lease.Valid() {
		return u.settle(ctx, run, stepExhausted, errLeaseLost)
	}
	return u.update(ctx, run, reread, stepRetry)
}

func (u *uploader) fenceOwn(ctx context.Context, run *casRun, reread *pointer) error {
	if err := u.fence(ctx, reread); err != nil {
		return u.settle(ctx, run, stepFence, err)
	}
	u.takeBackOrWait(ctx, *run.written.ref(), true)
	return failed(failCAS, fmt.Errorf("%w: %s", errFenced, run.written.name))
}

func (u *uploader) fence(ctx context.Context, found *pointer) error {
	if !u.store.lease.Valid() {
		return errLeaseLost
	}
	u.store.stats.fences.Add(1)
	data, err := encodePointer(&found.value)
	if err != nil {
		return err
	}
	return u.controlWithin(ctx, pointerWriteBudget, func(ctx context.Context) error {
		return u.putPointer(ctx, data, found.revision)
	})
}

func (u *uploader) unknown(run *casRun, cause error) error {
	u.store.stats.casUnknown.Add(1)
	u.wait(new(run.waiting()))
	return failed(failCAS, fmt.Errorf("%w: %s waits: %w", errCASUnknown, run.written.name, cause))
}

func (u *uploader) nextPointer(found *pointer, written *object) *pointerValue {
	return &pointerValue{
		Version:       pointerVersion,
		Epoch:         u.store.options.Epoch,
		StreamCreated: formatCreated(u.feed.Created()),
		Current:       written.ref(),
		Previous:      found.value.Current,
	}
}

func formatCreated(created time.Time) string {
	if created.IsZero() {
		return noStreamCreation
	}
	return created.UTC().Format(createdLayout)
}

func (u *uploader) committed(ctx context.Context, run *casRun, kept ...*objectRef) error {
	u.land(run.written)
	if u.retains(run.written.final) {
		u.retain(ctx, run.written.meta.Position.Sequence)
	}
	u.purgeLeft(ctx, run.left, kept...)
	return nil
}

func (u *uploader) land(written *object) {
	measured := written.measurement()
	u.landed = append(u.landed, &commitRecord{
		through:  written.meta.Position.Sequence,
		at:       u.store.now(),
		measured: measured,
		final:    written.final,
	})
	u.measured = remeasured(u.measured, measured)
}

func (o *object) measurement() *measurement {
	return &measurement{
		bytes:    o.size,
		upload:   o.took,
		verify:   o.verified,
		uploadBs: rate(o.size, o.took),
		verifyBs: rate(o.size, o.verified),
	}
}

func remeasured(known rates, measured *measurement) rates {
	return rates{
		upload: cmp.Or(measured.uploadBs, known.upload),
		verify: cmp.Or(measured.verifyBs, known.verify),
	}
}

func (u *uploader) retains(final bool) bool {
	return !final && !u.stopRequested()
}

func (u *uploader) retain(ctx context.Context, sequence uint64) {
	if u.feedClosed {
		return
	}
	through := processing.Position{
		Partition: u.store.options.Partition,
		Sequence:  sequence,
	}
	err := u.controlWithin(ctx, feedCommitBudget, func(ctx context.Context) error {
		return u.feed.Commit(ctx, through)
	})
	if err != nil {
		u.retainFailed(fmt.Errorf("commit the feed through %d: %w", through.Sequence, err))
	}
}

func (u *uploader) retainFailed(err error) {
	if stopCaused(err) {
		return
	}
	if errors.Is(err, processing.ErrFeedClosed) {
		u.feedClosed = true
	}
	if !u.feedClosed && !u.store.lease.Valid() {
		u.store.stats.skip(skipRetentionSuspended)
		return
	}
	u.store.stats.fail(failCommit, err)
}

func (u *uploader) pointerNow(ctx context.Context) (*pointer, error) {
	var found pointer
	err := u.controlWithin(ctx, pointerWriteBudget, func(ctx context.Context) error {
		var err error
		found, err = u.store.readPointer(ctx)
		return err
	})
	switch {
	case errors.Is(err, errNoPointer):
		return nil, nil
	case errors.Is(err, errCorrupt):
		found.corrupt = true
		return &found, nil
	case err != nil:
		return nil, err
	default:
		return &found, nil
	}
}
