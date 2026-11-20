package checkpoint

import (
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
	}
}

func (u *uploader) commit(root context.Context, written *object) error {
	if !u.store.lease.Valid() {
		return u.dropped(root, written, skipped(skipLeaseSuspended, errLeaseLost))
	}
	found, err := u.pointerNow(root)
	if err != nil {
		return u.dropped(root, written, failed(failCAS, fmt.Errorf("read the pointer before %s: %w", written.name, err)))
	}
	return u.judged(root, written, found)
}

func (u *uploader) judged(root context.Context, written *object, found *pointer) error {
	switch judgeBefore(found, u.store.options.Epoch, written.ref(), u.feed.Created()) {
	case verdictForeign:
		return terminal(fmt.Errorf("%w: before %s", errPointerForeign, written.name))
	case verdictLeaseLost:
		return terminal(fmt.Errorf("%w: a later epoch holds the pointer before %s", errLeaseLost, written.name))
	case verdictRegressed:
		return u.dropped(root, written, failed(failRegressed, fmt.Errorf("%w: %s", errPositionRegressed, written.name)))
	default:
		return u.update(root, &casRun{
			written: written,
		}, found, stepUpdate)
	}
}

func (u *uploader) update(root context.Context, run *casRun, found *pointer, step casStep) error {
	data, err := encodePointer(u.nextPointer(found, run.written))
	if err != nil && !run.sent {
		return u.dropped(root, run.written, failed(failCAS, err))
	}
	if err != nil {
		return u.settle(root, run, step, err)
	}
	run.basedOn(found)
	if err := u.writePointer(root, data, found.revision); err != nil {
		return u.settle(root, run, step, err)
	}
	return u.committed(root, run, run.written.ref(), found.value.Current)
}

func (u *uploader) writePointer(root context.Context, data []byte, revision uint64) error {
	return u.callWithin(root, pointerWriteBudget, func(ctx context.Context) error {
		_, err := u.store.buckets.pointers.Update(ctx, u.store.options.pointerKey(), data, revision)
		return err
	})
}

func (u *uploader) settle(root context.Context, run *casRun, step casStep, cause error) error {
	reread, err := u.pointerNow(root)
	if err != nil {
		return u.unknown(run, errors.Join(cause, err))
	}
	decided := judgeAfter(reread, u.store.options.Epoch, run.written.ref(), step)
	if ended := epochEnded(decided, run.written.name); ended != nil {
		return ended
	}
	return u.proceed(root, run, reread, decided, cause)
}

func (u *uploader) proceed(root context.Context, run *casRun, reread *pointer, decided verdict, cause error) error {
	switch decided {
	case verdictAccepted:
		return u.committed(root, run, reread.value.Current, reread.value.Previous)
	case verdictRetry:
		return u.retry(root, run, reread)
	case verdictFence:
		return u.fenceOwn(root, run, reread)
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

func (u *uploader) retry(root context.Context, run *casRun, reread *pointer) error {
	if !u.store.lease.Valid() {
		return u.settle(root, run, stepExhausted, errLeaseLost)
	}
	return u.update(root, run, reread, stepRetry)
}

func (u *uploader) fenceOwn(root context.Context, run *casRun, reread *pointer) error {
	if err := u.fence(root, reread); err != nil {
		return u.settle(root, run, stepFence, err)
	}
	u.takeBack(root, *run.written.ref())
	return failed(failCAS, fmt.Errorf("%w: %s", errFenced, run.written.name))
}

func (u *uploader) fence(root context.Context, found *pointer) error {
	if !u.store.lease.Valid() {
		return errLeaseLost
	}
	u.store.stats.fences.Add(1)
	data, err := encodePointer(&found.value)
	if err != nil {
		return err
	}
	return u.writePointer(root, data, found.revision)
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

func (u *uploader) committed(root context.Context, run *casRun, kept ...*objectRef) error {
	written := run.written
	u.landed = append(u.landed, &commitRecord{
		through: written.meta.Position.Sequence,
		at:      u.store.now(),
		measured: &measurement{
			bytes:    written.size,
			upload:   written.took,
			verify:   written.verified,
			uploadBs: rate(written.size, written.took),
			verifyBs: rate(written.size, written.verified),
		},
	})
	if !u.stopRequested() {
		u.retain(root, written.meta.Position.Sequence)
	}
	u.purgeLeft(root, run.left, kept...)
	return nil
}

func (u *uploader) retain(root context.Context, sequence uint64) {
	if u.feedClosed {
		return
	}
	through := processing.Position{
		Partition: u.store.options.Partition,
		Sequence:  sequence,
	}
	err := u.callWithin(root, feedCommitBudget, func(ctx context.Context) error {
		return u.feed.Commit(ctx, through)
	})
	if err != nil {
		u.retainFailed(fmt.Errorf("commit the feed through %d: %w", through.Sequence, err))
	}
}

func (u *uploader) retainFailed(err error) {
	if errors.Is(err, processing.ErrFeedClosed) {
		u.feedClosed = true
	}
	if !u.feedClosed && !u.store.lease.Valid() {
		u.store.stats.skip(skipRetentionSuspended)
		return
	}
	u.store.stats.fail(failCommit, err)
}

func (u *uploader) pointerNow(root context.Context) (*pointer, error) {
	var found pointer
	err := u.callWithin(root, pointerWriteBudget, func(ctx context.Context) error {
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
