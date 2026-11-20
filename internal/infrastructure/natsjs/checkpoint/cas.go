package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const feedCommitBudget = 5 * time.Second

var errPositionRegressed = errors.New("checkpoint pointer is past this checkpoint on the same stream")

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
		return u.update(root, written, found)
	}
}

func (u *uploader) update(root context.Context, written *object, found *pointer) error {
	data, err := encodePointer(u.nextPointer(found, written))
	if err != nil {
		return u.dropped(root, written, failed(failCAS, err))
	}
	err = u.callWithin(root, pointerWriteBudget, func(ctx context.Context) error {
		_, err := u.store.buckets.pointers.Update(ctx, u.store.options.pointerKey(), data, found.revision)
		return err
	})
	if err == nil {
		return u.committed(root, written, found)
	}
	return u.settle(root, written, found, err)
}

func (u *uploader) settle(root context.Context, written *object, found *pointer, cause error) error {
	reread, err := u.pointerNow(root)
	if err != nil {
		return terminal(fmt.Errorf("%w: %s: %w; reread: %w", errCASUnknown, written.name, cause, err))
	}
	switch judgeAfter(reread, u.store.options.Epoch, written.ref()) {
	case verdictAccepted:
		return u.committed(root, written, found)
	case verdictLeaseLost:
		return terminal(fmt.Errorf("%w: a later epoch holds the pointer after %s: %w", errLeaseLost, written.name,
			detachedCause{
				text: cause.Error(),
			}))
	default:
		return terminal(fmt.Errorf("%w: %s: %w", errCASUnknown, written.name, cause))
	}
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

func (u *uploader) committed(root context.Context, written *object, found *pointer) error {
	u.landed = &commitRecord{
		through:  written.meta.Position.Sequence,
		at:       time.Now(),
		bytes:    written.size,
		upload:   written.took,
		verify:   written.verified,
		uploadBs: rate(written.size, written.took),
		verifyBs: rate(written.size, written.verified),
	}
	if !u.stopRequested() {
		u.retain(root, written)
	}
	return u.purgeLeft(root, found.value.Previous)
}

func (u *uploader) retain(root context.Context, written *object) {
	through := processing.Position{
		Partition: u.store.options.Partition,
		Sequence:  written.meta.Position.Sequence,
	}
	err := u.callWithin(root, feedCommitBudget, func(ctx context.Context) error {
		return u.feed.Commit(ctx, through)
	})
	if err != nil {
		u.store.stats.fail(failCommit, fmt.Errorf("commit the feed through %d: %w", through.Sequence, err))
	}
}

func (u *uploader) purgeLeft(root context.Context, left *objectRef) error {
	if left == nil {
		return nil
	}
	subjects := []string{u.store.metaSubject(left.Name), u.store.chunkSubject(left.NUID)}
	if err := u.purgeSubjects(root, subjects); err != nil {
		return terminal(fmt.Errorf("%w: %s left the pointer: %w", errPurgeLeft, left.Name, err))
	}
	return nil
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
