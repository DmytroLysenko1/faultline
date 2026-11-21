package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

var errUnresolved = errors.New("checkpoint objects of this holder still wait for a decision")

type pendingObject struct {
	ref      objectRef
	updated  bool
	revision uint64
	left     []objectRef
	metaSent bool
	final    bool
}

func (u *uploader) resolve(ctx context.Context) error {
	if len(u.pending) == 0 {
		return nil
	}
	left := u.pending
	u.pending = nil
	for i, item := range left {
		if err := u.decide(ctx, &item); err != nil {
			u.pending = append(u.pending, left[i:]...)
			u.store.stats.pending.Store(uint64(len(u.pending)))
			return err
		}
	}
	u.store.stats.pending.Store(uint64(len(u.pending)))
	if len(u.pending) > 0 {
		return failed(failBucketUnresolved, fmt.Errorf("%w: %d", errUnresolved, len(u.pending)))
	}
	return nil
}

func (u *uploader) decide(ctx context.Context, item *pendingObject) error {
	found, err := u.pointerNow(ctx)
	if err != nil {
		return failed(failBucketUnresolved, fmt.Errorf("%w: read the pointer for %s: %w", errUnresolved, item.ref.Name,
			err))
	}
	decided := judgePending(found, u.store.options.Epoch, item)
	if ended := epochEnded(decided, item.ref.Name); ended != nil {
		return ended
	}
	u.act(ctx, item, found, decided)
	return nil
}

func (u *uploader) purgeLeft(ctx context.Context, left []objectRef, kept ...*objectRef) {
	for _, ref := range left {
		if slices.ContainsFunc(kept, func(named *objectRef) bool {
			return sameObject(named, &ref)
		}) {
			continue
		}
		u.leftBehind(ref, u.takeBack(ctx, ref, true))
	}
}

func (u *uploader) leftBehind(ref objectRef, err error) {
	switch {
	case err == nil:
	case stopCaused(err):
		u.store.stats.previousLeft.Add(1)
	default:
		u.wait(&pendingObject{
			ref:      ref,
			metaSent: true,
		})
	}
}

func (u *uploader) act(ctx context.Context, item *pendingObject, found *pointer, decided verdict) {
	switch decided {
	case verdictAccepted:
		u.accepted(ctx, item, found)
	case verdictPurge:
		u.takeBackOrWait(ctx, item.ref, item.metaSent)
	case verdictFence:
		u.fencePending(ctx, item, found)
	case verdictReferenced:
		u.purgeLeft(ctx, item.left, found.value.Current, found.value.Previous)
	default:
		u.wait(item)
	}
}

func (u *uploader) fencePending(ctx context.Context, item *pendingObject, found *pointer) {
	if u.fence(ctx, found) != nil {
		u.wait(item)
		return
	}
	u.takeBackOrWait(ctx, item.ref, item.metaSent)
}

func (u *uploader) accepted(ctx context.Context, item *pendingObject, found *pointer) {
	u.landed = append(u.landed, &commitRecord{
		through: item.ref.Position,
		at:      u.store.now(),
		final:   item.final,
	})
	if u.retains(item.final) {
		u.retain(ctx, item.ref.Position)
	}
	u.purgeLeft(ctx, item.left, found.value.Current, found.value.Previous)
}

func (p *pendingObject) unwritable(revision uint64) bool {
	return !p.updated || revision > p.revision
}

func (u *uploader) wait(item *pendingObject) {
	u.pending = append(u.pending, *item)
	u.store.stats.pending.Store(uint64(len(u.pending)))
}
