package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var errLeftOver = errors.New("checkpoint object still holds messages after its purge")

func (u *uploader) dropped(root context.Context, written *object, cause error) error {
	if err := u.purgeOwn(root, written); err != nil {
		return terminal(fmt.Errorf("%w: %s after %w: %w", errPurgeLeft, written.name, detachedCause{
			text: cause.Error(),
		}, err))
	}
	return u.classified(cause)
}

func (u *uploader) classified(cause error) error {
	known, isCycle := errors.AsType[*cycleError](cause)
	if isCycle && known.kind == kindTerminal {
		return cause
	}
	if u.stopRequested() {
		return stopped(cause)
	}
	if isCycle {
		return cause
	}
	return failed(failUpload, cause)
}

// purgeOwn has no lease check on purpose (no pointer names the object, C11) and purges the meta before the
// chunks, so a purge cut in the middle never leaves a meta naming chunks that are gone.
func (u *uploader) purgeOwn(root context.Context, written *object) error {
	subjects := []string{u.store.chunkSubject(written.nuid)}
	own, err := u.ownMetaLanded(root, written)
	if err != nil {
		return err
	}
	if own {
		subjects = append([]string{u.store.metaSubject(written.name)}, subjects...)
	}
	return u.purgeSubjects(root, subjects)
}

func (u *uploader) ownMetaLanded(root context.Context, written *object) (bool, error) {
	var last *jetstream.RawStreamMsg
	err := u.callWithin(root, sweepCallBudget, func(ctx context.Context) error {
		var err error
		last, err = u.store.lastMessage(ctx, u.store.metaSubject(written.name))
		return err
	})
	if errors.Is(err, errNoMessage) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read the meta of %s: %w", written.name, err)
	}
	return last.Header.Get(jetstream.MsgIDHeader) == written.metaID(), nil
}

func (u *uploader) purgeSubjects(root context.Context, subjects []string) error {
	for _, subject := range subjects {
		if err := u.callWithin(root, sweepCallBudget, func(ctx context.Context) error {
			return u.store.buckets.bucket.Purge(ctx, jetstream.WithPurgeSubject(subject))
		}); err != nil {
			return fmt.Errorf("purge %s: %w", subject, err)
		}
	}
	return u.nothingLeft(root, subjects)
}

func (u *uploader) nothingLeft(root context.Context, subjects []string) error {
	for _, subject := range subjects {
		var held uint64
		err := u.callWithin(root, sweepCallBudget, func(ctx context.Context) error {
			var err error
			held, err = u.subjectCount(ctx, subject)
			return err
		})
		if err != nil {
			return fmt.Errorf("count %s after its purge: %w", subject, err)
		}
		if held > 0 {
			return fmt.Errorf("%w: %s holds %d", errLeftOver, subject, held)
		}
	}
	return nil
}

func (u *uploader) callWithin(
	root context.Context,
	budget time.Duration,
	call func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(root, budget)
	defer cancel()
	return call(ctx)
}
