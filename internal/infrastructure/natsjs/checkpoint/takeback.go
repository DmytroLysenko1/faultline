package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const inFlightRemnant = 1

var errLeftOver = errors.New("checkpoint object still holds messages after its purge")

type metaFound uint8

const (
	metaNone metaFound = iota
	metaOwn
	metaForeign
)

type leftOverError struct {
	name   string
	chunks uint64
	meta   uint64
}

func (e *leftOverError) Error() string {
	return fmt.Sprintf("%s: %s holds %d chunks and %d metas", errLeftOver, e.name, e.chunks, e.meta)
}

func (e *leftOverError) Is(target error) bool {
	return target == errLeftOver
}

func (u *uploader) dropped(ctx context.Context, written *object, cause error) error {
	outcome := u.classified(cause)
	err := u.takeBack(ctx, *written.ref(), written.metaSent)
	if err != nil && !u.remnantOnly(err) {
		u.wait(written.pending())
	}
	return outcome
}

func (u *uploader) classified(cause error) error {
	known, isCycle := errors.AsType[*cycleError](cause)
	switch {
	case isCycle && known.kind == kindTerminal:
		return cause
	case stopCaused(cause):
		return stopped(cause)
	case isCycle:
		return cause
	default:
		return failed(failUpload, cause)
	}
}

func (u *uploader) remnantOnly(err error) bool {
	left, counted := errors.AsType[*leftOverError](err)
	return counted && u.stopRequested() &&
		left.chunks+left.meta <= inFlightRemnant
}

func (o *object) pending() *pendingObject {
	return &pendingObject{
		ref:      *o.ref(),
		metaSent: o.metaSent,
		final:    o.final,
	}
}

func (u *uploader) takeBackOrWait(ctx context.Context, ref objectRef, metaSent bool) {
	if err := u.takeBack(ctx, ref, metaSent); err != nil {
		u.wait(&pendingObject{
			ref:      ref,
			metaSent: metaSent,
		})
	}
}

func (u *uploader) takeBack(ctx context.Context, ref objectRef, metaSent bool) error {
	err := u.purgeRef(ctx, ref, metaSent)
	if errors.Is(err, errLeftOver) {
		u.store.stats.purgeLeftover.Add(1)
	}
	return err
}

func (u *uploader) purgeRef(ctx context.Context, ref objectRef, metaSent bool) error {
	found, err := u.metaOf(ctx, ref)
	if err != nil {
		return err
	}
	chunks := u.store.chunkSubject(ref.NUID)
	meta := u.metaToPurge(found, ref, metaSent)
	if err := u.purgeSubject(ctx, meta); err != nil {
		return err
	}
	if err := u.purgeSubject(ctx, chunks); err != nil {
		return err
	}
	return u.nothingLeft(ctx, ref.Name, chunks, meta)
}

func (u *uploader) metaToPurge(found metaFound, ref objectRef, metaSent bool) string {
	if found == metaOwn || found == metaNone && metaSent {
		return u.store.metaSubject(ref.Name)
	}
	return ""
}

func (u *uploader) metaOf(ctx context.Context, ref objectRef) (metaFound, error) {
	var last *jetstream.RawStreamMsg
	err := u.controlWithin(ctx, sweepCallBudget, func(ctx context.Context) error {
		var err error
		last, err = u.store.lastMessage(ctx, u.store.metaSubject(ref.Name))
		return err
	})
	switch {
	case errors.Is(err, errNoMessage):
		return metaNone, nil
	case err != nil:
		return metaNone, fmt.Errorf("read the meta of %s: %w", ref.Name, err)
	case describes(last.Data, ref.NUID):
		return metaOwn, nil
	default:
		return metaForeign, nil
	}
}

func (u *uploader) purgeSubject(ctx context.Context, subject string) error {
	if subject == "" {
		return nil
	}
	err := u.controlWithin(ctx, sweepCallBudget, func(ctx context.Context) error {
		return u.store.buckets.bucket.Purge(ctx, jetstream.WithPurgeSubject(subject))
	})
	if err != nil {
		return fmt.Errorf("purge %s: %w", subject, err)
	}
	return nil
}

func (u *uploader) nothingLeft(ctx context.Context, name, chunks, meta string) error {
	heldChunks, err := u.heldOn(ctx, chunks)
	if err != nil {
		return err
	}
	heldMeta, err := u.heldOn(ctx, meta)
	if err != nil {
		return err
	}
	if heldChunks+heldMeta == 0 {
		return nil
	}
	return &leftOverError{
		name:   name,
		chunks: heldChunks,
		meta:   heldMeta,
	}
}

func (u *uploader) heldOn(ctx context.Context, subject string) (uint64, error) {
	if subject == "" {
		return 0, nil
	}
	var held uint64
	err := u.controlWithin(ctx, sweepCallBudget, func(ctx context.Context) error {
		var err error
		held, err = u.subjectCount(ctx, subject)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("count %s after its purge: %w", subject, err)
	}
	return held, nil
}

func describes(data []byte, nuid string) bool {
	var body metaBody
	return json.Unmarshal(data, &body) == nil && body.NUID == nuid
}
