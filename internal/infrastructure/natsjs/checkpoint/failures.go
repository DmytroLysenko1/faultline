package checkpoint

import (
	"context"
	"errors"
	"io"
	"slices"

	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

type translation struct {
	cause error
	layer error
}

type detachedError struct {
	text string
}

func (d detachedError) Error() string {
	return d.text
}

func translations() []translation {
	return []translation{
		{cause: errPointerForeign, layer: processing.ErrCheckpointRefused},
		{cause: errPointerMoved, layer: processing.ErrCheckpointMoved},
		{cause: errObjectChanged, layer: processing.ErrCheckpointMoved},
		{cause: errCheckpointMissing, layer: processing.ErrCheckpointUnusable},
		{cause: errStreamRecreated, layer: processing.ErrCheckpointUnusable},
		{cause: errCorrupt, layer: processing.ErrCheckpointUnusable},
		{cause: errReadIncomplete, layer: processing.ErrCheckpointUnusable},
		{cause: errEpochRegressed, layer: processing.ErrCheckpointRefused},
		{cause: errStoreState, layer: processing.ErrCheckpointRefused},
		{cause: errStoreClosed, layer: processing.ErrCheckpointRefused},
		{cause: errDiscardTarget, layer: processing.ErrDiscardRefused},
		{cause: errLeaseLost, layer: processing.ErrLeaseLost},
		{cause: errSweepLeaseLost, layer: processing.ErrLeaseLost},
		{cause: errFenced, layer: processing.ErrCheckpointRefused},
		{cause: errCASUnknown, layer: processing.ErrCheckpointRefused},
		{cause: errUnresolved, layer: processing.ErrCheckpointRefused},
		{cause: errPositionRegressed, layer: processing.ErrCheckpointRefused},
		{cause: errNameCollision, layer: processing.ErrCheckpointRefused},
	}
}

func passedThrough() []error {
	return []error{
		context.Canceled,
		processing.ErrNoCheckpoint,
		processing.ErrCheckpointBusy,
		processing.ErrForeignCheckpoint,
		ErrInvalidOptions,
		ErrBucketDrift,
	}
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	if found := slices.IndexFunc(translations(), func(t translation) bool { return errors.Is(err, t.cause) }); found >= 0 {
		return errors.Join(translations()[found].layer, err)
	}
	if stopCaused(err) {
		return unavailable(err)
	}
	if slices.ContainsFunc(passedThrough(), func(own error) bool { return errors.Is(err, own) }) {
		return err
	}
	return unavailable(err)
}

func unavailable(err error) error {
	timedOut := errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded)
	if errors.Is(err, io.EOF) {
		err = detachedError{
			text: err.Error(),
		}
	}
	if timedOut {
		return errors.Join(processing.ErrCheckpointUnavailable, context.DeadlineExceeded, err)
	}
	return errors.Join(processing.ErrCheckpointUnavailable, err)
}
