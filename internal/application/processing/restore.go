package processing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

type checkpointSource interface {
	Latest(ctx context.Context, partition uint16) (CheckpointMeta, io.ReadCloser, error)
}

type Restored struct {
	Builder    *topology.Builder
	Through    Position
	ReceivedAt time.Time
	Meta       CheckpointMeta
	Found      bool
}

type restoreRequest struct {
	partition uint16
	limits    *topology.Limits
	startedAt time.Time
	options   []topology.Option
}

func Restore(
	ctx context.Context,
	source checkpointSource,
	partition uint16,
	limits *topology.Limits,
	startedAt time.Time,
	options ...topology.Option,
) (Restored, error) {
	request := restoreRequest{
		partition: partition,
		limits:    limits,
		startedAt: startedAt,
		options:   options,
	}
	meta, checkpoint, err := source.Latest(ctx, partition)
	restored, err := request.from(&meta, checkpoint, err)
	if checkpoint != nil {
		err = request.closeInto(checkpoint, err)
	}
	if err != nil {
		return Restored{}, err
	}
	return restored, nil
}

func (r *restoreRequest) from(meta *CheckpointMeta, checkpoint io.Reader, latest error) (Restored, error) {
	switch {
	case errors.Is(latest, ErrNoCheckpoint):
		return r.fresh()
	case latest != nil:
		return Restored{}, fmt.Errorf("latest checkpoint of partition %d: %w", r.partition, restoreFailure(latest))
	case checkpoint == nil:
		return Restored{}, fmt.Errorf("%w: partition %d has a checkpoint without data", ErrCheckpointRefused,
			r.partition)
	}
	return r.restore(meta, checkpoint)
}

func (r *restoreRequest) fresh() (Restored, error) {
	builder, err := topology.NewBuilder(r.limits, r.startedAt, r.options...)
	if err != nil {
		return Restored{}, fmt.Errorf("new builder for partition %d: %w", r.partition, err)
	}
	return Restored{
		Builder: builder,
		Through: Position{
			Partition: r.partition,
		},
	}, nil
}

func (r *restoreRequest) restore(meta *CheckpointMeta, checkpoint io.Reader) (Restored, error) {
	if meta.Position.Partition != r.partition {
		return Restored{}, fmt.Errorf("%w: partition %d got the checkpoint through %d of partition %d",
			ErrForeignCheckpoint, r.partition, meta.Position.Sequence, meta.Position.Partition)
	}
	builder, err := topology.RestoreBuilder(r.limits, checkpoint, r.options...)
	if err != nil {
		return Restored{}, fmt.Errorf("restore partition %d through %d: %w", r.partition, meta.Position.Sequence,
			restoreFailure(err))
	}
	return Restored{
		Builder:    builder,
		Through:    meta.Position,
		ReceivedAt: meta.ReceivedAt,
		Meta:       *meta,
		Found:      true,
	}, nil
}

func restoreFailure(err error) error {
	switch {
	case labelled(err):
		return err
	case errors.Is(err, topology.ErrCheckpointCorrupt) || errors.Is(err, topology.ErrCheckpointIncompatible):
		return errors.Join(ErrCheckpointUnusable, err)
	}
	return errors.Join(ErrCheckpointRefused, err)
}

func (r *restoreRequest) closeInto(checkpoint io.Closer, err error) error {
	if closeErr := checkpoint.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("close checkpoint of partition %d: %w", r.partition, restoreFailure(closeErr)))
	}
	return err
}
