package processing

import (
	"context"
	"errors"
	"slices"
	"time"
)

var (
	ErrForeignCheckpoint     = errors.New("checkpoint belongs to another partition")
	ErrCheckpointMoved       = errors.New("checkpoint moved while the partition started")
	ErrCheckpointUnusable    = errors.New("checkpoint cannot start the partition")
	ErrCheckpointRefused     = errors.New("checkpoint store refused to start the partition")
	ErrCheckpointUnavailable = errors.New("checkpoint store unavailable")
	ErrDiscardRefused        = errors.New("checkpoint discard refused")
	ErrLeaseLost             = errors.New("partition lease lost")
)

type Staked struct {
	After      Position
	ReceivedAt time.Time
	Created    time.Time
}

type DiscardTarget struct {
	Partition  uint16
	Checkpoint string
	FirstSeq   uint64
}

type startupFailure struct {
	target        error
	blocksDiscard bool
}

func Discardable(err error) bool {
	if !errors.Is(err, ErrCheckpointUnusable) {
		return false
	}
	return !slices.ContainsFunc(startupFailures(), func(failure startupFailure) bool {
		return failure.blocksDiscard && errors.Is(err, failure.target)
	})
}

func startupFailures() []startupFailure {
	return []startupFailure{
		{target: ErrNoCheckpoint},
		{target: ErrCheckpointUnusable},
		{target: ErrForeignCheckpoint, blocksDiscard: true},
		{target: ErrCheckpointMoved, blocksDiscard: true},
		{target: ErrCheckpointRefused, blocksDiscard: true},
		{target: ErrCheckpointUnavailable, blocksDiscard: true},
		{target: ErrDiscardRefused, blocksDiscard: true},
		{target: ErrLeaseLost, blocksDiscard: true},
		{target: context.Canceled, blocksDiscard: true},
		{target: context.DeadlineExceeded, blocksDiscard: true},
	}
}

func labelled(err error) bool {
	return slices.ContainsFunc(startupFailures(), func(failure startupFailure) bool {
		return errors.Is(err, failure.target)
	})
}
