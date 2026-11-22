package processingtest_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
)

const (
	fakePartition   = 3
	fakeEpoch       = 7
	fakeImageSize   = 64
	fakeStreamStart = 17
)

var (
	errPointerUnreadable = errors.New("pointer unreadable")
	errChunkMissing      = errors.New("chunk missing")
)

func fakeMeta(sequence uint64) *processing.CheckpointMeta {
	return &processing.CheckpointMeta{
		Position: processing.Position{
			Partition: fakePartition,
			Sequence:  sequence,
		},
		Epoch: fakeEpoch,
	}
}

func fakeImage() []byte {
	return bytes.Repeat([]byte{1}, fakeImageSize)
}

func TestTheFakeStoreKeepsTheResumerContract(t *testing.T) {
	processingtest.RunCheckpointResumerContract(t, func(t *testing.T) processingtest.ResumerHarness {
		store := processingtest.NewCheckpointStore(t, nil)
		var current uint64
		return processingtest.ResumerHarness{
			Store:     store,
			Partition: fakePartition,
			WithCheckpoint: func(sequence uint64) (processing.DiscardTarget, error) {
				store.StartStreamAt(fakeStreamStart)
				current = sequence
				store.Put(fakePartition, fakeMeta(sequence), fakeImage())
				return processing.DiscardTarget{
					Partition:  fakePartition,
					Checkpoint: processingtest.CheckpointName(fakeMeta(sequence)),
				}, nil
			},
			Corrupt: func() (processing.DiscardTarget, error) {
				store.StartStreamAt(fakeStreamStart)
				store.FailLatest(errors.Join(processing.ErrCheckpointUnusable, errPointerUnreadable))
				return processing.DiscardTarget{Partition: fakePartition, FirstSeq: fakeStreamStart}, nil
			},
			BreakImage: func() error {
				store.FailReads(errors.Join(processing.ErrCheckpointUnusable, errChunkMissing))
				return nil
			},
			Unpoint: func() error {
				store.Remove(fakePartition)
				return nil
			},
			MovePointer: func() error {
				current++
				store.MovePointer(fakePartition, fakeMeta(current), fakeImage())
				return nil
			},
		}
	})
}
