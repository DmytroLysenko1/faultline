package checkpoint

import (
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const partialRead = 16

func readSome(t *testing.T, reader io.Reader) {
	t.Helper()
	if _, err := io.ReadFull(reader, make([]byte, partialRead)); err != nil {
		t.Fatalf("read the first %d bytes: %v", partialRead, err)
	}
}

func drain(t *testing.T, reader io.Reader) {
	t.Helper()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("read to the end: %v", err)
	}
}

func TestAnEarlyCloseKeepsTheDiscardPath(t *testing.T) {
	w := startWorld(t)
	ref, _, reader := w.latestObject(t)
	readSome(t, reader)
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, stakeErr := w.store.Stake(t.Context())
	staked, discardErr := w.store.Discard(t.Context(), processing.DiscardTarget{
		Partition:  testPartition,
		Checkpoint: ref.NUID,
	})

	if !errors.Is(stakeErr, processing.ErrCheckpointRefused) {
		t.Errorf("Stake after an early close = %v, want %v", stakeErr, processing.ErrCheckpointRefused)
	}
	if discardErr != nil {
		t.Fatalf("Discard of the object the domain gave up on: %v", discardErr)
	}
	if diff := cmp.Diff(processing.Staked{After: processing.Position{Partition: testPartition}}, staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
}

func TestALateCloseDoesNotTouchTheNextVerdict(t *testing.T) {
	w := startWorld(t)
	_, _, first := w.latestObject(t)
	_, second, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("second Latest: %v", err)
	}
	drain(t, second)
	if err := second.Close(); err != nil {
		t.Fatalf("close the second reader: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close the first reader: %v", err)
	}
	_, err = w.store.Stake(t.Context())

	if err != nil {
		t.Fatalf("Stake after the first reader closed late: %v", err)
	}
}

func TestALateReadOfAnEarlierLatestKeepsAnEmptyVerdict(t *testing.T) {
	w := startWorld(t)
	_, _, first := w.latestObject(t)
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
		StreamCreated: w.streamCreated(t).Format(createdLayout)})
	if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("second Latest = %v, want ErrNoCheckpoint", err)
	}

	drain(t, first)
	if err := first.Close(); err != nil {
		t.Fatalf("close the first reader: %v", err)
	}
	staked, err := w.store.Stake(t.Context())

	if err != nil {
		t.Fatalf("Stake of the empty pointer: %v", err)
	}
	if diff := cmp.Diff(processing.Staked{After: processing.Position{Partition: testPartition}}, staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s, want the empty start, not the object the first reader verified", diff)
	}
}

func TestTheUploadReconcileReaderNeverSettlesTheVerdict(t *testing.T) {
	w := startWorld(t)
	info := w.objectInfo(t, w.found(t).Name)
	reconciling := w.store.readerWith(t.Context(), info, func() error { return nil })

	drain(t, reconciling)
	_, err := w.store.Stake(t.Context())

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Fatalf("Stake after only a reconcile reader read the object = %v, want %v", err,
			processing.ErrCheckpointRefused)
	}
}

func TestDiscardAfterAMovedReadIsRefused(t *testing.T) {
	w := startWorld(t)
	ref, _, reader := w.latestObject(t)
	moved := w.putObject(t, olderEpoch, restoredAt+1, imageOf(imageBytes))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
		StreamCreated: w.streamCreated(t).Format(createdLayout), Current: moved})
	w.purgeChunks(t, ref.NUID)

	_, readErr := io.Copy(io.Discard, reader)
	_, discardErr := w.store.Discard(t.Context(), processing.DiscardTarget{
		Partition:  testPartition,
		Checkpoint: ref.NUID,
	})

	if !errors.Is(readErr, processing.ErrCheckpointMoved) {
		t.Fatalf("read of an object the pointer left = %v, want %v", readErr, processing.ErrCheckpointMoved)
	}
	if !errors.Is(discardErr, processing.ErrCheckpointRefused) {
		t.Errorf("Discard after the pointer moved under the read = %v, want %v", discardErr,
			processing.ErrCheckpointRefused)
	}
}

var errStakeWrite = errors.New("pointer write failed")

func TestAReadEndingWhileAStakeFailsStillCountsForTheNextStake(t *testing.T) {
	w := startWorld(t)
	_, _, reader := w.latestObject(t)
	staking, err := w.store.enter(t.Context())
	if err != nil {
		t.Fatalf("enter: %v", err)
	}

	drain(t, reader)
	if err := w.store.exitStake(errStakeWrite); !errors.Is(err, errStakeWrite) {
		t.Fatalf("exitStake = %v, want %v", err, errStakeWrite)
	}
	if staking.Err() == nil {
		t.Fatal("the failed stake left its context running")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err = w.store.Stake(t.Context())

	if err != nil {
		t.Fatalf("Stake after a read that ended during a failed stake: %v", err)
	}
}

func TestAReadAfterTheEndKeepsAnsweringTheEnd(t *testing.T) {
	w := startWorld(t)
	_, _, reader := w.latestObject(t)
	drain(t, reader)

	_, again := reader.Read(make([]byte, partialRead))
	_, third := reader.Read(make([]byte, partialRead))

	if !errors.Is(again, io.EOF) || !errors.Is(third, io.EOF) {
		t.Errorf("reads after the end = %v, %v, want io.EOF both times", again, third)
	}
}

func TestNoTargetIsNamedOnceTheStoreIsStakedOrClosed(t *testing.T) {
	w := startWorld(t)
	ref, _, reader := w.latestObject(t)
	readSome(t, reader)
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	before, named := w.store.Refused(t.Context())
	if !named || before.Checkpoint != ref.NUID {
		t.Fatalf("Refused before the discard = %+v, %v, want the object", before, named)
	}
	if _, err := w.store.Discard(t.Context(), before); err != nil {
		t.Fatalf("Discard: %v", err)
	}

	after, stillNamed := w.store.Refused(t.Context())

	if stillNamed {
		t.Errorf("Refused named %+v after the partition was discarded and staked", after)
	}
}

func TestACloseThatWinsTheEndOfTheReadIsNoEnd(t *testing.T) {
	w := startWorld(t)
	_, _, latest := w.latestObject(t)
	reader, isObject := latest.(*objectReader)
	if !isObject {
		t.Fatalf("Latest returned %T, want *objectReader", latest)
	}
	fetch := reader.fetch
	reader.fetch = func(sequence uint64) (*jetstream.RawStreamMsg, error) {
		found, err := fetch(sequence)
		if reader.read == reader.info.Chunks {
			if closeErr := reader.Close(); closeErr != nil {
				t.Errorf("close: %v", closeErr)
			}
		}
		return found, err
	}

	_, err := io.Copy(io.Discard, reader)
	_, stakeErr := w.store.Stake(t.Context())

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Errorf("read that lost its end to Close = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
	if !errors.Is(stakeErr, processing.ErrCheckpointRefused) {
		t.Errorf("Stake after Close won the end = %v, want %v", stakeErr, processing.ErrCheckpointRefused)
	}
}
