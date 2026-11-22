package checkpoint

import (
	"bytes"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const corruptPointer = "{not a pointer"

func TestTheStoreKeepsTheResumerContract(t *testing.T) {
	processingtest.RunCheckpointResumerContract(t, func(t *testing.T) processingtest.ResumerHarness {
		w := startWorld(t)
		return storeHarness(t, w)
	})
}

func storeHarness(t *testing.T, w *world) processingtest.ResumerHarness {
	t.Helper()
	w.trim(t)
	var current *objectRef
	point := func(ref *objectRef) {
		w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
			StreamCreated: w.streamCreated(t).Format(createdLayout), Current: ref, DiscardedAt: trimmedAt})
	}
	return processingtest.ResumerHarness{
		Store:     w.store,
		Partition: testPartition,
		WithCheckpoint: func(sequence uint64) (processing.DiscardTarget, error) {
			current = w.putObject(t, olderEpoch, sequence, imageOf(imageBytes))
			point(current)
			return processing.DiscardTarget{Partition: testPartition, Checkpoint: current.NUID}, nil
		},
		Corrupt: func() (processing.DiscardTarget, error) {
			w.setRawPointer(t, corruptPointer)
			return processing.DiscardTarget{
				Partition: testPartition,
				FirstSeq:  w.journal(t).CachedInfo().State.FirstSeq,
			}, nil
		},
		BreakImage: func() error {
			w.purgeChunks(t, current.NUID)
			return nil
		},
		Unpoint: func() error {
			point(nil)
			return nil
		},
		MovePointer: func() error {
			left := current
			current = w.putObject(t, olderEpoch, left.Position+1, imageOf(imageBytes))
			point(current)
			w.purgeChunks(t, left.NUID)
			return nil
		},
	}
}

func TestResumeStakesARealImageReadFromTheStore(t *testing.T) {
	w := startWorld(t)
	limits := processingtest.Limits()
	startedAt := time.Unix(0, 0).UTC()
	builder, err := topology.NewBuilder(&limits, startedAt)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	var image bytes.Buffer
	if err := builder.WriteCheckpoint(&image); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	ref := w.putObject(t, olderEpoch, restoredAt, image.Bytes())
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
		StreamCreated: w.streamCreated(t).Format(createdLayout), Current: ref})

	resumed, err := processing.Resume(t.Context(), w.store, processing.ResumeRequest{
		Partition: testPartition,
		Limits:    &limits,
		StartedAt: startedAt,
	})

	if err != nil {
		t.Fatalf("Resume of a real image: %v", err)
	}
	if !resumed.Found || resumed.Staked.After.Sequence != restoredAt {
		t.Errorf("resumed = %+v, want the image through %d found and staked", resumed, restoredAt)
	}
}
