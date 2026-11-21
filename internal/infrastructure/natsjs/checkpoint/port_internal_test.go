package checkpoint

import (
	"bytes"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const writeLeaseChecks = 2

func TestAnEmptyStakeRacedByAnotherCreateIsAMovedPointer(t *testing.T) {
	w := startWorld(t)
	if err := latestErr(w.store.Latest(t.Context(), testPartition)); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	var raced atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isPointerWrite(f) && raced.CompareAndSwap(false, true) {
			w.putPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch})
		}
		return natsjstest.Forward()
	})

	_, err := w.store.StakeEmpty(t.Context())

	if got, _ := w.pointer(t); !errors.Is(err, processing.ErrCheckpointMoved) || got.Epoch != olderEpoch {
		t.Fatalf("StakeEmpty: err = %v, epoch %d; want %v and the other writer's pointer", err, got.Epoch,
			processing.ErrCheckpointMoved)
	}
}

func (w *world) journal(t *testing.T) jetstream.Stream {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), w.options.Topology.Stream(w.options.Partition))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	return stream
}

func TestLatestTellsAFirstStartFromLostState(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, w *world)
		wantErr error
	}{
		{name: "no pointer over an empty stream is a first start", arrange: func(*testing.T, *world) {},
			wantErr: processing.ErrNoCheckpoint},
		{name: "no pointer over a stream that starts at its first record is a first start",
			arrange: func(t *testing.T, w *world) {
				if _, err := w.operator.Publish(t.Context(), journalSubject, []byte("r")); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}, wantErr: processing.ErrNoCheckpoint},
		{name: "no pointer and no stream yet is a first start", arrange: func(t *testing.T, w *world) {
			if err := w.operator.DeleteStream(t.Context(), w.options.Topology.Stream(0)); err != nil {
				t.Fatalf("delete journal: %v", err)
			}
		}, wantErr: processing.ErrNoCheckpoint},
		{name: "a pointer with a checkpoint but no stream is a recreated stream", arrange: func(t *testing.T, w *world) {
			ref := w.putObject(t, olderEpoch, restoredAt, imageOf(9))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref})
			if err := w.operator.DeleteStream(t.Context(), w.options.Topology.Stream(0)); err != nil {
				t.Fatalf("delete journal: %v", err)
			}
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "no pointer over a trimmed stream is lost state", arrange: func(t *testing.T, w *world) {
			for range 2 {
				if _, err := w.operator.Publish(t.Context(), "journal.p0", []byte("r")); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}
			if err := w.journal(t).Purge(t.Context(), jetstream.WithPurgeSequence(2)); err != nil {
				t.Fatalf("trim: %v", err)
			}
		}, wantErr: processing.ErrCheckpointUnusable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			tt.arrange(t, w)

			_, reader, err := w.store.Latest(t.Context(), testPartition)
			closeOnCleanup(t, reader)

			if !errors.Is(err, tt.wantErr) || reader != nil {
				t.Fatalf("Latest: err = %v, reader %v; want %v", err, reader, tt.wantErr)
			}
		})
	}
}

func TestAChunkReadAfterTheBucketWasDeletedIsLostState(t *testing.T) {
	w := startWorld(t)
	w.restorable(t)

	err := readAfterFirstChunk(t, w, func() {
		if err := w.operator.DeleteObjectStore(t.Context(), w.options.objectBucket()); err != nil {
			t.Fatalf("delete bucket: %v", err)
		}
	})

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("read: err = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
}

func TestAMetaGoneBeforeItWasReadDoesNotStopTheSweep(t *testing.T) {
	w := startWorld(t)
	stale := w.putObject(t, olderEpoch, 20, imageOf(9))
	other := w.putObject(t, olderEpoch, 21, imageOf(9))
	_, meta := w.restorable(t)
	subject := []byte(`"last_by_subj":"` + w.metaSubject(stale.Name) + `"`)
	var gone atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if bytes.Contains(f.Payload, subject) && gone.CompareAndSwap(false, true) {
			w.purgeSubject(t, w.metaSubject(stale.Name))
		}
		return natsjstest.Forward()
	})

	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake: %v", err)
	}

	if got := w.store.Stats(); !gone.Load() || got.SweepFailures != 0 || got.SweptObjects != 1 {
		t.Errorf("stats %+v (meta purged first: %t), want %s swept and no failure for the meta that was gone", got,
			gone.Load(), other.Name)
	}
	if slices.Contains(w.subjects(t), w.metaSubject(other.Name)) {
		t.Errorf("the sweep stopped before %s", other.Name)
	}
}

func (w *world) purgeSubject(t *testing.T, subject string) {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), objStreamPrefix+w.options.objectBucket())
	if err != nil {
		t.Errorf("object stream: %v", err)
		return
	}
	if err := stream.Purge(t.Context(), jetstream.WithPurgeSubject(subject)); err != nil {
		t.Errorf("purge %s: %v", subject, err)
	}
}

func TestASweepStopsWithoutAValidLeaseAndIsNoFailure(t *testing.T) {
	w := startWorld(t)
	stale := w.putObject(t, olderEpoch, 20, imageOf(9))
	_, meta := w.restorable(t)
	before := w.subjects(t)
	w.store.lease = &validTimes{left: writeLeaseChecks}

	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake: %v", err)
	}

	if diff := cmp.Diff(before, w.subjects(t)); diff != "" {
		t.Errorf("a sweep without a valid lease purged %s (-before +after):\n%s", stale.Name, diff)
	}
	got := w.store.Stats()
	if got.LastSweepError != nil || got.SweptObjects != 0 || got.SweepFailures != 0 {
		t.Errorf("stats %+v, want a sweep the suspended lease stopped before purging %s, no failure counted", got, stale.Name)
	}
}
