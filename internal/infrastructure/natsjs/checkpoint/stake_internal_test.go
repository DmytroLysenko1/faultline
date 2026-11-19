package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

func TestStakeTakesThePointerForTheNewEpochAndKeepsWhatTheOperatorWrote(t *testing.T) {
	w := startWorld(t)
	ref, meta := w.restorable(t)

	staked, err := w.store.Stake(t.Context(), &meta)

	if err != nil {
		t.Fatalf("Stake: %v", err)
	}
	created := w.streamCreated(t)
	got, _ := w.pointer(t)
	want := &pointerValue{Version: pointerVersion, Epoch: testEpoch, StreamCreated: created.Format(createdLayout),
		Current: ref}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("pointer after Stake (-want +got):\n%s", diff)
	}
	wantStaked := processing.Staked{After: meta.Position, ReceivedAt: testStart, Created: created}
	if diff := cmp.Diff(wantStaked, staked, cmpTimes()); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
}

func TestStakeRefusesAPointerThatMovedAfterLatest(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, w *world, ref *objectRef)
		wantErr error
	}{
		{name: "a zombie put another object in the pointer", arrange: func(t *testing.T, w *world, _ *objectRef) {
			other := w.putObject(t, olderEpoch, restoredAt+1, imageOf(9))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: other})
		}, wantErr: processing.ErrCheckpointMoved},
		{name: "the same name with another nuid is another object", arrange: func(t *testing.T, w *world, ref *objectRef) {
			twin := *ref
			twin.NUID = "ANOTHER"
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: &twin})
		}, wantErr: processing.ErrCheckpointMoved},
		{name: "the same object with another digest is another object", arrange: func(t *testing.T, w *world, ref *objectRef) {
			twin := *ref
			twin.Digest = corruptDigest()
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: &twin})
		}, wantErr: processing.ErrCheckpointMoved},
		{name: "a later holder took the pointer", arrange: func(t *testing.T, w *world, ref *objectRef) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch + 1, Current: ref})
		}, wantErr: processing.ErrCheckpointRefused},
		{name: "the lease is not valid", arrange: func(_ *testing.T, w *world, _ *objectRef) {
			w.lease.set(false)
		}, wantErr: processing.ErrLeaseLost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			ref, meta := w.restorable(t)
			tt.arrange(t, w, ref)
			before, _ := w.pointer(t)

			_, err := w.store.Stake(t.Context(), &meta)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Stake: err = %v, want %v", err, tt.wantErr)
			}
			if after, _ := w.pointer(t); !cmp.Equal(before, after) {
				t.Errorf("a refused Stake wrote the pointer:\n%s", cmp.Diff(before, after))
			}
		})
	}
}

func TestStakeNeedsTheCheckpointLatestReturned(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	meta.Position.Sequence++

	_, err := w.store.Stake(t.Context(), &meta)

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Fatalf("Stake: err = %v, want %v", err, processing.ErrCheckpointRefused)
	}
}

func TestASecondStakeIsOutOfOrder(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("first Stake: %v", err)
	}

	_, err := w.store.Stake(t.Context(), &meta)

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Fatalf("second Stake: err = %v, want %v", err, processing.ErrCheckpointRefused)
	}
}

func TestAStakeRacedByAnotherWriterIsAMovedPointer(t *testing.T) {
	w := startWorld(t)
	ref, meta := w.restorable(t)
	raced := &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref, DiscardedAt: 3}
	var once atomic.Bool
	w.proxy.intercept(func(f *frame) verdict {
		if isPointerWrite(f) && once.CompareAndSwap(false, true) {
			w.putPointer(t, raced)
		}
		return forward
	})

	_, err := w.store.Stake(t.Context(), &meta)

	if !errors.Is(err, processing.ErrCheckpointMoved) {
		t.Fatalf("Stake: err = %v, want %v", err, processing.ErrCheckpointMoved)
	}
	if got, _ := w.pointer(t); !cmp.Equal(raced, got) {
		t.Errorf("the racing write was overwritten:\n%s", cmp.Diff(raced, got))
	}
}

func TestAStakeRefusedIsNotStakedAndMayBeRetried(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	w.once(isPointerWrite, drop)
	if _, err := w.store.Stake(t.Context(), &meta); err == nil {
		t.Fatal("first Stake passed although its write never landed")
	}

	_, err := w.store.Stake(t.Context(), &meta)

	if got, _ := w.pointer(t); err != nil || got.Epoch != testEpoch {
		t.Fatalf("retried Stake: err = %v, epoch %d; want success at %d", err, got.Epoch, testEpoch)
	}
}

func TestStakeEmptyStartsAFreshPartition(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, w *world) *pointerValue
	}{
		{name: "the first start of the cluster creates the key", arrange: func(*testing.T, *world) *pointerValue {
			return &pointerValue{Version: pointerVersion, Epoch: testEpoch}
		}},
		{name: "a start after a discard carries the discard mark and the discarded object",
			arrange: func(t *testing.T, w *world) *pointerValue {
				discarded := w.putObject(t, olderEpoch, 3, imageOf(9))
				w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Previous: discarded,
					DiscardedAt: trimmedAt})
				w.trim(t)
				return &pointerValue{Version: pointerVersion, Epoch: testEpoch, Previous: discarded, DiscardedAt: trimmedAt}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			want := tt.arrange(t, w)
			if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrNoCheckpoint) {
				t.Fatalf("Latest: %v, want %v", err, processing.ErrNoCheckpoint)
			}

			staked, err := w.store.StakeEmpty(t.Context())

			if err != nil {
				t.Fatalf("StakeEmpty: %v", err)
			}
			if got, _ := w.pointer(t); !cmp.Equal(want, got) {
				t.Errorf("pointer (-want +got):\n%s", cmp.Diff(want, got))
			}
			if diff := cmp.Diff(processing.Staked{After: processing.Position{Partition: testPartition}}, staked); diff != "" {
				t.Errorf("staked (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStakeEmptyNeverWritesOverACheckpointThatAppeared(t *testing.T) {
	w := startWorld(t)
	if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	ref := w.putObject(t, olderEpoch, 3, imageOf(9))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref})

	_, err := w.store.StakeEmpty(t.Context())

	if got, _ := w.pointer(t); !errors.Is(err, processing.ErrCheckpointMoved) || got.Current == nil {
		t.Fatalf("StakeEmpty: err = %v, pointer %+v; want %v and the zombie checkpoint kept", err, got,
			processing.ErrCheckpointMoved)
	}
}

func TestStakeEmptyNeedsLatestWithoutACheckpoint(t *testing.T) {
	w := startWorld(t)

	_, err := w.store.StakeEmpty(t.Context())

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Fatalf("StakeEmpty before Latest: err = %v, want %v", err, processing.ErrCheckpointRefused)
	}
}

func TestStakeEmptyRefusesAnEmptyPointerRewrittenAfterLatest(t *testing.T) {
	w := startWorld(t)
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch})
	if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, DiscardedAt: 5})

	_, err := w.store.StakeEmpty(t.Context())

	if got, _ := w.pointer(t); !errors.Is(err, processing.ErrCheckpointMoved) || got.Epoch != olderEpoch {
		t.Fatalf("StakeEmpty: err = %v, epoch %d; want %v and nothing written", err, got.Epoch,
			processing.ErrCheckpointMoved)
	}
}

func TestACreateThatNeverLandedIsAMovedPointer(t *testing.T) {
	w := startWorld(t)
	if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	w.once(isPointerWrite, drop)

	_, err := w.store.StakeEmpty(t.Context())

	if _, exists := w.rawPointer(t); !errors.Is(err, processing.ErrCheckpointMoved) || exists {
		t.Fatalf("StakeEmpty: err = %v, key %t; want %v and no key", err, exists, processing.ErrCheckpointMoved)
	}
}

func TestAnEmptyStartNeedsLatestToHaveFoundNothing(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, w *world)
	}{
		{name: "no pointer over a trimmed stream", arrange: func(t *testing.T, w *world) { w.trim(t) }},
		{name: "a pointer that does not parse", arrange: func(t *testing.T, w *world) {
			w.setRawPointer(t, unparsedPointer)
		}},
		{name: "a stream that could not be described", arrange: func(_ *testing.T, w *world) {
			w.dropAll(w.isJournalInfo)
		}},
		{name: "an empty pointer over a trimmed stream", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch})
			w.trim(t)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			tt.arrange(t, w)
			ctx, cancel := context.WithTimeout(t.Context(), refusedWithin)
			defer cancel()
			if _, _, err := w.store.Latest(ctx, testPartition); err == nil || errors.Is(err, processing.ErrNoCheckpoint) {
				t.Fatalf("Latest: err = %v, want a refusal other than %v", err, processing.ErrNoCheckpoint)
			}
			w.proxy.intercept(nil)
			before, existed := w.rawPointer(t)

			_, err := w.store.StakeEmpty(t.Context())

			after, exists := w.rawPointer(t)
			unchanged := exists == existed && bytes.Equal(before, after)
			if !errors.Is(err, processing.ErrCheckpointRefused) || !unchanged {
				t.Fatalf("StakeEmpty: err = %v, key %t -> %t; want %v and nothing written", err, existed, exists,
					processing.ErrCheckpointRefused)
			}
		})
	}
}

func TestStakeNeedsACheckpointLatestCouldOpen(t *testing.T) {
	w := startWorld(t)
	ref := w.putShaped(t, &storedObject{epoch: olderEpoch, position: restoredAt, headerPosition: restoredAt + 1},
		imageOf(9))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref})
	if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("Latest: %v, want %v", err, processing.ErrCheckpointUnusable)
	}

	_, err := w.store.Stake(t.Context(), &processing.CheckpointMeta{})

	if got, _ := w.pointer(t); !errors.Is(err, processing.ErrCheckpointRefused) || got.Epoch != olderEpoch {
		t.Fatalf("Stake: err = %v, epoch %d; want %v and the pointer kept", err, got.Epoch,
			processing.ErrCheckpointRefused)
	}
}

func TestDiscardNeedsThePointerAndTheOperatorToAgree(t *testing.T) {
	tests := []struct {
		name    string
		target  func(ref *objectRef) processing.DiscardTarget
		arrange func(t *testing.T, w *world)
		wantErr error
	}{
		{name: "the pointer and the operator agree", target: func(ref *objectRef) processing.DiscardTarget {
			return processing.DiscardTarget{Partition: testPartition, Checkpoint: ref.NUID}
		}},
		{name: "an old checkpoint left in the environment", target: func(*objectRef) processing.DiscardTarget {
			return processing.DiscardTarget{Partition: testPartition, Checkpoint: "OLD"}
		}, wantErr: processing.ErrDiscardRefused},
		{name: "another partition", target: func(ref *objectRef) processing.DiscardTarget {
			return processing.DiscardTarget{Partition: testPartition + 1, Checkpoint: ref.NUID}
		}, wantErr: processing.ErrDiscardRefused},
		{name: "the pointer moved after Latest is retried, not refused", target: func(ref *objectRef) processing.DiscardTarget {
			return processing.DiscardTarget{Partition: testPartition, Checkpoint: ref.NUID}
		}, arrange: func(t *testing.T, w *world) {
			current, _ := w.pointer(t)
			w.setPointer(t, current)
		}, wantErr: processing.ErrCheckpointMoved},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			ref, _ := w.restorable(t)
			if tt.arrange != nil {
				tt.arrange(t, w)
			}
			before, _ := w.pointer(t)

			_, err := w.store.Discard(t.Context(), tt.target(ref))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Discard: err = %v, want %v", err, tt.wantErr)
			}
			want := before
			if tt.wantErr == nil {
				want = &pointerValue{Version: pointerVersion, Epoch: testEpoch, Previous: ref,
					DiscardedAt: w.journal(t).CachedInfo().State.FirstSeq}
			}
			if got, _ := w.pointer(t); !cmp.Equal(want, got) {
				t.Errorf("pointer (-want +got):\n%s", cmp.Diff(want, got))
			}
		})
	}
}

func TestDiscardOfAMissingPointerNeedsTheIncidentFirstSequence(t *testing.T) {
	tests := []struct {
		name     string
		firstSeq uint64
		wantErr  error
	}{
		{name: "the incident's first sequence", firstSeq: trimmedAt},
		{name: "an older incident's first sequence", firstSeq: 40, wantErr: processing.ErrDiscardRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			w.trim(t)
			if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrCheckpointUnusable) {
				t.Fatalf("Latest: %v, want %v", err, processing.ErrCheckpointUnusable)
			}
			w.putObject(t, olderEpoch, 3, imageOf(9))
			subjects := w.subjects(t)

			_, err := w.store.Discard(t.Context(), processing.DiscardTarget{Partition: testPartition, FirstSeq: tt.firstSeq})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Discard: err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(subjects, w.subjects(t)); diff != "" {
				t.Errorf("a discard without a pointer key swept the bucket (-before +after):\n%s", diff)
			}
			if tt.wantErr != nil {
				return
			}
			want := &pointerValue{Version: pointerVersion, Epoch: testEpoch, DiscardedAt: trimmedAt}
			if got, _ := w.pointer(t); !cmp.Equal(want, got) {
				t.Errorf("pointer (-want +got):\n%s", cmp.Diff(want, got))
			}
		})
	}
}

func TestDiscardOfAnUnparsedPointerMarksTheIncidentAndKeepsTheBucket(t *testing.T) {
	tests := []struct {
		name     string
		firstSeq uint64
		wantErr  error
	}{
		{name: "the incident's first sequence", firstSeq: trimmedAt},
		{name: "no first sequence", firstSeq: 0, wantErr: processing.ErrDiscardRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			w.trim(t)
			w.putObject(t, olderEpoch, 3, imageOf(9))
			w.setRawPointer(t, unparsedPointer)
			if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrCheckpointUnusable) {
				t.Fatalf("Latest: %v, want %v", err, processing.ErrCheckpointUnusable)
			}
			subjects := w.subjects(t)

			_, err := w.store.Discard(t.Context(), processing.DiscardTarget{Partition: testPartition, FirstSeq: tt.firstSeq})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Discard: err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(subjects, w.subjects(t)); diff != "" {
				t.Errorf("a discard over an unparsed pointer swept the bucket (-before +after):\n%s", diff)
			}
			if tt.wantErr != nil {
				return
			}
			want := &pointerValue{Version: pointerVersion, Epoch: testEpoch, DiscardedAt: trimmedAt}
			if got, _ := w.pointer(t); !cmp.Equal(want, got) {
				t.Errorf("pointer (-want +got):\n%s", cmp.Diff(want, got))
			}
		})
	}
}

func TestDiscardBeforeLatestIsRefused(t *testing.T) {
	w := startWorld(t)

	_, err := w.store.Discard(t.Context(), processing.DiscardTarget{Partition: testPartition})

	if _, exists := w.rawPointer(t); !errors.Is(err, processing.ErrCheckpointRefused) || exists {
		t.Fatalf("Discard: err = %v, key written %t; want %v and no key", err, exists, processing.ErrCheckpointRefused)
	}
}

const (
	canceledCalls  = 50
	queuedDeadline = 200 * time.Millisecond
)

func TestACanceledCallLeavesWithoutAskingNATS(t *testing.T) {
	latest := func(ctx context.Context, w *world) error {
		_, _, err := w.store.Latest(ctx, testPartition)
		return err
	}
	discard := func(ctx context.Context, w *world) error {
		_, err := w.store.Discard(ctx, processing.DiscardTarget{Partition: testPartition})
		return err
	}
	tests := []struct {
		name string
		busy bool
		call func(ctx context.Context, w *world) error
	}{
		{name: "Latest waiting for another Latest", busy: true, call: latest},
		{name: "Discard waiting for a Latest", busy: true, call: discard},
		{name: "Latest with nobody ahead", call: latest},
		{name: "Discard with nobody ahead", call: discard},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			if tt.busy {
				occupy(t, w.store)
			}
			requests := w.count(isRequest)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			for range canceledCalls {
				if err := tt.call(ctx, w); !errors.Is(err, context.Canceled) || requests.Load() != 0 {
					t.Fatalf("err = %v after %d requests, want %v and none", err, requests.Load(), context.Canceled)
				}
			}
		})
	}
}

func occupy(t *testing.T, store *Store) {
	t.Helper()
	if err := store.acquire(t.Context()); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(store.release)
}

func TestALatestWhoseDeadlinePassesInTheQueueLeaves(t *testing.T) {
	w := startWorld(t)
	occupy(t, w.store)
	requests := w.count(isRequest)
	ctx, cancel := context.WithTimeout(t.Context(), queuedDeadline)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := w.store.Latest(ctx, testPartition)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) || requests.Load() != 0 {
			t.Fatalf("err = %v after %d requests, want %v and none", err, requests.Load(), context.DeadlineExceeded)
		}
	case <-time.After(refusedWithin):
		t.Fatalf("Latest still waits in the queue %s past its deadline", refusedWithin)
	}
}
