package checkpoint

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const failedLatestWithin = 3 * time.Second

func (w *world) once(match func(f *frame) bool, then verdict) *atomic.Bool {
	var fired atomic.Bool
	w.proxy.intercept(func(f *frame) verdict {
		if match(f) && fired.CompareAndSwap(false, true) {
			return then
		}
		return forward
	})
	return &fired
}

func TestAStakeWhoseAnswerWasLostCountsAsDoneWhenThePointerIsOurs(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	w.once(isPointerWrite, loseReply)

	_, err := w.store.Stake(t.Context(), &meta)

	if got, _ := w.pointer(t); err != nil || got.Epoch != testEpoch || w.proxy.answersAltered() != 1 {
		t.Fatalf("Stake: err = %v, pointer epoch %d, %d answers lost; want success at %d after one lost answer", err,
			got.Epoch, w.proxy.answersAltered(), testEpoch)
	}
}

func TestAStakeThatNeverLandedIsAMovedPointer(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	w.once(isPointerWrite, drop)

	_, err := w.store.Stake(t.Context(), &meta)

	if got, _ := w.pointer(t); !errors.Is(err, processing.ErrCheckpointMoved) || got.Epoch != olderEpoch {
		t.Fatalf("Stake: err = %v, pointer epoch %d; want %v and the old epoch", err, got.Epoch,
			processing.ErrCheckpointMoved)
	}
}

func (w *world) purgeChunks(t *testing.T, nuid string) {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), objStreamPrefix+w.options.objectBucket())
	if err != nil {
		t.Fatalf("object stream: %v", err)
	}
	if err := stream.Purge(t.Context(), jetstream.WithPurgeSubject(w.store.chunkSubject(nuid))); err != nil {
		t.Fatalf("purge: %v", err)
	}
}

func readAfterFirstChunk(t *testing.T, w *world, between func()) error {
	t.Helper()
	_, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	if _, err := reader.Read(make([]byte, testChunk)); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	between()
	_, err = new(bytes.Buffer).ReadFrom(reader)
	return err
}

func TestAChunkGoneWhileThePointerStillNamesTheObjectIsCorruption(t *testing.T) {
	w := startWorld(t)
	ref, _ := w.restorable(t)

	err := readAfterFirstChunk(t, w, func() { w.purgeChunks(t, ref.NUID) })

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("read: err = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
}

func TestAChunkGoneBecauseThePointerMovedOnIsAChangedObjectNotCorruption(t *testing.T) {
	w := startWorld(t)
	ref, _ := w.restorable(t)

	err := readAfterFirstChunk(t, w, func() {
		next := w.putObject(t, testEpoch, restoredAt+1, imageOf(9))
		w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: next})
		w.purgeChunks(t, ref.NUID)
	})

	if !errors.Is(err, processing.ErrCheckpointMoved) || errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("read: err = %v, want %v and not corruption", err, processing.ErrCheckpointMoved)
	}
}

func TestLatestFollowsAPointerAZombieMovedDuringTheLookup(t *testing.T) {
	w := startWorld(t)
	first := w.putObject(t, olderEpoch, 5, imageOf(9))
	moved := imageOf(imageBytes)
	next := w.putObject(t, olderEpoch, restoredAt, moved)
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: first})
	var zombie atomic.Bool
	w.proxy.intercept(func(f *frame) verdict {
		if isInfoLookup(f) && zombie.CompareAndSwap(false, true) {
			w.putPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: next})
			if err := w.objects.Delete(context.WithoutCancel(t.Context()), first.Name); err != nil {
				t.Errorf("delete: %v", err)
			}
		}
		return forward
	})

	meta, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil || meta.Position.Sequence != restoredAt {
		t.Fatalf("Latest: err = %v, position %d; want the moved checkpoint at %d", err, meta.Position.Sequence,
			restoredAt)
	}
	got, err := readAllOf(reader)
	if diff := cmp.Diff(moved, got); err != nil || diff != "" {
		t.Errorf("image of the moved checkpoint: err = %v (-want +got):\n%s", err, diff)
	}
}

func readAllOf(reader interface {
	Read(p []byte) (int, error)
	Close() error
},
) ([]byte, error) {
	var buffer bytes.Buffer
	_, err := buffer.ReadFrom(reader)
	return buffer.Bytes(), errors.Join(err, reader.Close())
}

func TestAPointerThatKeepsMovingDuringTheLookupIsAMovedPointerNotCorruption(t *testing.T) {
	w := startWorld(t)
	ref := w.putObject(t, testEpoch, 5, imageOf(9))
	ref.Digest = digestPrefix + base64.URLEncoding.EncodeToString(make([]byte, digestBytes))
	value := &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref}
	w.setPointer(t, value)
	var lookups atomic.Int32
	w.proxy.intercept(func(f *frame) verdict {
		if isInfoLookup(f) {
			lookups.Add(1)
			w.putPointer(t, value)
		}
		return forward
	})

	_, _, err := w.store.Latest(t.Context(), testPartition)

	if !errors.Is(err, processing.ErrCheckpointMoved) || errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("Latest: err = %v after %d lookups, want %v and not corruption", err, lookups.Load(),
			processing.ErrCheckpointMoved)
	}
}

func TestAnObjectThatNeverMatchesItsPointerIsCorruptAfterThreeReads(t *testing.T) {
	w := startWorld(t)
	ref := w.putObject(t, testEpoch, 5, imageOf(9))
	ref.Digest = digestPrefix + base64.URLEncoding.EncodeToString(make([]byte, digestBytes))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
	var lookups atomic.Int32
	w.proxy.intercept(func(f *frame) verdict {
		if isInfoLookup(f) {
			lookups.Add(1)
		}
		return forward
	})

	_, _, err := w.store.Latest(t.Context(), testPartition)

	if !errors.Is(err, processing.ErrCheckpointUnusable) || lookups.Load() != pointerRereads {
		t.Fatalf("Latest: err = %v after %d lookups, want %v after %d", err, lookups.Load(),
			processing.ErrCheckpointUnusable, pointerRereads)
	}
}

func TestALeaseLostWhileThePointerIsReadStopsTheWrite(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	_, before := w.pointer(t)
	w.store.lease = &validTimes{left: 1}
	var writes atomic.Int32
	w.proxy.intercept(func(f *frame) verdict {
		if isPointerWrite(f) {
			writes.Add(1)
		}
		return forward
	})

	_, err := w.store.Stake(t.Context(), &meta)

	if _, after := w.pointer(t); !errors.Is(err, processing.ErrLeaseLost) || after != before || writes.Load() != 0 {
		t.Fatalf("Stake: err = %v, revision %d→%d, %d writes; want %v and no write", err, before, after,
			writes.Load(), processing.ErrLeaseLost)
	}
}

func TestOneLostObjectLookupIsAskedAgainWithinTheStartBudget(t *testing.T) {
	w := startWorld(t)
	w.putObject(t, olderEpoch, restoredAt, imageOf(imageBytes))
	ref := w.putObject(t, olderEpoch, restoredAt+1, imageOf(imageBytes))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref})
	dropped := w.once(isInfoLookup, drop)
	ctx, cancel := context.WithTimeout(t.Context(), startCallBudget)
	defer cancel()

	meta, reader, err := w.store.Latest(ctx, testPartition)
	if err != nil || meta.Position.Sequence != restoredAt+1 || !dropped.Load() {
		t.Fatalf("Latest: err = %v, position %d, lookup dropped %t; want success at %d after one lost lookup", err,
			meta.Position.Sequence, dropped.Load(), restoredAt+1)
	}
	if _, err := readAllOf(reader); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestDiscardAfterAPassingFailureOfLatestIsRefused(t *testing.T) {
	w := startWorld(t)
	ref := w.putObject(t, olderEpoch, restoredAt, imageOf(9))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref})
	_, before := w.pointer(t)
	w.proxy.intercept(func(f *frame) verdict {
		if isInfoLookup(f) {
			return drop
		}
		return forward
	})
	ctx, cancel := context.WithTimeout(t.Context(), failedLatestWithin)
	defer cancel()
	if _, _, err := w.store.Latest(ctx, testPartition); !errors.Is(err, processing.ErrCheckpointUnavailable) {
		t.Fatalf("Latest: %v, want %v", err, processing.ErrCheckpointUnavailable)
	}
	w.proxy.intercept(nil)

	_, err := w.store.Discard(t.Context(), processing.DiscardTarget{Partition: testPartition, Checkpoint: ref.NUID})

	if _, after := w.pointer(t); !errors.Is(err, processing.ErrCheckpointRefused) || after != before {
		t.Fatalf("Discard: err = %v, revision %d→%d; want %v and no write", err, before, after,
			processing.ErrCheckpointRefused)
	}
}

func (w *world) subjects(t *testing.T) []string {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), objStreamPrefix+w.options.objectBucket())
	if err != nil {
		t.Fatalf("object stream: %v", err)
	}
	info, err := stream.Info(t.Context(), jetstream.WithSubjectFilter(w.store.subjectPrefix()+subjectFilterAll))
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	return slices.Sorted(maps.Keys(info.State.Subjects))
}

func (w *world) metaSubject(name string) string {
	return w.store.subjectPrefix() + metaSubjectPart + base64.URLEncoding.EncodeToString([]byte(name))
}

func TestStakingSweepsOnlyWhatNoHolderCanStillNeed(t *testing.T) {
	tests := []struct {
		name        string
		later       time.Duration
		orphanKept  bool
		sweptOrphan uint64
	}{
		{name: "exactly the orphan age is an upload in flight", orphanKept: true},
		{name: "a nanosecond older is an orphan", later: time.Nanosecond, sweptOrphan: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			stale := w.putObject(t, olderEpoch, 20, imageOf(9))
			later := w.putObject(t, testEpoch+1, 50, imageOf(9))
			ref, meta := w.restorable(t)
			orphan := w.store.chunkSubject("ORPHAN")
			if _, err := w.operator.Publish(t.Context(), orphan, imageOf(9)); err != nil {
				t.Fatalf("publish orphan: %v", err)
			}
			published := w.lastTime(t, orphan)
			w.store.now = func() time.Time { return published.Add(w.options.orphanAge() + tt.later) }

			if _, err := w.store.Stake(t.Context(), &meta); err != nil {
				t.Fatalf("Stake: %v", err)
			}

			want := []string{w.store.chunkSubject(ref.NUID), w.metaSubject(ref.Name),
				w.store.chunkSubject(later.NUID), w.metaSubject(later.Name)}
			if tt.orphanKept {
				want = append(want, orphan)
			}
			slices.Sort(want)
			if diff := cmp.Diff(want, w.subjects(t)); diff != "" {
				t.Errorf("subjects after the sweep, %s swept (-want +got):\n%s", stale.Name, diff)
			}
			wantStats := Stats{Epoch: testEpoch, SweptObjects: 1, SweptOrphans: tt.sweptOrphan}
			if diff := cmp.Diff(wantStats, w.store.Stats()); diff != "" {
				t.Errorf("stats (-want +got):\n%s", diff)
			}
		})
	}
}

func (w *world) lastTime(t *testing.T, subject string) time.Time {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), objStreamPrefix+w.options.objectBucket())
	if err != nil {
		t.Fatalf("object stream: %v", err)
	}
	last, err := stream.GetLastMsgForSubject(t.Context(), subject)
	if err != nil {
		t.Fatalf("last of %s: %v", subject, err)
	}
	return last.Time
}

func TestLatestFollowsAPointerThatMovedOnAllButTheLastRead(t *testing.T) {
	w := startWorld(t)
	positions := []uint64{5, 6, restoredAt}
	refs := make([]*objectRef, 0, len(positions))
	for _, position := range positions {
		refs = append(refs, w.putObject(t, olderEpoch, position, imageOf(9)))
	}
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: refs[0]})
	var lookups atomic.Int32
	w.proxy.intercept(func(f *frame) verdict {
		if !isInfoLookup(f) {
			return forward
		}
		moved := int(lookups.Add(1))
		if moved >= len(refs) {
			return forward
		}
		w.putPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: refs[moved]})
		if err := w.objects.Delete(context.WithoutCancel(t.Context()), refs[moved-1].Name); err != nil {
			t.Errorf("delete: %v", err)
		}
		return forward
	})

	meta, reader, err := w.store.Latest(t.Context(), testPartition)

	if err != nil || meta.Position.Sequence != restoredAt {
		t.Fatalf("Latest: err = %v, position %d after %d lookups; want the checkpoint at %d on the last read", err,
			meta.Position.Sequence, lookups.Load(), restoredAt)
	}
	if _, err := readAllOf(reader); err != nil {
		t.Fatalf("read: %v", err)
	}
}
