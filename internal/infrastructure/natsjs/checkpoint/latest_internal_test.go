package checkpoint

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const refusedWithin = 3 * time.Second

func TestLatestRestoresTheCheckpointThePointerNames(t *testing.T) {
	w := startWorld(t)
	image := imageOf(imageBytes)
	ref := w.putObject(t, olderEpoch, restoredAt, image)
	created := w.streamCreated(t)
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
		StreamCreated: created.Format(createdLayout), Current: ref})

	meta, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	got, err := readAllOf(reader)

	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := processing.CheckpointMeta{
		Position:          processing.Position{Partition: testPartition, Sequence: restoredAt},
		ReceivedAt:        testStart,
		StreamCreated:     created,
		Epoch:             olderEpoch,
		CheckpointVersion: 1,
		RecordVersion:     1,
		LimitsHash:        11,
		RulesHash:         12,
	}
	if diff := cmp.Diff(want, meta, cmpTimes()); diff != "" {
		t.Errorf("meta (-want +got):\n%s", diff)
	}
	if !bytes.Equal(image, got) {
		t.Errorf("image of %d bytes, want the %d written", len(got), len(image))
	}
}

func cmpTimes() cmp.Option {
	return cmp.Comparer(func(a, b time.Time) bool { return a.Equal(b) })
}

func TestLatestRefusesWhatItCannotTrust(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, w *world)
		wantErr error
		notErr  error
	}{
		{name: "a pointer that does not parse is corrupt", arrange: func(t *testing.T, w *world) {
			w.setRawPointer(t, unparsedPointer)
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "a pointer of an unknown version is corrupt", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion + 1, Epoch: olderEpoch})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "a later epoch in the pointer refuses an older holder", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch + 1})
		}, wantErr: processing.ErrCheckpointRefused},
		{name: "a pointer of another stream is a recreated stream", arrange: func(t *testing.T, w *world) {
			ref := w.putObject(t, olderEpoch, restoredAt, imageOf(9))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref,
				StreamCreated: w.streamCreated(t).Add(-time.Hour).Format(createdLayout)})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an empty pointer without a stream is a fresh start", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch})
			w.dropJournal(t)
		}, wantErr: processing.ErrNoCheckpoint},
		{name: "an empty pointer of a stream that is gone is a recreated stream", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
				StreamCreated: w.streamCreated(t).Format(createdLayout)})
			w.dropJournal(t)
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an empty pointer over a trimmed stream without a discard mark is lost state",
			arrange: func(t *testing.T, w *world) {
				w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch})
				w.trim(t)
			}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an empty pointer discarded at the stream start is a fresh start", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, DiscardedAt: trimmedAt})
			w.trim(t)
		}, wantErr: processing.ErrNoCheckpoint},
		{name: "an empty pointer trimmed past its discard mark is lost state", arrange: func(t *testing.T, w *world) {
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, DiscardedAt: trimmedAt - 1})
			w.trim(t)
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an object whose meta disagrees with the pointer is corrupt", arrange: func(t *testing.T, w *world) {
			ref := w.putObject(t, testEpoch, 5, imageOf(9))
			ref.NUID = "ANOTHER"
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an object whose header says another position is corrupt", arrange: func(t *testing.T, w *world) {
			ref := w.putShaped(t, &storedObject{epoch: testEpoch, position: 5, headerPosition: 6}, imageOf(9))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an object whose header says another epoch is corrupt", arrange: func(t *testing.T, w *world) {
			ref := w.putShaped(t, &storedObject{epoch: testEpoch, position: 5, headerEpoch: testEpoch - 1}, imageOf(9))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an object whose meta names another object is corrupt", arrange: func(t *testing.T, w *world) {
			ref := w.putObject(t, testEpoch, 5, imageOf(9))
			w.rewriteMeta(t, ref.Name, func(info *jetstream.ObjectInfo) { info.Name = "7-6" })
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "an object whose meta says another size is corrupt", arrange: func(t *testing.T, w *world) {
			ref := w.putObject(t, testEpoch, 5, imageOf(9))
			w.rewriteMeta(t, ref.Name, func(info *jetstream.ObjectInfo) { info.Size++ })
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "a bucket that is gone is lost state", arrange: func(t *testing.T, w *world) {
			ref := w.putObject(t, testEpoch, 5, imageOf(9))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})
			if err := w.operator.DeleteObjectStore(t.Context(), w.options.objectBucket()); err != nil {
				t.Fatalf("delete bucket: %v", err)
			}
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "a pointer read that keeps timing out is a timeout, not corruption", arrange: func(_ *testing.T, w *world) {
			w.dropAll(isPointerRead)
		}, wantErr: context.DeadlineExceeded, notErr: processing.ErrCheckpointUnusable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			tt.arrange(t, w)
			ctx, cancel := context.WithTimeout(t.Context(), refusedWithin)
			defer cancel()

			_, reader, err := w.store.Latest(ctx, testPartition)
			closeOnCleanup(t, reader)

			if !errors.Is(err, tt.wantErr) || reader != nil {
				t.Fatalf("Latest: err = %v, reader %v; want %v and no reader", err, reader, tt.wantErr)
			}
			if tt.notErr != nil && errors.Is(err, tt.notErr) {
				t.Fatalf("Latest: err = %v, must not be %v", err, tt.notErr)
			}
		})
	}
}

func TestLatestOfAnotherPartitionIsRefusedWithoutAskingNATS(t *testing.T) {
	w := startWorld(t)
	requests := w.count(isRequest)

	err := latestErr(w.store.Latest(t.Context(), testPartition+1))

	if !errors.Is(err, processing.ErrForeignCheckpoint) || requests.Load() != 0 {
		t.Fatalf("err = %v after %d requests, want %v and none", err, requests.Load(), processing.ErrForeignCheckpoint)
	}
	if err := latestErr(w.store.Latest(t.Context(), testPartition)); !errors.Is(err, processing.ErrNoCheckpoint) ||
		requests.Load() == 0 {
		t.Fatalf("own Latest: err = %v after %d requests; the request counter sees nothing", err, requests.Load())
	}
}

func TestLatestOfAClosedStoreIsRefused(t *testing.T) {
	w := startWorld(t)
	w.store.Close(t.Context())

	err := latestErr(w.store.Latest(t.Context(), testPartition))

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Fatalf("err = %v, want %v", err, processing.ErrCheckpointRefused)
	}
}

func TestACanceledRestoreStaysCanceled(t *testing.T) {
	w := startWorld(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := latestErr(w.store.Latest(ctx, testPartition))

	if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want only %v", err, context.Canceled)
	}
}

func TestLatestTellsTheStreamIdentityOfThePointerNotOfTheObject(t *testing.T) {
	w := startWorld(t)
	created := w.streamCreated(t)
	ref := w.putObject(t, olderEpoch, 5, imageOf(9))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
		StreamCreated: created.Format(createdLayout), Current: ref})

	meta, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if _, err := readAllOf(reader); err != nil || !meta.StreamCreated.Equal(created) {
		t.Fatalf("read: err = %v, stream created %s; want the pointer's %s, not the header's", err,
			meta.StreamCreated, created)
	}
}

func TestTheReaderGivesTheImageOnlyWhenEveryCheckHolds(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, w *world, ref *objectRef)
		wantErr error
	}{
		{name: "a chunk lost to an election is fetched again", arrange: func(t *testing.T, w *world, _ *objectRef) {
			expectFired(t, w.once(isChunkRead, natsjstest.Drop()))
		}},
		{name: "a chunk from another subject is corruption", arrange: func(t *testing.T, w *world, _ *objectRef) {
			expectFired(t, w.once(isChunkRead, natsjstest.AlterReply("subject")))
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "a chunk past the declared count is corruption", arrange: func(t *testing.T, w *world, ref *objectRef) {
			if _, err := w.operator.Publish(t.Context(), w.store.chunkSubject(ref.NUID), imageOf(9)); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}, wantErr: processing.ErrCheckpointUnusable},
		{name: "chunks with other bytes fail the digest", arrange: func(t *testing.T, w *world, ref *objectRef) {
			w.purgeChunks(t, ref.NUID)
			flipped := imageOf(imageBytes)
			flipped[testChunk] ^= 0xff
			for start := 0; start < len(flipped); start += testChunk {
				end := min(start+testChunk, len(flipped))
				if _, err := w.operator.Publish(t.Context(), w.store.chunkSubject(ref.NUID), flipped[start:end]); err != nil {
					t.Fatalf("publish chunk: %v", err)
				}
			}
		}, wantErr: processing.ErrCheckpointUnusable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			ref, _ := w.restorable(t)
			tt.arrange(t, w, ref)

			_, reader, err := w.store.Latest(t.Context(), testPartition)
			if err != nil {
				t.Fatalf("Latest: %v", err)
			}
			got, err := readAllOf(reader)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("read: err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && !bytes.Equal(got, imageOf(imageBytes)) {
				t.Errorf("image of %d bytes, want the %d written", len(got), imageBytes)
			}
		})
	}
}

func expectFired(t *testing.T, fired *atomic.Bool) {
	t.Cleanup(func() {
		if !fired.Load() {
			t.Error("the proxy never saw the frame the test meant to alter")
		}
	})
}

func TestClosingTheReaderStopsAReadWaitingOnAnElection(t *testing.T) {
	w := startWorld(t)
	w.restorable(t)
	_, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	waiting := make(chan struct{})
	var signal sync.Once
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !isChunkRead(f) {
			return natsjstest.Forward()
		}
		signal.Do(func() { close(waiting) })
		return natsjstest.Drop()
	})
	done := make(chan error, 1)
	go func() {
		_, err := new(bytes.Buffer).ReadFrom(reader)
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(refusedWithin):
		t.Fatal("no chunk read reached the proxy")
	}

	if err := reader.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read after Close: err = %v, want %v", err, context.Canceled)
		}
	case <-time.After(refusedWithin):
		t.Fatalf("read still waits %s after Close", refusedWithin)
	}
}

func TestAPointerDeletedWhileTheObjectIsReadIsAChangedObject(t *testing.T) {
	w := startWorld(t)
	ref, _ := w.restorable(t)

	err := readAfterFirstChunk(t, w, func() {
		if err := w.kv.Delete(t.Context(), w.options.pointerKey()); err != nil {
			t.Fatalf("delete pointer: %v", err)
		}
		w.purgeChunks(t, ref.NUID)
	})

	if !errors.Is(err, processing.ErrCheckpointMoved) {
		t.Fatalf("read: err = %v, want %v", err, processing.ErrCheckpointMoved)
	}
}

func TestAFailedTailCheckIsNotTheEndOfTheObject(t *testing.T) {
	w := startWorld(t)
	w.restorable(t)
	chunks := int32((imageBytes + testChunk - 1) / testChunk)
	var reads atomic.Int32
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !isChunkRead(f) || reads.Add(1) <= chunks {
			return natsjstest.Forward()
		}
		return natsjstest.Drop()
	})
	ctx, cancel := context.WithTimeout(t.Context(), refusedWithin)
	defer cancel()
	_, reader, err := w.store.Latest(ctx, testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}

	_, err = readAllOf(reader)

	if !errors.Is(err, processing.ErrCheckpointUnavailable) || errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("read: err = %v, want %v and not corruption", err, processing.ErrCheckpointUnavailable)
	}
	if reads.Load() <= chunks {
		t.Fatalf("%d chunk reads, want the tail check past the %d chunks", reads.Load(), chunks)
	}
}

func TestAnObjectShorterThanItsMetaSaysIsCorruption(t *testing.T) {
	w := startWorld(t)
	ref := w.putObject(t, testEpoch, 5, imageOf(imageBytes))
	w.rewriteMeta(t, ref.Name, func(info *jetstream.ObjectInfo) { info.Size++ })
	ref.Size++
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: ref})

	_, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	_, err = readAllOf(reader)

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("read: err = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
}

func TestAReadWithoutACallerDeadlineGetsItsOwn(t *testing.T) {
	const size = 450 << 20
	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
		want time.Duration
	}{
		{
			name: "no deadline: size at 16 MiB/s plus 30 s",
			ctx:  func(t *testing.T) context.Context { return context.WithoutCancel(t.Context()) },
			want: 28125*time.Millisecond + readSlack,
		},
		{
			name: "the caller's deadline stays",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				t.Cleanup(cancel)
				return ctx
			},
			want: time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			ctx, cancel := readContext(tt.ctx(t), size)
			defer cancel()

			deadline, bounded := ctx.Deadline()

			if drift := deadline.Sub(start) - tt.want; !bounded || drift.Abs() > time.Second {
				t.Fatalf("deadline in %s (bounded %t), want about %s", deadline.Sub(start), bounded, tt.want)
			}
		})
	}
}

func corruptDigest() string {
	return digestPrefix + base64.URLEncoding.EncodeToString(make([]byte, digestBytes))
}
