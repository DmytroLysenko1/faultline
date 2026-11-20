package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
	framed "github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	msgIDHeader        = "Nats-Msg-Id: "
	pacedRate          = 4 << 10
	storeFullAnswer    = `{"error":{"code":503,"err_code":10077,"description":"maximum bytes exceeded"}}`
	inProcessAnswer    = `{"error":{"code":409,"err_code":10158,"description":"duplicate message id is in process"}}`
	chunksBeforeLoss   = 2
	storeFailedCode    = 10077
	staleAckChunk      = 2
	stopPromptly       = time.Second
	threeNodes         = 3
	bucketPurge        = "$JS.API.STREAM.PURGE." + objStreamPrefix + "faultline_checkpoints_p0"
	changedChunk       = 3
	staleAck           = `{"stream":"OBJ_faultline_checkpoints_p0","seq":1}`
	replicatedConflict = `{"error":{"code":400,"err_code":10164,"description":"wrong last sequence"}}`
)

var clusterSizes = []int{testReplicas, threeNodes}

type pointerShape struct {
	Epoch    uint64
	Current  uint64
	Previous uint64
}

func shapeOfPointer(value *pointerValue) pointerShape {
	shape := pointerShape{Epoch: value.Epoch}
	if value.Current != nil {
		shape.Current = value.Current.Position
	}
	if value.Previous != nil {
		shape.Previous = value.Previous.Position
	}
	return shape
}

type gate struct {
	held   atomic.Bool
	open   chan struct{}
	opened sync.Once
}

func newGate(t *testing.T) *gate {
	g := &gate{open: make(chan struct{})}
	t.Cleanup(g.release)
	return g
}

func (g *gate) release() {
	g.opened.Do(func() { close(g.open) })
}

func (w *world) holdFirst(g *gate, match func(f *natsjstest.Frame) bool) {
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if match(f) && g.held.CompareAndSwap(false, true) {
			<-g.open
		}
		return natsjstest.Forward()
	})
}

func (g *gate) waitHeld(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(settleWithin)
	for !g.held.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("no frame was held within %s", settleWithin)
		}
		time.Sleep(settlePoll)
	}
}

func TestCheckpointsAreUploadedVerifiedCommittedAndRotated(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	image := checkpointImage(t)

	for _, position := range []uint64{firstPosition, secondPosition, thirdPosition} {
		if err := spoolCheckpoint(t, sink, position, image); err != nil {
			t.Fatalf("spool %d: %v", position, err)
		}
		w.settled(t, committedThrough(position))
	}

	value, _ := w.pointer(t)
	want := pointerShape{Epoch: testEpoch, Current: thirdPosition, Previous: secondPosition}
	if diff := cmp.Diff(want, shapeOfPointer(value)); diff != "" {
		t.Errorf("pointer mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]uint64{firstPosition, secondPosition, thirdPosition}, retention.committed()); diff != "" {
		t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 0)
	w.assertLatest(t, image, retention.created)
}

func (w *world) assertLatest(t *testing.T, image []byte, created time.Time) {
	t.Helper()
	next := newStore(w.options, w.lease, &w.store.buckets, time.Now)
	meta, reader, err := next.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	read, err := io.ReadAll(reader)
	if err = errors.Join(err, reader.Close()); err != nil {
		t.Fatalf("read the latest: %v", err)
	}
	want := processing.CheckpointMeta{
		Position:          processing.Position{Partition: testPartition, Sequence: thirdPosition},
		ReceivedAt:        testStart,
		StreamCreated:     created.UTC(),
		Epoch:             testEpoch,
		CheckpointVersion: topology.CheckpointVersion,
		RecordVersion:     record.Version,
	}
	if diff := cmp.Diff(want, meta); diff != "" {
		t.Errorf("meta mismatch (-want +got):\n%s", diff)
	}
	if !bytes.Equal(read, image) {
		t.Errorf("the latest image is %d bytes, not the %d spooled", len(read), len(image))
	}
	value, _ := w.pointer(t)
	headers := w.objectInfo(t, value.Current.Name).Headers
	if got := headers.Get(headerStreamCreated); got != strconv.FormatInt(created.UnixNano(), decimal) {
		t.Errorf("object header %s = %q, want the stream's creation %d", headerStreamCreated, got, created.UnixNano())
	}
}

func TestACheckpointWhileOneUploadsIsSkippedAsBusy(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	image := checkpointImage(t)
	held := newGate(t)
	w.holdFirst(held, isChunk)
	if err := spoolCheckpoint(t, sink, firstPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)

	err := spoolCheckpoint(t, sink, secondPosition, image)
	held.release()

	if !errors.Is(err, processing.ErrCheckpointBusy) {
		t.Fatalf("second spool = %v, want ErrCheckpointBusy", err)
	}
	stats := w.settled(t, committedThrough(firstPosition))
	if stats.Skipped["busy"] != 1 {
		t.Errorf("busy skips = %d, want 1", stats.Skipped["busy"])
	}
	w.assertInvariants(t, retention, 0)
}

func TestALeaseLostBetweenChunksTakesTheUploadBack(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var mu sync.Mutex
	sent := map[string]bool{}
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunk(f) {
			mu.Lock()
			sent[msgIDOf(f.Payload)] = true
			if len(sent) == chunksBeforeLoss {
				w.lease.set(false)
			}
			mu.Unlock()
		}
		return natsjstest.Forward()
	})

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	w.settled(t, skippedFor("lease_suspended"))
	if objects := w.ownObjects(t); len(objects) != 0 {
		t.Errorf("objects left after a suspended upload: %v", objects)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != chunksBeforeLoss {
		t.Errorf("%d chunks sent, want none after the lease was lost at chunk %d", len(sent), chunksBeforeLoss)
	}
	w.assertInvariants(t, retention, 0)
}

func msgIDOf(payload []byte) string {
	_, after, found := bytes.Cut(payload, []byte(msgIDHeader))
	if !found {
		return ""
	}
	id, _, _ := bytes.Cut(after, []byte("\r\n"))
	return string(id)
}

type refusedUploadArgs struct {
	answer natsjstest.Verdict
}

type refusedUploadWant struct {
	Failed  uint64
	Class   string
	Objects int
}

func TestARefusedChunkFailsTheUploadAndLeavesNothing(t *testing.T) {
	tests := []struct {
		name    string
		args    refusedUploadArgs
		want    refusedUploadWant
		wantErr error
	}{
		{
			name:    "acknowledged by another stream",
			args:    refusedUploadArgs{answer: natsjstest.AlterReply("stream")},
			want:    refusedUploadWant{Failed: 1, Class: "foreign_stream"},
			wantErr: errForeignAck,
		},
		{
			name:    "a full bucket",
			args:    refusedUploadArgs{answer: natsjstest.Answer([]byte(storeFullAnswer))},
			want:    refusedUploadWant{Failed: 1, Class: "bucket_full"},
			wantErr: &jetstream.APIError{ErrorCode: storeFailedCode},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			w.once(isChunk, tt.args.answer)

			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}

			stats := w.settled(t, failedFor("upload"))
			if !errors.Is(stats.LastFailure, tt.wantErr) {
				t.Errorf("last failure = %v, want %v", stats.LastFailure, tt.wantErr)
			}
			got := refusedUploadWant{Failed: stats.Failed["upload"], Class: classOf(stats.UploadFailed),
				Objects: len(w.ownObjects(t))}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("outcome mismatch (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestAChunkStillInProcessIsRetried(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	w.once(isChunk, natsjstest.Answer([]byte(inProcessAnswer)))

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	w.settled(t, committedThrough(firstPosition))
	w.assertInvariants(t, retention, 0)
}

func TestAChunkTheUploadDidNotSendFailsTheCount(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var subject atomic.Pointer[string]
	var added atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunk(f) {
			subject.Store(&f.Subject)
		}
		if isChunkCount(f) && added.CompareAndSwap(false, true) {
			if _, err := w.operator.Publish(context.WithoutCancel(t.Context()), *subject.Load(), []byte("extra")); err != nil {
				t.Errorf("publish an extra chunk: %v", err)
			}
		}
		return natsjstest.Forward()
	})

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	stats := w.settled(t, failedFor("chunk_count"))
	if !errors.Is(stats.LastFailure, errChunkCount) || len(w.ownObjects(t)) != 0 {
		t.Errorf("last failure %v, objects %v; want a chunk count failure and nothing left", stats.LastFailure,
			w.ownObjects(t))
	}
	w.assertInvariants(t, retention, 0)
}

func TestAnObjectOfTheSameNameIsACollisionThatLeavesItAlone(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	foreign := w.putObject(t, testEpoch, firstPosition, imageOf(imageBytes))

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	stats := w.settled(t, failedFor("name_collision"))
	if !errors.Is(stats.LastFailure, errNameCollision) {
		t.Errorf("last failure = %v, want a name collision", stats.LastFailure)
	}
	if info := w.objectInfo(t, foreign.Name); info.NUID != foreign.NUID {
		t.Errorf("the object of that name is %s, want the one already there, %s", info.NUID, foreign.NUID)
	}
	if diff := cmp.Diff([]string{foreign.NUID}, w.ownObjects(t)); diff != "" {
		t.Errorf("objects mismatch (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 1)
}

func TestAMetaWhoseAnswerWasLostIsSettledByItsOwnMessage(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			sink, retention := w.started(t)
			w.once(isMeta, natsjstest.LoseReply())

			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}

			w.settled(t, committedThrough(firstPosition))
			w.assertInvariants(t, retention, 0)
		})
	}
}

type terminalArgs struct {
	pointerEpoch uint64
}

func TestAPointerOfAnotherHolderEndsTheStoreAndKeepsTheObject(t *testing.T) {
	tests := []struct {
		name    string
		args    terminalArgs
		wantErr error
	}{
		{name: "an earlier epoch", args: terminalArgs{pointerEpoch: olderEpoch}, wantErr: processing.ErrCheckpointRefused},
		{name: "a later epoch", args: terminalArgs{pointerEpoch: testEpoch + 1}, wantErr: processing.ErrLeaseLost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: tt.args.pointerEpoch})

			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}

			if err := w.ended(t); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Err() = %v, want %v", err, tt.wantErr)
			}
			if objects := w.ownObjects(t); len(objects) != 1 {
				t.Errorf("objects %v, want the uploaded one kept", objects)
			}
			if err := spoolCheckpoint(t, sink, secondPosition, checkpointImage(t)); !errors.Is(err, tt.wantErr) {
				t.Errorf("a checkpoint after the end = %v, want %v", err, tt.wantErr)
			}
			w.assertInvariants(t, retention, 1)
		})
	}
}

func TestAnUpdateWhoseAnswerWasLostIsSettledByTheReread(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			sink, retention := w.started(t)
			w.once(isPointerWrite, natsjstest.LoseReply())

			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}

			w.settled(t, committedThrough(firstPosition))
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestAnUpdateWithoutAKnownOutcomeWaitsAndIsFencedOffBeforeTheNextUpload(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			sink, retention := w.started(t)
			var chunks atomic.Int32
			w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
				if isChunk(f) {
					chunks.Add(1)
				}
				if isPointerWrite(f) {
					return natsjstest.Drop()
				}
				return natsjstest.Forward()
			})

			w.spoolAndSettle(t, sink, firstPosition, failedFor("cas"))
			waiting := w.store.Stats()
			if objects := w.ownObjects(t); len(objects) != 1 {
				t.Errorf("objects %v, want the uploaded one kept", objects)
			}
			sent := chunks.Load()
			w.spoolAndSettle(t, sink, secondPosition, failedFor("bucket_unresolved"))
			if chunks.Load() != sent {
				t.Errorf("%d chunks sent while an object waits, want none", chunks.Load()-sent)
			}
			w.proxy.Intercept(nil)
			w.spoolAndSettle(t, sink, thirdPosition, committedThrough(thirdPosition))

			got := waitingCounts{Pending: waiting.Pending, Unknown: waiting.CASUnknown, Fences: waiting.Fences}
			if diff := cmp.Diff(waitingCounts{Pending: 1, Unknown: 1, Fences: 1}, got); diff != "" {
				t.Errorf("counts while waiting mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]uint64{thirdPosition}, retention.committed()); diff != "" {
				t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

type waitingCounts struct {
	Pending uint64
	Unknown uint64
	Fences  uint64
}

func (w *world) spoolAndSettle(t *testing.T, sink *Sink, position uint64, done func(stats *Stats) bool) {
	t.Helper()
	cycles := w.store.Stats().Cycles
	if err := spoolCheckpoint(t, sink, position, checkpointImage(t)); err != nil {
		t.Fatalf("spool %d: %v", position, err)
	}
	w.settled(t, func(stats *Stats) bool {
		return stats.Cycles > cycles && done(stats)
	})
}

func TestARefusedFeedCommitIsCountedAndTheNextCheckpointCommits(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	retention.refuse = 1
	image := checkpointImage(t)

	for _, position := range []uint64{firstPosition, secondPosition} {
		if err := spoolCheckpoint(t, sink, position, image); err != nil {
			t.Fatalf("spool %d: %v", position, err)
		}
		w.settled(t, committedThrough(position))
	}

	if stats := w.store.Stats(); stats.Failed["commit"] != 1 || stats.Committed != 2 {
		t.Errorf("commit failures %d, committed %d; want 1 and 2", stats.Failed["commit"], stats.Committed)
	}
	if diff := cmp.Diff([]uint64{secondPosition}, retention.committed()); diff != "" {
		t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 0)
}

func TestCloseDuringTheUploadTakesItBack(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	held := newGate(t)
	w.holdFirst(held, isChunk)
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)

	w.closeWhile(t, held)

	stats := w.store.Stats()
	if objects := w.ownObjects(t); len(objects) != 0 || stats.Committed != 0 || stats.Failed["upload"] != 0 {
		t.Errorf("objects %v, committed %d, failed uploads %d; want the upload taken back as a stop", objects,
			stats.Committed, stats.Failed["upload"])
	}
	w.assertInvariants(t, retention, 0)
}

func TestCloseDuringTheCASCompletesIt(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	held := newGate(t)
	w.holdFirst(held, isPointerWrite)
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)

	w.closeWhile(t, held)

	value, _ := w.pointer(t)
	if diff := cmp.Diff(pointerShape{Epoch: testEpoch, Current: firstPosition}, shapeOfPointer(value)); diff != "" {
		t.Errorf("pointer mismatch (-want +got):\n%s", diff)
	}
	if commits := retention.committed(); len(commits) != 0 {
		t.Errorf("feed commits %v on the way out, want none", commits)
	}
	w.assertInvariants(t, retention, 0)
}

func (w *world) closeWhile(t *testing.T, held *gate) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), stopWithin)
	defer cancel()
	w.store.Close(ctx)
	held.release()
	if err := w.store.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestAWriterFinishedAfterTheStoreStoppedIsRefusedAndLeavesNoSpool(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	writer, err := sink.Begin(t.Context(), spoolAt(firstPosition))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	w.stop(t)
	if _, err := writer.Write(checkpointImage(t)); err != nil {
		t.Fatalf("write: %v", err)
	}

	err = writer.Close()

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Errorf("Close = %v, want ErrCheckpointRefused", err)
	}
	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools left: %v", left)
	}
	if _, err := sink.Begin(t.Context(), spoolAt(secondPosition)); !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Errorf("Begin after Close = %v, want ErrCheckpointRefused", err)
	}
}

func spooled(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the spool dir: %v", err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestAnUploadIsPacedToItsRate(t *testing.T) {
	w := startWorld(t)
	w.store.options.UploadRate = pacedRate
	sink, retention := w.started(t)
	image := checkpointImage(t)

	if err := spoolCheckpoint(t, sink, firstPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}

	stats := w.settled(t, committedThrough(firstPosition))
	atLeast := transfer(int64(len(image)-burstChunks*uploadChunk), pacedRate)
	if stats.LastUpload < atLeast {
		t.Errorf("upload took %s, want at least %s at %d bytes/s", stats.LastUpload, atLeast, pacedRate)
	}
	w.assertInvariants(t, retention, 0)
}

func TestOpenRemovesTheSpoolsOfAnEarlierProcessOnly(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"7-10" + spoolSuffix, "keep.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	if err := removeSpools(dir); err != nil {
		t.Fatalf("removeSpools: %v", err)
	}

	if diff := cmp.Diff([]string{"keep.txt"}, spooled(t, dir)); diff != "" {
		t.Errorf("spool dir mismatch (-want +got):\n%s", diff)
	}
}

func TestAnAbortedCheckpointLeavesNothingAndFreesTheSlot(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	writer, err := sink.Begin(t.Context(), spoolAt(firstPosition))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := writer.Write(checkpointImage(t)); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := writer.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools left after Abort: %v", left)
	}
	if err := spoolCheckpoint(t, sink, secondPosition, checkpointImage(t)); err != nil {
		t.Fatalf("a checkpoint after Abort: %v", err)
	}
	w.settled(t, committedThrough(secondPosition))
	w.assertInvariants(t, retention, 0)
}

func TestAMetaConflictWithTheReplicatedCodeIsSettledByItsOwnMessage(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			sink, retention := w.started(t)
			w.once(isMeta, natsjstest.Answer([]byte(replicatedConflict)))

			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}

			w.settled(t, committedThrough(firstPosition))
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestAChunkAckThatDidNotGrowFailsTheUpload(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var chunks atomic.Int32
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunk(f) && chunks.Add(1) == staleAckChunk {
			return natsjstest.Answer([]byte(staleAck))
		}
		return natsjstest.Forward()
	})

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	stats := w.settled(t, failedFor("upload"))
	if !errors.Is(stats.LastFailure, errForeignAck) || len(w.ownObjects(t)) != 0 {
		t.Errorf("last failure %v, objects %v; want a stale ack and nothing left", stats.LastFailure, w.ownObjects(t))
	}
	w.assertInvariants(t, retention, 0)
}

func TestAChunkChangedOnItsWayFailsTheVerify(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var chunks atomic.Int32
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunk(f) && chunks.Add(1) == changedChunk {
			f.Payload[len(f.Payload)-1] ^= 1
		}
		return natsjstest.Forward()
	})

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	stats := w.settled(t, failedFor("verify"))
	if !errors.Is(stats.LastFailure, framed.ErrCorrupt) || len(w.ownObjects(t)) != 0 {
		t.Errorf("last failure %v, objects %v; want a corrupt read and nothing left", stats.LastFailure,
			w.ownObjects(t))
	}
	w.assertInvariants(t, retention, 0)
}

func TestAPointerPastThisCheckpointOnTheSameStreamRefusesIt(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	ahead := w.putObject(t, testEpoch, thirdPosition, checkpointImage(t))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch,
		StreamCreated: formatCreated(retention.created), Current: ahead})

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}

	stats := w.settled(t, failedFor("position_regressed"))
	if !errors.Is(stats.LastFailure, errPositionRegressed) {
		t.Errorf("last failure = %v, want a regressed position", stats.LastFailure)
	}
	if diff := cmp.Diff([]string{ahead.NUID}, w.ownObjects(t)); diff != "" {
		t.Errorf("objects mismatch (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 0)
}

func isPurge(f *natsjstest.Frame) bool {
	return f.Subject == bucketPurge
}

func TestAnObjectThatCannotBeTakenBackWaitsAndIsPurgedByALaterCycle(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var chunks atomic.Int32
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunk(f) && chunks.Add(1) == changedChunk {
			f.Payload[len(f.Payload)-1] ^= 1
		}
		if isPurge(f) {
			return natsjstest.Answer([]byte(storeFullAnswer))
		}
		return natsjstest.Forward()
	})

	w.spoolAndSettle(t, sink, firstPosition, failedFor("verify"))
	if stats := w.store.Stats(); stats.Pending != 1 {
		t.Errorf("pending = %d, want the object that could not be purged", stats.Pending)
	}
	w.spoolAndSettle(t, sink, secondPosition, failedFor("bucket_unresolved"))
	w.proxy.Intercept(nil)
	w.spoolAndSettle(t, sink, thirdPosition, committedThrough(thirdPosition))

	if stats := w.store.Stats(); stats.Pending != 0 {
		t.Errorf("pending = %d after a purge that went through", stats.Pending)
	}
	w.assertInvariants(t, retention, 0)
}

func TestAnObjectThatLeftThePointerButCannotBePurgedWaitsForTheNextCycle(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	for _, position := range []uint64{firstPosition, secondPosition} {
		w.spoolAndSettle(t, sink, position, committedThrough(position))
	}
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isPurge(f) {
			return natsjstest.Answer([]byte(storeFullAnswer))
		}
		return natsjstest.Forward()
	})

	w.spoolAndSettle(t, sink, thirdPosition, committedThrough(thirdPosition))
	if stats := w.store.Stats(); stats.Pending != 1 {
		t.Errorf("pending = %d, want the object that left", stats.Pending)
	}
	w.proxy.Intercept(nil)
	w.spoolAndSettle(t, sink, fourthPosition, committedThrough(fourthPosition))

	value, _ := w.pointer(t)
	if diff := cmp.Diff(pointerShape{Epoch: testEpoch, Current: fourthPosition, Previous: thirdPosition},
		shapeOfPointer(value)); diff != "" {
		t.Errorf("pointer mismatch (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 0)
}

func TestACheckpointBetweenCloseAndTheEndOfTheUploadIsRefused(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	held := newGate(t)
	w.holdFirst(held, isPointerWrite)
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)
	ctx, cancel := context.WithTimeout(t.Context(), stopWithin)
	defer cancel()
	w.store.Close(ctx)

	err := spoolCheckpoint(t, sink, secondPosition, checkpointImage(t))
	held.release()

	if !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Errorf("a checkpoint after Close = %v, want ErrCheckpointRefused", err)
	}
	if err := w.store.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools left: %v", left)
	}
	w.assertInvariants(t, retention, 0)
}

func TestACloseWithoutTimeLeftIsAStopNotAnEnd(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	held := newGate(t)
	w.holdFirst(held, isPointerWrite)
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)
	expired, cancel := context.WithDeadline(t.Context(), time.Now())
	defer cancel()

	w.store.Close(expired)
	held.release()

	waiting, stop := context.WithTimeout(t.Context(), stopWithin)
	defer stop()
	if err := w.store.Wait(waiting); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	select {
	case <-w.store.Done():
		t.Errorf("Done after a stop, Err() = %v", w.store.Err())
	default:
	}
	w.assertInvariants(t, retention, 1)
}

func classOf(counts map[string]uint64) string {
	var counted []string
	for class, count := range counts {
		if count > 0 {
			counted = append(counted, class)
		}
	}
	if len(counted) != 1 {
		return ""
	}
	return counted[0]
}

type stopArgs struct {
	corrupt bool
	drop    func(f *natsjstest.Frame) bool
}

func TestACloseWithoutTimeLeftLeavesTheRestToTheNextHolder(t *testing.T) {
	tests := []struct {
		name string
		args stopArgs
		want uint64
	}{
		{name: "while the own object is purged", args: stopArgs{corrupt: true, drop: isPurge}, want: 1},
		{name: "while an Update without an answer is settled", args: stopArgs{drop: isPointerWrite}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			var seen atomic.Bool
			var chunks atomic.Int32
			w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
				if tt.args.corrupt && isChunk(f) && chunks.Add(1) == changedChunk {
					f.Payload[len(f.Payload)-1] ^= 1
				}
				if tt.args.drop(f) {
					seen.Store(true)
					return natsjstest.Drop()
				}
				return natsjstest.Forward()
			})
			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}
			waitFor(t, &seen)

			w.closeExpired(t)

			if got := w.store.Stats().LeftAtStop; got != tt.want {
				t.Errorf("left at stop = %d, want %d", got, tt.want)
			}
			w.assertInvariants(t, retention, 1)
		})
	}
}

func waitFor(t *testing.T, seen *atomic.Bool) {
	t.Helper()
	deadline := time.Now().Add(settleWithin)
	for !seen.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("the frame did not come within %s", settleWithin)
		}
		time.Sleep(settlePoll)
	}
}

func (w *world) closeExpired(t *testing.T) {
	t.Helper()
	expired, cancel := context.WithDeadline(t.Context(), time.Now())
	defer cancel()
	w.store.Close(expired)
	waiting, stop := context.WithTimeout(t.Context(), stopPromptly)
	defer stop()
	if err := w.store.Wait(waiting); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	select {
	case <-w.store.Done():
		t.Errorf("Done after a stop, Err() = %v", w.store.Err())
	default:
	}
}

func TestABucketGoneDuringTheVerifyLeavesTheObjectWaiting(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	var gone atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunkRead(f) && gone.CompareAndSwap(false, true) {
			err := w.operator.DeleteStream(context.WithoutCancel(t.Context()), objStreamPrefix+w.options.objectBucket())
			if err != nil {
				t.Errorf("delete the object bucket: %v", err)
			}
		}
		return natsjstest.Forward()
	})

	w.spoolAndSettle(t, sink, firstPosition, failedFor("verify"))

	if stats := w.store.Stats(); stats.Pending != 1 {
		t.Errorf("pending = %d, want the object that could not be purged", stats.Pending)
	}
	select {
	case <-w.store.Done():
		t.Errorf("the store ended: %v", w.store.Err())
	default:
	}
}

func TestACheckpointWhileTheLeaseIsSuspendedIsBusy(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	w.lease.set(false)

	_, err := sink.Begin(t.Context(), spoolAt(firstPosition))

	if !errors.Is(err, processing.ErrCheckpointBusy) {
		t.Fatalf("Begin = %v, want ErrCheckpointBusy", err)
	}
	if stats := w.store.Stats(); stats.Skipped["lease_suspended"] != 1 {
		t.Errorf("lease suspended skips = %d, want 1", stats.Skipped["lease_suspended"])
	}
	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools written: %v", left)
	}
}
