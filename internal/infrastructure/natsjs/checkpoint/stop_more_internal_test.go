package checkpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	retryStop          = 2500 * time.Millisecond
	beforeTheCallsEdge = 50 * time.Millisecond
	casSettleStop      = 2 * time.Second
	verifyDropRate     = 1200
	pacedUploadRate    = 1024
	pausedChunk        = 3
	pausedStop         = 1600 * time.Millisecond
	leftName           = "1-10"
	spoolName          = "1-10.spool"
	retriedTimes       = 3
)

func closedStopping() chan struct{} {
	stopping := make(chan struct{})
	close(stopping)
	return stopping
}

func spoolStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, spoolName), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &Store{options: Options{SpoolDir: dir}}
}

func TestASpoolTakenAfterTheStopIsNeverUploaded(t *testing.T) {
	type want struct {
		Uploaded   bool
		Slots      int
		Queued     bool
		NotStarted uint64
		OnDisk     bool
	}
	tests := []struct {
		name string
		args bool
		want want
	}{
		{name: "an ordinary spool is given up and frees its slot", want: want{NotStarted: 1}},
		{name: "the final waits for its plan", args: true, want: want{Slots: 1, Queued: true, OnDisk: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &uploader{store: spoolStore(t), stopping: closedStopping(), slots: 1}
			uploaded := up.taken(t.Context(), t.Context(), &spool{name: spoolName, final: tt.args})
			_, statErr := os.Stat(filepath.Join(up.store.options.SpoolDir, spoolName))
			got := want{Uploaded: uploaded, Slots: up.slots, Queued: up.queued != nil,
				NotStarted: up.store.stats.skipped[skipNotStarted].Load(), OnDisk: statErr == nil}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("taken after the stop (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAFinalThePlanSkipsFreesItsSlotAndSpool(t *testing.T) {
	up := &uploader{store: spoolStore(t), stopping: closedStopping(), slots: 1,
		queued: &spool{name: spoolName, final: true, size: mebibyte}, stopAt: time.Now()}
	up.store.options.UploadRate = oneMiBPerSec

	up.startQueued(t.Context(), t.Context())

	if _, err := os.Stat(filepath.Join(up.store.options.SpoolDir, spoolName)); up.slots != 0 || err == nil {
		t.Errorf("slots %d, spool on disk %t; want both freed", up.slots, err == nil)
	}
}

func TestOnlyAMetaThisUploadMaySendIsPurged(t *testing.T) {
	up := &uploader{store: &Store{}}
	ref := objectRef{Name: leftName}
	tests := []struct {
		name     string
		found    metaFound
		metaSent bool
		want     string
	}{
		{name: "its own meta", found: metaOwn, want: up.store.metaSubject(leftName)},
		{name: "no meta yet but one was sent", found: metaNone, metaSent: true, want: up.store.metaSubject(leftName)},
		{name: "no meta and none sent", found: metaNone},
		{name: "another object's meta of that name", found: metaForeign, metaSent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, up.metaToPurge(tt.found, ref, tt.metaSent)); diff != "" {
				t.Errorf("meta subject (-want +got):\n%s", diff)
			}
		})
	}
}

func TestADeadParentIsAStopOnlyWhenTheStopKilledIt(t *testing.T) {
	tests := []struct {
		name    string
		stopped bool
		want    error
	}{
		{name: "the upload budget ran out", want: context.DeadlineExceeded},
		{name: "the cut edge fired", stopped: true, want: errNoTimeLeft},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, cancel := context.WithDeadline(t.Context(), time.Now())
			defer cancel()
			if tt.stopped {
				parent = canceledWith(errStopUpload)
			}
			up := stoppingAt(time.Now().Add(time.Hour))
			if _, _, err := up.bounded(parent, time.Second, edgeUpload); !errors.Is(err, tt.want) {
				t.Errorf("bounded = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestAMetaLeftAfterThePurgeIsCounted(t *testing.T) {
	w := startWorld(t)
	w.started(t)
	if _, err := w.operator.PublishMsg(t.Context(), &nats.Msg{Subject: w.store.metaSubject(leftName),
		Data: []byte("{}")}); err != nil {
		t.Fatal(err)
	}

	err := w.store.up.nothingLeft(t.Context(), leftName, w.store.chunkSubject("absent"),
		w.store.metaSubject(leftName))

	left, counted := errors.AsType[*leftOverError](err)
	if !counted || left.meta != 1 || left.chunks != 0 {
		t.Errorf("nothingLeft = %v, want one meta left", err)
	}
}

type pointerScript struct {
	reads     atomic.Int32
	writes    atomic.Int32
	dropReads atomic.Bool
	readHeld  atomic.Bool
	holdRead  chan time.Time
	write     natsjstest.Verdict
}

func (w *world) scripted(script *pointerScript) {
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		w.request(f)
		if isPointerRead(f) {
			script.reads.Add(1)
		}
		switch {
		case isPointerWrite(f) && script.writes.Add(1) == 1:
			return script.write
		case isPointerRead(f) && script.dropReads.Load() && script.writes.Load() > 0:
			return natsjstest.Drop()
		case isPointerRead(f) && script.holdRead != nil && script.writes.Load() > 0 &&
			script.readHeld.CompareAndSwap(false, true):
			time.Sleep(time.Until(<-script.holdRead))
		}
		return natsjstest.Forward()
	})
}

func (w *world) closeAfter(t *testing.T, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), within)
	defer cancel()
	w.store.Close(ctx)
	if err := w.store.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

type casOutcome struct {
	Unknown      uint64
	LeftAtStop   uint64
	FailedCAS    uint64
	Finals       uint64
	FinalSkipped uint64
	Objects      int
}

func (w *world) casOutcome(t *testing.T) casOutcome {
	t.Helper()
	stats := w.store.Stats()
	return casOutcome{
		Unknown:      stats.CASUnknown,
		LeftAtStop:   stats.LeftAtStop,
		FailedCAS:    stats.Failed["cas"],
		Finals:       stats.Finals,
		FinalSkipped: stats.Skipped["final_skipped"],
		Objects:      len(w.ownObjects(t)),
	}
}

func TestAFinalWhosePointerWriteGotNoAnswerAtTheStopIsKeptAndLeft(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	script := &pointerScript{write: natsjstest.Drop()}
	script.dropReads.Store(true)
	w.scripted(script)
	if err := spoolFinal(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("final: %v", err)
	}
	w.settled(t, func(*Stats) bool {
		return script.writes.Load() > 0
	})

	w.closeAfter(t, casSettleStop)

	want := casOutcome{Unknown: 1, LeftAtStop: 1, FailedCAS: 1, FinalSkipped: 1, Objects: 1}
	if diff := cmp.Diff(want, w.casOutcome(t)); diff != "" {
		t.Errorf("outcome (-want +got):\n%s", diff)
	}
	w.assertStopInvariants(t)
}

func TestARetryOfASentWriteWithNoTimeLeftWaitsInsteadOfDropping(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	script := &pointerScript{write: natsjstest.Drop(), holdRead: make(chan time.Time, 1)}
	w.scripted(script)
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	w.settled(t, func(*Stats) bool {
		return script.readHeld.Load()
	})
	closedAt := time.Now()
	script.holdRead <- closedAt.Add(retryStop - stopMargin - stopMargin - beforeTheCallsEdge)

	w.closeAfter(t, retryStop)

	if got := w.casOutcome(t); got.Unknown != 1 || got.Objects != 1 || w.store.Stats().Skipped["stopped"] != 0 {
		t.Errorf("outcome %+v, stopped %d; want the sent write unknown and the object kept", got,
			w.store.Stats().Skipped["stopped"])
	}
}

func TestAnUnknownFinalTheStopFindsInThePointerIsCountedAsAFinal(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	script := &pointerScript{write: natsjstest.LoseReply()}
	script.dropReads.Store(true)
	w.scripted(script)
	if err := spoolFinal(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("final: %v", err)
	}
	w.settled(t, func(stats *Stats) bool {
		return stats.CASUnknown == 1
	})
	script.dropReads.Store(false)

	w.closeAfter(t, stopWithin)

	want := casOutcome{Unknown: 1, FailedCAS: 1, Finals: 1, Objects: 1}
	if diff := cmp.Diff(want, w.casOutcome(t)); diff != "" {
		t.Errorf("outcome (-want +got):\n%s", diff)
	}
	w.assertStopInvariants(t)
}

func TestAWaitingListCutByTheStopIsAStopNotAFailure(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	script := &pointerScript{write: natsjstest.Drop()}
	script.dropReads.Store(true)
	w.scripted(script)
	w.spoolAndSettle(t, sink, firstPosition, func(stats *Stats) bool {
		return stats.CASUnknown == 1
	})
	reads := script.reads.Load()
	if err := spoolCheckpoint(t, sink, secondPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	w.settled(t, func(*Stats) bool {
		return script.reads.Load() > reads
	})

	w.closeAfter(t, tightStop)

	stats := w.store.Stats()
	if stats.Failed["bucket_unresolved"] != 0 || stats.Skipped["stopped"] != 1 {
		t.Errorf("unresolved failures %d, stopped %d; want the cut list counted as a stop",
			stats.Failed["bucket_unresolved"], stats.Skipped["stopped"])
	}
}

func TestAVerifyThatNoLongerFitsIsDroppedForTheQueuedFinal(t *testing.T) {
	w := startWorld(t)
	image := checkpointImage(t)
	w.store.options.VerifyRate = verifyDropRate
	sink, retention := w.started(t)
	var reading atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		w.request(f)
		if isChunkRead(f) {
			reading.Store(true)
		}
		return natsjstest.Forward()
	})
	if err := spoolCheckpoint(t, sink, firstPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	waitFor(t, &reading)
	if err := spoolFinal(t, sink, secondPosition, image); err != nil {
		t.Fatalf("final: %v", err)
	}

	w.closeAfter(t, bothFitStop)

	want := stopOutcome{Committed: 1, Finals: 1, Stopped: 1}
	if diff := cmp.Diff(want, w.outcome(retention), cmpEmptyAsNil()); diff != "" {
		t.Errorf("outcome (-want +got):\n%s", diff)
	}
	w.assertStopInvariants(t)
}

func TestAPacePauseCutByTheStopIsAStopNotAFailure(t *testing.T) {
	w := startWorld(t)
	w.store.options.UploadRate = pacedUploadRate
	sink, retention := w.started(t)
	var chunks atomic.Int32
	var paused atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		w.request(f)
		if isChunk(f) && chunks.Add(1) == pausedChunk {
			paused.Store(true)
		}
		return natsjstest.Forward()
	})
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	waitFor(t, &paused)

	w.closeAfter(t, pausedStop)

	stats := w.store.Stats()
	if stats.Skipped["stopped"] != 1 || stats.Failed["upload"] != 0 || stats.LastFailure != nil {
		t.Errorf("stopped %d, failed uploads %d, last failure %v; want a stop", stats.Skipped["stopped"],
			stats.Failed["upload"], stats.LastFailure)
	}
	w.assertInvariants(t, retention, 0)
}

func TestWaitDuringAStakeWaitsForIt(t *testing.T) {
	w := startWorld(t)
	if err := latestErr(w.store.Latest(t.Context(), testPartition)); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	held := newGate(t)
	w.holdFirst(held, isPointerWrite)
	staked := make(chan error, 1)
	go func() {
		_, err := w.store.StakeEmpty(t.Context())
		staked <- err
	}()
	held.waitHeld(t)

	waiting, cancel := context.WithTimeout(t.Context(), waitStaysShut)
	defer cancel()
	err := w.store.Wait(waiting)
	held.release()

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait during the stake = %v, want it to wait past its own deadline", err)
	}
	if err := <-staked; err != nil {
		t.Errorf("stake after the release = %v, want it done", err)
	}
}

func TestSpoolsLeftAtATerminalEndAreCountedAsNotStarted(t *testing.T) {
	store := spoolStore(t)
	store.failed = errPointerForeign
	store.done = make(chan struct{})
	up := &uploader{store: store, spools: make(chan spool, spoolSlots), exited: make(chan struct{}), slots: 1,
		cancels: [edges]func(){func() {}, func() {}, func() {}}}
	up.spools <- spool{name: spoolName}

	up.exit()

	_, statErr := os.Stat(filepath.Join(store.options.SpoolDir, spoolName))
	if got := store.stats.skipped[skipNotStarted].Load(); got != 1 || statErr == nil || up.slots != 0 {
		t.Errorf("not started %d, spool on disk %t, slots %d; want one counted, removed and freed", got,
			statErr == nil, up.slots)
	}
}

func TestACutInsideARetryPauseIsAStopNotAFailure(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var refused atomic.Int32
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		w.request(f)
		if isChunk(f) {
			refused.Add(1)
			return natsjstest.Answer([]byte(inProcessAnswer))
		}
		return natsjstest.Forward()
	})
	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	w.settled(t, func(*Stats) bool {
		return refused.Load() >= retriedTimes
	})

	w.closeAfter(t, pausedStop)

	stats := w.store.Stats()
	if stats.Skipped["stopped"] != 1 || stats.Failed["upload"] != 0 || stats.LastFailure != nil {
		t.Errorf("stopped %d, failed uploads %d, last failure %v; want a stop", stats.Skipped["stopped"],
			stats.Failed["upload"], stats.LastFailure)
	}
	w.assertInvariants(t, retention, 0)
}

func TestABeginWithACanceledContextLeavesNoSpool(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for name, begin := range map[string]func(context.Context, *processing.CheckpointMeta) (
		processing.CheckpointWriter, error){"Begin": sink.Begin, "BeginOnStop": sink.BeginOnStop} {
		if _, err := begin(canceled, spoolAt(firstPosition)); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a canceled context = %v, want the cancel", name, err)
		}
	}
	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools %v left by a canceled begin", left)
	}
}
