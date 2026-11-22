package checkpoint

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	bothFitStop     = 6 * time.Second
	finalOnlyStop   = 3500 * time.Millisecond
	nothingFitsStop = 1800 * time.Millisecond
	expiredRuns     = 20
	waitStaysShut   = 300 * time.Millisecond
	verifyPacePart  = 4
	pacedRead       = 3
	cutInsidePace   = 1800 * time.Millisecond
)

type stopOutcome struct {
	Committed    uint64
	Finals       uint64
	Stopped      uint64
	FinalSkipped uint64
	Failed       uint64
	Commits      []uint64
}

func (w *world) outcome(retention *fakeRetention) stopOutcome {
	stats := w.store.Stats()
	var failures uint64
	for _, count := range stats.Failed {
		failures += count
	}
	return stopOutcome{
		Committed:    stats.Committed,
		Finals:       stats.Finals,
		Stopped:      stats.Skipped["stopped"],
		FinalSkipped: stats.Skipped["final_skipped"],
		Failed:       failures,
		Commits:      retention.committed(),
	}
}

func spoolFinal(t *testing.T, sink *Sink, position uint64, image []byte) error {
	t.Helper()
	writer, err := sink.BeginOnStop(t.Context(), spoolAt(position))
	if err != nil {
		return err
	}
	if _, err := writer.Write(image); err != nil {
		t.Fatalf("write spool: %v", err)
	}
	return writer.Close()
}

func (w *world) assertStopInvariants(t *testing.T) {
	t.Helper()
	stats := w.store.Stats()
	w.store.mu.Lock()
	handedOff := uint64(boolCount(w.store.up.finalHandedOff))
	w.store.mu.Unlock()
	if stats.Finals > stats.Committed || stats.Finals+stats.Skipped["final_skipped"] != handedOff {
		t.Errorf("finals %d, committed %d, final skipped %d, handed off %d; want each handed off final counted once",
			stats.Finals, stats.Committed, stats.Skipped["final_skipped"], handedOff)
	}
	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools %v left after the stop", left)
	}
}

func TestOneFinalIsLetInPastABusyUploadAndLeavesTheFeedAlone(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	image := checkpointImage(t)
	held := newGate(t)
	w.holdFirst(held, isChunk)
	if err := spoolCheckpoint(t, sink, firstPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)

	refusals := []error{
		spoolCheckpoint(t, sink, secondPosition, image),
		spoolFinal(t, sink, secondPosition, image),
		spoolFinal(t, sink, thirdPosition, image),
		spoolCheckpoint(t, sink, thirdPosition, image),
	}
	held.release()
	w.settled(t, committedThrough(secondPosition))

	for i, want := range []error{processing.ErrCheckpointBusy, nil, processing.ErrCheckpointBusy,
		processing.ErrCheckpointBusy} {
		if !errors.Is(refusals[i], want) || want == nil && refusals[i] != nil {
			t.Errorf("begin %d = %v, want %v", i, refusals[i], want)
		}
	}
	want := stopOutcome{Committed: 2, Finals: 1, Commits: []uint64{firstPosition}}
	if diff := cmp.Diff(want, w.outcome(retention)); diff != "" {
		t.Errorf("outcome (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 1)
	w.assertStopInvariants(t)
}

func TestASecondFinalWithNothingInFlightIsAnOrdinaryCheckpoint(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	image := checkpointImage(t)
	if err := spoolFinal(t, sink, firstPosition, image); err != nil {
		t.Fatalf("first final: %v", err)
	}
	w.settled(t, committedThrough(firstPosition))
	if err := spoolFinal(t, sink, secondPosition, image); err != nil {
		t.Fatalf("second final: %v", err)
	}
	w.settled(t, committedThrough(secondPosition))

	want := stopOutcome{Committed: 2, Finals: 1, Commits: []uint64{secondPosition}}
	if diff := cmp.Diff(want, w.outcome(retention)); diff != "" {
		t.Errorf("outcome (-want +got):\n%s", diff)
	}
	w.assertStopInvariants(t)
}

func (w *world) finalBehindAnUpload(t *testing.T, match func(f *natsjstest.Frame) bool) (*fakeRetention, *gate) {
	t.Helper()
	sink, retention := w.started(t)
	image := checkpointImage(t)
	held := newGate(t)
	w.holdFirst(held, match)
	if err := spoolCheckpoint(t, sink, firstPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	held.waitHeld(t)
	if err := spoolFinal(t, sink, secondPosition, image); err != nil {
		t.Fatalf("final: %v", err)
	}
	return retention, held
}

func TestTheStopPlanPicksWhatTheTimeLeftHolds(t *testing.T) {
	tests := []struct {
		name  string
		args  time.Duration
		match func(f *natsjstest.Frame) bool
		want  stopOutcome
	}{
		{name: "both fit: the upload and then the final land", args: bothFitStop, match: isChunk,
			want: stopOutcome{Committed: 2, Finals: 1}},
		{name: "only the final fits: the upload is taken back for it", args: finalOnlyStop, match: isChunk,
			want: stopOutcome{Committed: 1, Finals: 1, Stopped: 1}},
		{name: "nothing fits: both are given up", args: nothingFitsStop, match: isChunk,
			want: stopOutcome{Stopped: 1, FinalSkipped: 1}},
		{name: "a pointer write in flight is finished and the final decided after it", args: bothFitStop,
			match: isPointerWrite, want: stopOutcome{Committed: 2, Finals: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			retention, held := w.finalBehindAnUpload(t, tt.match)

			w.closeWithin(t, held, tt.args)

			if diff := cmp.Diff(tt.want, w.outcome(retention), cmpEmptyAsNil()); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 1)
			w.assertStopInvariants(t)
		})
	}
}

func TestAFinalUploadingAtTheStopFinishesOrIsGivenUp(t *testing.T) {
	tests := []struct {
		name string
		args time.Duration
		want stopOutcome
	}{
		{name: "with time left it lands without the feed", args: bothFitStop, want: stopOutcome{Committed: 1,
			Finals: 1}},
		{name: "without time it is taken back and counted", args: tightStop, want: stopOutcome{Stopped: 1,
			FinalSkipped: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			held := newGate(t)
			w.holdFirst(held, isChunk)
			if err := spoolFinal(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("final: %v", err)
			}
			held.waitHeld(t)

			w.closeWithin(t, held, tt.args)

			if diff := cmp.Diff(tt.want, w.outcome(retention), cmpEmptyAsNil()); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
			w.assertStopInvariants(t)
		})
	}
}

func TestACloseAlreadyPastItsDeadlineIsAStopEveryTime(t *testing.T) {
	for range expiredRuns {
		w := startWorld(t)
		retention, held := w.finalBehindAnUpload(t, isChunk)
		expired, cancel := context.WithDeadline(t.Context(), time.Now())
		w.store.Close(expired)
		cancel()
		held.release()
		waiting, stop := context.WithTimeout(t.Context(), stopPromptly)
		if err := w.store.Wait(waiting); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		stop()
		if got := w.outcome(retention); got.Failed != 0 || got.FinalSkipped != 1 {
			t.Fatalf("outcome %+v, want no failure and the final counted as skipped", got)
		}
	}
}

func TestAVerifyCutWhilePacedIsAStopNotAFailure(t *testing.T) {
	w := startWorld(t)
	image := checkpointImage(t)
	w.store.options.VerifyRate = int64(len(image)) / verifyPacePart
	sink, retention := w.started(t)
	var reads atomic.Int32
	var paced atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunkRead(f) && reads.Add(1) == pacedRead {
			paced.Store(true)
		}
		return natsjstest.Forward()
	})
	if err := spoolCheckpoint(t, sink, firstPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	waitFor(t, &paced)

	w.closeWithin(t, newGate(t), cutInsidePace)

	stats := w.store.Stats()
	if stats.Skipped["stopped"] != 1 || stats.Failed["verify"] != 0 || stats.LastFailure != nil {
		t.Errorf("stopped %d, failed verifies %d, last failure %v; want a stop", stats.Skipped["stopped"],
			stats.Failed["verify"], stats.LastFailure)
	}
	if objects := w.ownObjects(t); len(objects) != 0 {
		t.Errorf("objects %v left after the stop, want the upload taken back", objects)
	}
	w.assertInvariants(t, retention, 0)
}

func TestCloseDuringTheStakeCancelsItAndWaitWaitsForIt(t *testing.T) {
	w := startWorld(t)
	if err := latestErr(w.store.Latest(t.Context(), testPartition)); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	held := newGate(t)
	w.holdFirst(held, isPointerWrite)
	staked := make(chan error, 1)
	go func() {
		_, err := w.store.Stake(t.Context())
		staked <- err
	}()
	held.waitHeld(t)
	if _, err := w.store.Start(t.Context(), &fakeRetention{}); !errors.Is(err, processing.ErrCheckpointRefused) {
		t.Errorf("Start during the stake = %v, want refused", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), stopWithin)
	defer cancel()
	w.store.Close(ctx)
	err := <-staked
	waitErr := w.store.Wait(ctx)
	held.release()

	if !errors.Is(err, processing.ErrCheckpointRefused) || waitErr != nil {
		t.Errorf("stake %v, Wait %v; want the stake refused by the close and Wait done", err, waitErr)
	}
}

func TestCloseCancelsADiscardThatWaitsBehindALatest(t *testing.T) {
	w := startWorld(t)
	w.putPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch - 1})
	held := newGate(t)
	w.holdFirst(held, isPointerRead)
	go func() {
		_ = latestErr(w.store.Latest(context.WithoutCancel(t.Context()), testPartition))
	}()
	held.waitHeld(t)
	discarded := make(chan error, 1)
	go func() {
		_, err := w.store.Discard(t.Context(), processing.DiscardTarget{Partition: testPartition})
		discarded <- err
	}()
	time.Sleep(waitStaysShut)

	ctx, cancel := context.WithTimeout(t.Context(), stopWithin)
	defer cancel()
	w.store.Close(ctx)
	err := <-discarded
	waitErr := w.store.Wait(ctx)
	held.release()

	if !errors.Is(err, processing.ErrCheckpointRefused) || waitErr != nil {
		t.Errorf("discard %v, Wait %v; want the discard refused while the Latest still reads", err, waitErr)
	}
}

func (w *world) idleSlots(ctx context.Context) error {
	for {
		w.store.mu.Lock()
		idle := w.store.up.slots == 0
		w.store.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(settlePoll):
		}
	}
}

func (r *fakeRetention) last() (processing.Position, bool) {
	commits := r.committed()
	if len(commits) == 0 {
		return processing.Position{}, false
	}
	return processing.Position{
		Partition: testPartition,
		Sequence:  commits[len(commits)-1],
	}, true
}

func TestTheStoreKeepsTheSinkContract(t *testing.T) {
	processingtest.RunCheckpointSinkContract(t, func(t *testing.T) processingtest.SinkHarness {
		w := startWorld(t)
		sink, retention := w.started(t)
		return processingtest.SinkHarness{
			Sink:      sink,
			Partition: testPartition,
			Hold: func() func() {
				held := newGate(t)
				w.holdFirst(held, isChunk)
				return held.release
			},
			Settled: func(ctx context.Context) error {
				waiting, cancel := context.WithTimeout(ctx, settleWithin)
				defer cancel()
				return w.idleSlots(waiting)
			},
			Stop: func(ctx context.Context) error {
				stopping, cancel := context.WithTimeout(ctx, stopWithin)
				defer cancel()
				w.store.Close(stopping)
				return w.store.Wait(stopping)
			},
			Latest: func(ctx context.Context) (processing.CheckpointMeta, io.ReadCloser, error) {
				return w.store.Latest(ctx, testPartition)
			},
			Retained: retention.last,
		}
	})
}
