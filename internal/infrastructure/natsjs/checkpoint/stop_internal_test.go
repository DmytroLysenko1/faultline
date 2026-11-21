package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	edgeRaceRuns   = 1000
	edgeFutureRuns = 50
	slotRuns       = 2000
	slotSteps      = 12
	boundSlack     = 50 * time.Millisecond
)

var errFromServer = errors.New("an answer of the server")

func canceledWith(cause error) context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	return ctx
}

func TestAStopCauseIsJoinedOnlyToTheErrorOfItsOwnContext(t *testing.T) {
	type args struct {
		upload bool
		err    error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "the call ended with its context", args: args{upload: true, err: fmt.Errorf("publish: %w",
			context.Canceled)}, want: true},
		{name: "a server answer after the cut", args: args{upload: true, err: errFromServer}},
		{name: "a context still alive", args: args{err: context.Canceled}},
		{name: "no error", args: args{upload: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			if tt.args.upload {
				ctx = canceledWith(errStopUpload)
			}
			if diff := cmp.Diff(tt.want, stopCaused(stopMarked(ctx, tt.args.err))); diff != "" {
				t.Errorf("stop caused (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAFailureIsAStopOnlyWhenTheStopCausedIt(t *testing.T) {
	up := &uploader{store: &Store{}}
	tests := []struct {
		name string
		args error
		want cycleKind
	}{
		{name: "a terminal stays terminal under a stop cause", args: terminal(errors.Join(errLeaseLost, errStopUpload)),
			want: kindTerminal},
		{name: "the plan dropped the upload", args: errStopPlan, want: kindStopped},
		{name: "a verify cut by the stop", args: failed(failVerify, fmt.Errorf("read: %w", errStopUpload)),
			want: kindStopped},
		{name: "no time left for a call", args: errNoTimeLeft, want: kindStopped},
		{name: "a regressed position read after the cut", args: failed(failRegressed, errPositionRegressed),
			want: kindFailed},
		{name: "a refused chunk", args: errFromServer, want: kindFailed},
		{name: "a suspended lease", args: skipped(skipLeaseSuspended, errLeaseLost), want: kindSkipped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome, known := errors.AsType[*cycleError](up.classified(tt.args))
			if !known {
				t.Fatalf("classified %v is no cycle outcome", tt.args)
			}
			if diff := cmp.Diff(tt.want, outcome.kind); diff != "" {
				t.Errorf("kind (-want +got):\n%s", diff)
			}
		})
	}
}

func stoppingAt(stopAt time.Time) *uploader {
	return &uploader{
		store:  &Store{},
		stopAt: stopAt,
	}
}

func TestACallIsBoundedByTheStopOnlyWhenTheStopBinds(t *testing.T) {
	type want struct {
		left  time.Duration
		cause error
		err   error
	}
	tests := []struct {
		name string
		args time.Duration
		own  time.Duration
		want want
	}{
		{name: "before the stop the call has its own budget", own: time.Second,
			want: want{left: time.Second, cause: context.DeadlineExceeded}},
		{name: "its own budget ends before the edge", args: 5 * time.Second, own: time.Second,
			want: want{left: time.Second, cause: context.DeadlineExceeded}},
		{name: "the edge ends it first", args: 2 * time.Second, own: 5 * time.Second,
			want: want{left: 1500 * time.Millisecond, cause: errStopControl}},
		{name: "less than the floor before the edge", args: stopMargin + minCallBudget/2, own: time.Second,
			want: want{err: errNoTimeLeft}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := stoppingAt(time.Time{})
			if tt.args > 0 {
				up = stoppingAt(time.Now().Add(tt.args))
			}
			ctx, cancel, err := up.bounded(t.Context(), tt.own, edgeControl)
			if !errors.Is(err, tt.want.err) {
				t.Fatalf("bounded = %v, want %v", err, tt.want.err)
			}
			if err != nil {
				return
			}
			defer cancel()
			deadline, _ := ctx.Deadline()
			if left := time.Until(deadline); left > tt.want.left || left < tt.want.left-boundSlack {
				t.Errorf("deadline in %s, want %s", left, tt.want.left)
			}
			<-ctx.Done()
			if cause := context.Cause(ctx); !errors.Is(cause, tt.want.cause) {
				t.Errorf("cause = %v, want %v", cause, tt.want.cause)
			}
		})
	}
}

func TestACallAfterItsEdgeIsNotLetOut(t *testing.T) {
	up := stoppingAt(time.Now().Add(time.Hour))
	called := false
	err := up.controlWithin(canceledWith(errStopControl), time.Second, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, errNoTimeLeft) || called {
		t.Errorf("callWithin = %v, called %t; want errNoTimeLeft before the call", err, called)
	}
}

func edgedUploader() (*uploader, context.Context, context.Context, context.Context) {
	root, cancelRoot := context.WithCancel(context.Background())
	upload, cancelUpload := context.WithCancelCause(root)
	control, cancelControl := context.WithCancelCause(root)
	return &uploader{
		store:     &Store{},
		stopping:  make(chan struct{}),
		accepting: true,
		cancels: [edges]func(){
			edgeUpload: func() {
				cancelUpload(errStopUpload)
			},
			edgeControl: func() {
				cancelControl(errStopControl)
			},
			edgeRoot: cancelRoot,
		},
	}, root, upload, control
}

func assertEdgeCauses(t *testing.T, root, upload, control context.Context) {
	t.Helper()
	<-root.Done()
	if cause := context.Cause(upload); !errors.Is(cause, errStopUpload) {
		t.Fatalf("cut cause = %v, want errStopCut", cause)
	}
	if cause := context.Cause(control); !errors.Is(cause, errStopControl) {
		t.Fatalf("calls cause = %v, want errStopCalls", cause)
	}
}

func TestAStopPastItsDeadlineKeepsEveryEdgeCause(t *testing.T) {
	for range edgeRaceRuns {
		up, root, upload, control := edgedUploader()
		up.stop(time.Now().Add(-time.Second))
		assertEdgeCauses(t, root, upload, control)
	}
}

func TestEdgesFiredByTheirTimersKeepTheirCauses(t *testing.T) {
	random := rand.New(rand.NewPCG(planSeed, planSeed+4))
	for range edgeFutureRuns {
		up, root, upload, control := edgedUploader()
		up.stop(time.Now().Add(time.Duration(random.Int64N(int64(20 * time.Millisecond)))))
		assertEdgeCauses(t, root, upload, control)
	}
}

func TestAStopAfterTheGoroutineLeftArmsNoTimer(t *testing.T) {
	up, root, _, _ := edgedUploader()
	up.accepting = false
	up.stop(time.Now().Add(-time.Second))
	if root.Err() != nil || up.timers != [edges]*time.Timer{} {
		t.Errorf("root %v, timers %v; want nothing touched after the goroutine left", root.Err(), up.timers)
	}
}

func slotUploader() *uploader {
	return &uploader{
		store: &Store{
			lease: &fakeLease{valid: true},
		},
		accepting: true,
	}
}

func TestTheSlotLetsOneFinalPastABusyUpload(t *testing.T) {
	type step struct {
		final bool
		want  bool
		err   error
	}
	tests := []struct {
		name string
		args []step
	}{
		{name: "a final past one in flight, then nothing more", args: []step{{}, {err: processing.ErrCheckpointBusy},
			{final: true, want: true}, {final: true, err: processing.ErrCheckpointBusy},
			{err: processing.ErrCheckpointBusy}}},
		{name: "a second final while the first is in flight is busy", args: []step{{final: true, want: true},
			{final: true, err: processing.ErrCheckpointBusy}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := slotUploader()
			for i, step := range tt.args {
				consumed, err := up.reserve(kindOf(step.final))
				if !errors.Is(err, step.err) || consumed != step.want {
					t.Fatalf("step %d: reserve(%t) = %t, %v; want %t, %v", i, step.final, consumed, err, step.want,
						step.err)
				}
			}
		})
	}
	up := slotUploader()
	if _, err := up.reserve(stopSpool); err != nil {
		t.Fatal(err)
	}
	up.free()
	if consumed, err := up.reserve(stopSpool); err != nil || consumed {
		t.Errorf("second final with nothing in flight = %t, %v; want an ordinary spool", consumed, err)
	}
}

func TestTheSlotNeverHoldsMoreThanTwoOrMarksTwoFinals(t *testing.T) {
	random := rand.New(rand.NewPCG(planSeed, planSeed+5))
	for range slotRuns {
		up := slotUploader()
		finals, peak, wide := 0, 0, 0
		for range slotSteps {
			if up.slots > 0 && random.IntN(3) == 0 {
				up.free()
				continue
			}
			consumed, err := up.reserve(kindOf(random.IntN(2) == 0))
			if err != nil {
				continue
			}
			finals += boolCount(consumed)
			peak = max(peak, up.slots)
			wide += boolCount(up.slots == spoolSlots)
		}
		if peak > spoolSlots || finals > 1 || wide > 1 {
			t.Fatalf("peak %d slots, %d finals, %d times full; want ≤ 2, ≤ 1, ≤ 1", peak, finals, wide)
		}
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestAFeedCommitCutByTheStopIsNoFailure(t *testing.T) {
	up := &uploader{store: &Store{lease: &fakeLease{valid: true}}}
	up.retainFailed(fmt.Errorf("commit: %w", errors.Join(context.Canceled, errStopControl)))
	if stats := up.store.stats.snapshot(testEpoch, testMaxBytes); stats.Failed["commit"] != 0 ||
		stats.LastFailure != nil {
		t.Errorf("failed commits %d, last failure %v; want neither", stats.Failed["commit"], stats.LastFailure)
	}
}

func TestAnInternalStopCauseNeverLeavesTheStoreAsACancel(t *testing.T) {
	err := translate(errors.Join(context.Canceled, errStopControl))
	if !errors.Is(err, processing.ErrCheckpointUnavailable) {
		t.Errorf("translate = %v, want ErrCheckpointUnavailable", err)
	}
	if err := translate(context.Canceled); !errors.Is(err, context.Canceled) ||
		errors.Is(err, processing.ErrCheckpointUnavailable) {
		t.Errorf("translate(Canceled) = %v, want the cancel as it is", err)
	}
}

func TestOnlyTheRemnantOfOneAttemptAtTheStopSkipsTheList(t *testing.T) {
	stopping := make(chan struct{})
	close(stopping)
	tests := []struct {
		name     string
		args     error
		stopping bool
		want     bool
	}{
		{name: "one chunk left at the stop", args: &leftOverError{chunks: 1}, stopping: true, want: true},
		{name: "one meta left at the stop", args: &leftOverError{meta: 1}, stopping: true, want: true},
		{name: "a chunk and a meta left", args: &leftOverError{chunks: 1, meta: 1}, stopping: true},
		{name: "one chunk left without a stop", args: &leftOverError{chunks: 1}},
		{name: "a refused purge at the stop", args: errFromServer, stopping: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &uploader{store: &Store{}, stopping: make(chan struct{})}
			if tt.stopping {
				up.stopping = stopping
			}
			if diff := cmp.Diff(tt.want, up.remnantOnly(tt.args)); diff != "" {
				t.Errorf("remnant only (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheSpeedsAreTheSlowestKnown(t *testing.T) {
	up := &uploader{store: &Store{options: Options{UploadRate: 8 * mebibyte}}}
	before := rates{upload: 8 * mebibyte, verify: verifyFloorRate}
	if diff := cmp.Diff(before, up.speed(), cmp.AllowUnexported(rates{})); diff != "" {
		t.Errorf("before any measure (-want +got):\n%s", diff)
	}
	up.measured = rates{upload: 4 * mebibyte, verify: 2 * mebibyte}
	up.progress = progress{started: time.Now().Add(-time.Second), sent: mebibyte, acked: burstChunks}
	up.phase = phaseUpload
	if got := up.speed(); got.upload > mebibyte || got.upload < mebibyte/2 || got.verify != 2*mebibyte {
		t.Errorf("speed = %+v, want the observed upload near 1 MiB/s and the measured verify", got)
	}
	up.store.options.VerifyRate = mebibyte
	if got := up.speed(); got.verify != mebibyte {
		t.Errorf("verify speed = %d, want the verify rate as the cap", got.verify)
	}
	up.phase = phaseVerify
	if got := up.speed(); got.upload != 4*mebibyte {
		t.Errorf("upload speed after the upload = %d, want the measure, not the stale observed one", got.upload)
	}
}

func TestAPreviousNotTakenBackAtTheStopIsCountedNotListed(t *testing.T) {
	type want struct {
		Pending      int
		PreviousLeft uint64
	}
	tests := []struct {
		name string
		args error
		want want
	}{
		{name: "taken back", want: want{}},
		{name: "cut by the stop", args: fmt.Errorf("purge: %w", errStopControl), want: want{PreviousLeft: 1}},
		{name: "no time left for the purge", args: errNoTimeLeft, want: want{PreviousLeft: 1}},
		{name: "refused by the server", args: errFromServer, want: want{Pending: 1}},
		{name: "a message left after the purge", args: &leftOverError{chunks: 1}, want: want{Pending: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &uploader{store: &Store{}}
			up.leftBehind(objectRef{Name: "1-10"}, tt.args)
			got := want{Pending: len(up.pending), PreviousLeft: up.store.stats.previousLeft.Load()}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("left behind (-want +got):\n%s", diff)
			}
		})
	}
}

func kindOf(final bool) spoolKind {
	if final {
		return stopSpool
	}
	return periodicSpool
}
