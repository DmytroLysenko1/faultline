package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	ruleEpoch      = 7
	staleRuns      = 2000
	staleEpochs    = 4
	staleNames     = 3
	runbookName    = "runbook"
	pauseOrphanAge = time.Minute
	boundWorst     = 6 << 10
	boundRate      = 1 << 10
	shortWindow    = 20 * time.Second
	longWindow     = 40 * time.Second
	nameFields     = 2
)

var pauseNow = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func ruleName(epoch, position uint64) string {
	return strconv.FormatUint(epoch, decimal) + nameSeparator + strconv.FormatUint(position, decimal)
}

func ruleRef(epoch, position uint64) *objectRef {
	return &objectRef{Name: ruleName(epoch, position), Position: position}
}

func TestOnlyAnObjectBelowTheRuleAndOutsideThePointerIsStale(t *testing.T) {
	held := &pointer{value: pointerValue{Current: ruleRef(ruleEpoch-1, firstPosition),
		Previous: ruleRef(ruleEpoch-2, firstPosition)}}
	tests := []struct {
		name string
		args string
		want bool
	}{
		{name: "an older epoch outside the pointer", args: ruleName(ruleEpoch-1, secondPosition), want: true},
		{name: "the current of the pointer", args: ruleName(ruleEpoch-1, firstPosition)},
		{name: "the previous of the pointer", args: ruleName(ruleEpoch-2, firstPosition)},
		{name: "the epoch of the rule itself", args: ruleName(ruleEpoch, secondPosition)},
		{name: "a later epoch", args: ruleName(ruleEpoch+1, secondPosition)},
		{name: "a name that tells no epoch", args: runbookName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &sweepRule{pointer: held, below: ruleEpoch}

			got := rule.stale(&storedMeta{name: tt.args})

			if got != tt.want {
				t.Errorf("stale(%s) = %t, want %t", tt.args, got, tt.want)
			}
		})
	}
}

func TestTheRuleCallsStaleExactlyTheNamedOlderObjectsNoPointerHolds(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	var counted [2]int
	for range staleRuns {
		below := random.Uint64N(staleEpochs) + 1
		current, previous := randomRuleRef(random), randomRuleRef(random)
		heldNames := map[string]bool{current.Name: true, previous.Name: true}
		name := randomRuleRef(random).Name
		if random.IntN(staleNames) == 0 {
			name = runbookName
		}
		rule := &sweepRule{pointer: &pointer{value: pointerValue{Current: current, Previous: previous}}, below: below}
		got := rule.stale(&storedMeta{name: name})
		if want := olderAndFree(name, below, heldNames); got != want {
			t.Fatalf("stale(%s) below epoch %d holding %v = %t, want %t", name, below, heldNames, got, want)
		}
		counted[boolCount(got)]++
	}
	if counted[0] == 0 || counted[1] == 0 {
		t.Fatalf("kept %d and stale %d names; want both outcomes exercised", counted[0], counted[1])
	}
}

func olderAndFree(name string, below uint64, held map[string]bool) bool {
	var epoch, position uint64
	read, err := fmt.Sscanf(name, "%d-%d", &epoch, &position)
	return err == nil && read == nameFields && epoch < below && !held[name]
}

func randomRuleRef(random *rand.Rand) *objectRef {
	return ruleRef(random.Uint64N(staleEpochs+1), random.Uint64N(staleNames))
}

func TestOnlyACompletedSweepThatRemovedNothingPausesTheNext(t *testing.T) {
	mature := pauseNow.Add(time.Second)
	tests := []struct {
		name string
		args sweepOutcome
		want time.Time
	}{
		{name: "an incomplete sweep", args: sweepOutcome{firstMature: mature}},
		{name: "a sweep that removed something", args: sweepOutcome{completed: true, removed: 1, firstMature: mature}},
		{name: "nothing removed and no orphan waits", args: sweepOutcome{completed: true},
			want: pauseNow.Add(pauseOrphanAge)},
		{name: "nothing removed until the oldest orphan matures", args: sweepOutcome{completed: true,
			firstMature: mature}, want: mature},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.args.pausedUntil(pauseNow, pauseOrphanAge)

			if !got.Equal(tt.want) {
				t.Errorf("paused until %s, want %s", got, tt.want)
			}
		})
	}
}

func TestTheFirstOrphanToMatureSetsThePause(t *testing.T) {
	var outcome sweepOutcome
	for _, offset := range []time.Duration{3 * time.Second, time.Second, 2 * time.Second} {
		outcome.matures(pauseNow.Add(offset))
	}

	if want := pauseNow.Add(time.Second); !outcome.firstMature.Equal(want) {
		t.Errorf("first mature %s, want %s", outcome.firstMature, want)
	}
}

func TestTheRetryBoundIsTheEarlierOfTheWindowAndTheWorstUpload(t *testing.T) {
	type args struct {
		window  time.Duration
		options *Options
	}
	bounded := &Options{WorstImage: boundWorst, UploadRate: boundRate}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{name: "a short window binds", args: args{window: shortWindow, options: bounded},
			want: shortWindow - chunkAttempt - uploadSlack},
		{name: "the worst upload binds", args: args{window: longWindow, options: bounded},
			want: time.Duration(boundWorst/boundRate)*time.Second + uploadSlack},
		{name: "the production window and the default worst image", args: args{window: dedupWindow,
			options: testOptions()}, want: transfer(testWorst, testUploadRate) + uploadSlack},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryWithin(tt.args.window, tt.args.options); got != tt.want {
				t.Errorf("retry bound %s, want %s", got, tt.want)
			}
		})
	}
}

func TestASweepGoesOnOnlyWithTheLeaseAndWithoutAStop(t *testing.T) {
	tests := []struct {
		name    string
		args    *uploader
		wantErr error
	}{
		{name: "the stop", args: &uploader{store: &Store{lease: &fakeLease{valid: true}}, stopping: closedStopping()},
			wantErr: errStopSweep},
		{name: "a lost lease", args: &uploader{store: &Store{lease: &fakeLease{}}, stopping: make(chan struct{})},
			wantErr: errSweepLeaseLost},
		{name: "a sweep that may go on", args: &uploader{store: &Store{lease: &fakeLease{valid: true}},
			stopping: make(chan struct{})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.args.sweepGoesOn(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("sweepGoesOn = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestOnlyASweepNotCutByTheStopCountsAsAFailure(t *testing.T) {
	tests := []struct {
		name string
		args error
		want uint64
	}{
		{name: "no error"},
		{name: "the goroutine sweep cut by the stop", args: fmt.Errorf("read meta: %w", errStopSweep)},
		{name: "the stake sweep cut by the stop", args: fmt.Errorf("purge: %w", errStopStake)},
		{name: "a suspended lease", args: fmt.Errorf("purge: %w", errSweepLeaseLost)},
		{name: "an unreadable meta", args: errUnreadableMeta, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stats counters

			stats.sweepEnded(tt.args)

			if got := stats.sweepFailures.Load(); got != tt.want {
				t.Errorf("sweep failures %d, want %d", got, tt.want)
			}
		})
	}
}

func TestASweepCutByTheStopOrTheLeaseEndsTheCycleWithItsCause(t *testing.T) {
	tests := []struct {
		name string
		args error
		want *cycleError
	}{
		{name: "the stop", args: errStopSweep, want: &cycleError{kind: kindStopped}},
		{name: "a lost lease", args: errSweepLeaseLost, want: &cycleError{kind: kindSkipped, skip: skipLeaseSuspended}},
		{name: "any other failure lets the cycle go on", args: errUnreadableMeta},
		{name: "no error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := errors.AsType[*cycleError](sweepCut(tt.args))

			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(cycleError{}),
				cmp.FilterPath(isCause, cmp.Ignore())); diff != "" {
				t.Errorf("cycle outcome (-want +got):\n%s", diff)
			}
		})
	}
}

func isCause(path cmp.Path) bool {
	return path.Last().String() == ".cause"
}

func TestTheScheduleSweepsWhenDueOrFullPastThePause(t *testing.T) {
	type args struct {
		schedule  sweepSchedule
		overflows bool
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "due after a full bucket, even with room", args: args{schedule: sweepSchedule{due: true,
			pausedUntil: pauseNow.Add(time.Hour)}}, want: true},
		{name: "full and the pause is over", args: args{schedule: sweepSchedule{pausedUntil: pauseNow.Add(-time.Nanosecond)},
			overflows: true}, want: true},
		{name: "full at the very end of the pause", args: args{schedule: sweepSchedule{pausedUntil: pauseNow},
			overflows: true}},
		{name: "room and nothing due", args: args{schedule: sweepSchedule{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.args.schedule.wanted(pauseNow, tt.args.overflows); got != tt.want {
				t.Errorf("wanted = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestOnlyACompletedSweepClearsTheDueSweep(t *testing.T) {
	tests := []struct {
		name string
		args sweepOutcome
		want sweepSchedule
	}{
		{name: "a cut sweep keeps it due", args: sweepOutcome{}, want: sweepSchedule{due: true}},
		{name: "a completed sweep that removed something", args: sweepOutcome{completed: true, removed: 1}},
		{name: "a completed empty sweep pauses", args: sweepOutcome{completed: true},
			want: sweepSchedule{pausedUntil: pauseNow.Add(pauseOrphanAge)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := sweepSchedule{}
			schedule.bucketFull()

			schedule.swept(&tt.args, pauseNow, pauseOrphanAge)

			if diff := cmp.Diff(tt.want, schedule, cmp.AllowUnexported(sweepSchedule{})); diff != "" {
				t.Errorf("schedule (-want +got):\n%s", diff)
			}
		})
	}
}

func TestOnlyAFullBucketMakesTheNextCycleSweep(t *testing.T) {
	for class := classOther; class < uploadClasses; class++ {
		up := &uploader{store: &Store{}}

		up.uploadFailed(class)

		if up.schedule.due != (class == classBucketFull) || up.store.stats.uploadFailed[class].Load() != 1 {
			t.Errorf("class %s: due %t, counted %d; want due only for a full bucket and the class counted",
				uploadClassNames[class], up.schedule.due, up.store.stats.uploadFailed[class].Load())
		}
	}
}

func TestASweepAfterTheStopIsCutAtOnceAndNotCounted(t *testing.T) {
	up := &uploader{store: &Store{lease: &fakeLease{valid: true}}, stopping: closedStopping()}

	removed, err := up.spaceSweep(t.Context())

	if outcome, _ := errors.AsType[*cycleError](err); removed || outcome == nil || outcome.kind != kindStopped ||
		up.store.stats.spaceSweeps.Load() != 0 || up.cancelSweep != nil {
		t.Errorf("spaceSweep = %t, %v, sweeps %d, registered %t; want a stop, no sweep counted, nothing registered",
			removed, err, up.store.stats.spaceSweeps.Load(), up.cancelSweep != nil)
	}
}

func TestTheStopCutsARegisteredSweep(t *testing.T) {
	up := &uploader{store: &Store{}, stopping: make(chan struct{})}
	ctx, cancel := up.stoppableSweep(t.Context())
	defer cancel()

	up.stop(time.Now().Add(stopWithin))

	if cause := context.Cause(ctx); !errors.Is(cause, errStopSweep) {
		t.Errorf("cause %v, want the sweep cut by the stop", cause)
	}
}

func TestOnlyTheErrorOfItsOwnContextMakesACutSweepAStop(t *testing.T) {
	type want struct {
		Stopped  bool
		Failures uint64
	}
	tests := []struct {
		name string
		args func(ctx context.Context) error
		want want
	}{
		{name: "the error of the sweep's own context", args: func(ctx context.Context) error {
			return fmt.Errorf("purge: %w", ctx.Err())
		}, want: want{Stopped: true}},
		{name: "a foreign error while the context is cut", args: func(context.Context) error {
			return fmt.Errorf("purge: %w", errInjected)
		}, want: want{Failures: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &uploader{store: &Store{now: time.Now}, stopping: closedStopping()}
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(errStopSweep)

			err := up.sweepFinished(ctx, &sweepOutcome{}, tt.args(ctx))

			outcome, _ := errors.AsType[*cycleError](err)
			got := want{Stopped: outcome != nil && outcome.kind == kindStopped,
				Failures: up.store.stats.sweepFailures.Load()}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("sweepFinished = %v (-want +got):\n%s", err, diff)
			}
		})
	}
}

func TestASweepIsNotCountedWhenTheLeaseIsSuspended(t *testing.T) {
	up := &uploader{store: &Store{lease: &fakeLease{}}, stopping: make(chan struct{})}

	removed, err := up.spaceSweep(t.Context())

	got, _ := errors.AsType[*cycleError](err)
	want := &cycleError{kind: kindSkipped, skip: skipLeaseSuspended}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(cycleError{}),
		cmp.FilterPath(isCause, cmp.Ignore())); diff != "" || removed || up.store.stats.spaceSweeps.Load() != 0 {
		t.Errorf("removed %t, sweeps %d, outcome (-want +got):\n%s", removed, up.store.stats.spaceSweeps.Load(), diff)
	}
}

func TestAnImageThatFillsTheBucketExactlyStillFits(t *testing.T) {
	type args struct {
		used uint64
		size int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{name: "the image fills the bucket to the last byte", args: args{used: bucketLimit - imageSize, size: imageSize}},
		{name: "one byte over the bucket", args: args{used: bucketLimit - imageSize + 1, size: imageSize},
			wantErr: errBucketFull},
		{name: "an empty bucket and an image of the whole bucket", args: args{size: bucketLimit}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := &uploader{store: &Store{options: Options{MaxBytes: bucketLimit}}}
			if err := up.roomFor(tt.args.used, tt.args.size); !errors.Is(err, tt.wantErr) {
				t.Errorf("roomFor = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

const (
	bucketLimit = 1 << 20
	imageSize   = 1 << 12
)
