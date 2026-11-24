package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	stressRuns       = 2000
	concurrentRuns   = 1000
	raceJitters      = 8
	branchDecide     = "decide"
	branchLate       = "late"
	decisionPause    = 20 * time.Millisecond
	shortWait        = 100 * time.Millisecond
	lateLineInterval = time.Millisecond
)

type supervision struct {
	sup    *supervisor
	failed <-chan struct{}
	cause  func() error
	logs   *logBuffer
}

func newTestSupervisor(t *testing.T) supervision {
	t.Helper()
	logs := &logBuffer{}
	startCtx, failStart := context.WithCancelCause(t.Context())
	t.Cleanup(func() {
		failStart(nil)
	})
	return supervision{
		sup:    newSupervisor(failStart, testLogger(logs)),
		failed: startCtx.Done(),
		cause: func() error {
			return context.Cause(startCtx)
		},
		logs: logs,
	}
}

func exitNow(sup *supervisor, name string, err error) {
	sup.spawn(name, func() error {
		return err
	})
	synctest.Wait()
}

func spawnGated(sup *supervisor, name string, gate <-chan struct{}, err error) {
	sup.spawn(name, func() error {
		<-gate
		return err
	})
}

func decided(sup *supervisor, level slog.Level) error {
	var seen error
	sup.snapshot(func(unit error) exitOutcome {
		seen = unit
		return exitOutcome{
			level: level,
		}
	})
	return seen
}

func TestUnknownNamesAreNoOps(t *testing.T) {
	under := newTestSupervisor(t)
	under.sup.stopping("missing")
	exited, err := under.sup.wait(t.Context(), "missing")
	if !exited || err != nil {
		t.Fatalf("wait(missing) = (%v, %v), want (true, nil)", exited, err)
	}
	if err := under.sup.outlived(t.Context(), "missing"); err != nil {
		t.Fatalf("outlived(missing) = %v, want nil", err)
	}
	if seen := decided(under.sup, slog.LevelInfo); seen != nil {
		t.Fatalf("decide saw %v, want nil", seen)
	}
}

func TestOnlyTheFirstUnexpectedExitIsAnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		exitNow(under.sup, "a", errA)
		exitNow(under.sup, "b", errB)
		want := []map[string]any{
			{"level": levelError, "component": "a"},
			{"level": levelWarn, "component": "b", "followup": true},
		}
		got := picked(under.logs.all(t, eventComponentExit), "level", "component", "followup")
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("component_exit lines mismatch (-want +got):\n%s", diff)
		}
		cause := under.cause()
		exit, found := errors.AsType[*unitExitError](cause)
		if !found || exit.name != "a" || !errors.Is(cause, errA) {
			t.Fatalf("start cause = %v, want the exit of a", cause)
		}
	})
}

func TestALostLeaseExitIsLeftToTheOutcome(t *testing.T) {
	lost := fmt.Errorf("renew: %w", processing.ErrLeaseLost)
	tests := []struct {
		Name string
		Args []string
		Want []map[string]any
	}{
		{
			Name: "the lease exit alone logs no error",
			Args: []string{
				"p0/lease",
			},
			Want: []map[string]any{
				{"level": levelWarn, "component": "p0/lease", "followup": true},
			},
		},
		{
			Name: "an exit after the lease exit is a followup",
			Args: []string{
				"p0/lease",
				"b",
			},
			Want: []map[string]any{
				{"level": levelWarn, "component": "p0/lease", "followup": true},
				{"level": levelWarn, "component": "b", "followup": true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				under := newTestSupervisor(t)
				exitNow(under.sup, tt.Args[0], lost)
				for _, name := range tt.Args[1:] {
					exitNow(under.sup, name, errB)
				}
				got := picked(under.logs.all(t, eventComponentExit), "level", "component", "followup")
				if diff := cmp.Diff(tt.Want, got); diff != "" {
					t.Errorf("component_exit lines mismatch (-want +got):\n%s", diff)
				}
				if found := errorLines(t, under.logs); len(found) != 0 {
					t.Errorf("error lines = %v, want none", found)
				}
				if !under.sup.incidentOpened() {
					t.Error("the lease exit opened no incident")
				}
				if seen := decided(under.sup, slog.LevelInfo); !errors.Is(seen, processing.ErrLeaseLost) {
					t.Errorf("decide saw %v, want the lost lease", seen)
				}
			})
		})
	}
}

func TestANilExitBeforeStoppingIsUnexpected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		exitNow(under.sup, "a", nil)
		cause := under.cause()
		if !errors.Is(cause, errUnexpectedExit) {
			t.Fatalf("start cause = %v, want %v", cause, errUnexpectedExit)
		}
		if got, want := cause.Error(), "a: component exited unexpectedly"; got != want {
			t.Errorf("start cause = %q, want %q", got, want)
		}
	})
}

func TestAnErrorAfterStoppingIsAStopFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		gate := make(chan struct{})
		spawnGated(under.sup, "a", gate, errA)
		under.sup.stopping("a")
		close(gate)
		for attempt := range 2 {
			exited, err := under.sup.wait(t.Context(), "a")
			if !exited || !errors.Is(err, errA) {
				t.Fatalf("wait %d = (%v, %v), want (true, %v)", attempt, exited, err, errA)
			}
		}
		if seen := decided(under.sup, slog.LevelInfo); seen != nil {
			t.Errorf("decide saw %v, want nil", seen)
		}
		if late := under.sup.late(); late != nil {
			t.Errorf("late = %v, want nil", late)
		}
		if exits := under.logs.all(t, eventComponentExit); len(exits) != 0 {
			t.Errorf("component_exit lines = %v, want none", exits)
		}
	})
}

func TestAnExitBeforeStoppingIsLeftToTheOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		exitNow(under.sup, "a", errA)
		under.sup.stopping("a")
		exited, err := under.sup.wait(t.Context(), "a")
		if !exited || err != nil {
			t.Fatalf("wait = (%v, %v), want (true, nil)", exited, err)
		}
		if err := under.sup.outlived(t.Context(), "a"); err != nil {
			t.Fatalf("outlived = %v, want nil", err)
		}
		if seen := decided(under.sup, slog.LevelInfo); !errors.Is(seen, errA) {
			t.Errorf("decide saw %v, want %v", seen, errA)
		}
	})
}

type outlivedUnit struct {
	Name  string
	Err   error
	Hangs bool
}

type outlivedArgs struct {
	Units   []outlivedUnit
	Claimed bool
}

type outlivedWant struct {
	Text    string
	Is      []error
	Elapsed time.Duration
}

func TestOutlivedReportsWhatWaitDidNotClaim(t *testing.T) {
	tests := []struct {
		Name string
		Args outlivedArgs
		Want outlivedWant
	}{
		{
			Name: "an exit error the layer waited for is not reported again",
			Args: outlivedArgs{
				Units: []outlivedUnit{
					{
						Name: "a",
						Err:  errA,
					},
				},
				Claimed: true,
			},
		},
		{
			Name: "an exit error the layer did not wait for is named",
			Args: outlivedArgs{
				Units: []outlivedUnit{
					{
						Name: "a",
						Err:  errA,
					},
				},
			},
			Want: outlivedWant{
				Text: "a: a failed",
				Is:   []error{errA},
			},
		},
		{
			Name: "a unit that never exits outlives the stop at its deadline",
			Args: outlivedArgs{
				Units: []outlivedUnit{
					{
						Name:  "a",
						Hangs: true,
					},
				},
			},
			Want: outlivedWant{
				Text:    "a: " + errComponentOutlivedStop.Error(),
				Is:      []error{errComponentOutlivedStop},
				Elapsed: testBudget,
			},
		},
		{
			Name: "a clean exit is no failure",
			Args: outlivedArgs{
				Units: []outlivedUnit{{Name: "a"}},
			},
		},
		{
			Name: "every unclaimed exit error is named",
			Args: outlivedArgs{
				Units: []outlivedUnit{
					{
						Name: "a",
						Err:  errA,
					},
					{
						Name: "b",
						Err:  errB,
					},
				},
			},
			Want: outlivedWant{
				Text: "a: a failed\nb: b failed",
				Is:   []error{errA, errB},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				got, err := outlivedAfter(t, tt.Args)
				for _, target := range tt.Want.Is {
					if !errors.Is(err, target) {
						t.Errorf("outlived = %v, want it to carry %v", err, target)
					}
				}
				if diff := cmp.Diff(tt.Want.Text, errorText(err)); diff != "" {
					t.Errorf("outlived text mismatch (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(tt.Want.Elapsed, got); diff != "" {
					t.Errorf("outlived elapsed mismatch (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func outlivedAfter(t *testing.T, args outlivedArgs) (time.Duration, error) {
	t.Helper()
	under := newTestSupervisor(t)
	gate, hold := make(chan struct{}), make(chan struct{})
	names := make([]string, 0, len(args.Units))
	for _, current := range args.Units {
		names = append(names, current.Name)
		spawnGated(under.sup, current.Name, gateOf(current, gate, hold), current.Err)
	}
	under.sup.stopping(names...)
	close(gate)
	synctest.Wait()
	claimAll(t, under.sup, args.Claimed, names)
	ctx, cancel := context.WithTimeout(t.Context(), testBudget)
	defer cancel()
	began := time.Now()
	err := under.sup.outlived(ctx, names...)
	elapsed := time.Since(began)
	close(hold)
	synctest.Wait()
	return elapsed, err
}

func gateOf(current outlivedUnit, gate, hold chan struct{}) chan struct{} {
	if current.Hangs {
		return hold
	}
	return gate
}

func claimAll(t *testing.T, sup *supervisor, claimed bool, names []string) {
	t.Helper()
	if !claimed {
		return
	}
	for _, name := range names {
		if exited, _ := sup.wait(t.Context(), name); !exited {
			t.Fatalf("wait(%s) found no exit", name)
		}
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestATakenNameIsADuplicateExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		gate := make(chan struct{})
		spawnGated(under.sup, "a", gate, nil)
		var secondRan atomic.Bool
		under.sup.spawn("a", func() error {
			secondRan.Store(true)
			return nil
		})
		synctest.Wait()
		if secondRan.Load() {
			t.Fatal("the duplicate run started")
		}
		if cause := under.cause(); !errors.Is(cause, errDuplicateComponent) {
			t.Fatalf("start cause = %v, want %v", cause, errDuplicateComponent)
		}
		under.sup.stopping("a")
		close(gate)
		if exited, err := under.sup.wait(t.Context(), "a"); !exited || err != nil {
			t.Fatalf("wait = (%v, %v), want (true, nil)", exited, err)
		}
		want := []map[string]any{
			{"level": levelError, "component": "a", "error": "a: component exited unexpectedly: " +
				errDuplicateComponent.Error()},
		}
		got := picked(under.logs.all(t, eventComponentExit), "level", "component", "error")
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("component_exit lines mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestAnUndeclaredSpawnLeavesTheNameFree(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		var undeclaredRan atomic.Bool
		layerSupervisor{
			sup:        under.sup,
			layer:      "a",
			components: []string{"a/run"},
		}.spawn("z/run", func() error {
			undeclaredRan.Store(true)
			return nil
		})
		ran, quit := make(chan struct{}), make(chan struct{})
		layerSupervisor{
			sup:        under.sup,
			layer:      "z",
			components: []string{"z/run"},
		}.spawn("z/run", func() error {
			close(ran)
			<-quit
			return errZ
		})
		synctest.Wait()
		if undeclaredRan.Load() {
			t.Fatal("the undeclared run started")
		}
		select {
		case <-ran:
		default:
			t.Fatal("the declared z/run did not start")
		}
		under.sup.stopping("z/run")
		close(quit)
		if exited, err := under.sup.wait(t.Context(), "z/run"); !exited || !errors.Is(err, errZ) {
			t.Fatalf("wait = (%v, %v), want (true, %v)", exited, err, errZ)
		}
		want := []string{
			"z/run: component exited unexpectedly: layer a: " + errUndeclaredComponent.Error(),
		}
		got := errorTexts(under.logs.all(t, eventComponentExit))
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("component_exit errors mismatch (-want +got):\n%s", diff)
		}
	})
}

func errorTexts(records []map[string]any) []string {
	texts := make([]string, 0, len(records))
	for _, record := range records {
		text, _ := record["error"].(string)
		texts = append(texts, text)
	}
	return texts
}

type firstExitWant struct {
	Now  bool
	Late bool
}

func TestTheFirstExitIsSeenEitherNowOrLate(t *testing.T) {
	tests := []struct {
		Name string
		Args bool
		Want firstExitWant
	}{
		{
			Name: "exits before the snapshot reach the outcome",
			Args: false,
			Want: firstExitWant{
				Now: true,
			},
		},
		{
			Name: "exits after the snapshot are late",
			Args: true,
			Want: firstExitWant{
				Late: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				under := newTestSupervisor(t)
				now, late := firstExitSeen(under.sup, tt.Args)
				assertOnlyTheFirst(t, "decide", now, tt.Want.Now)
				assertOnlyTheFirst(t, "late", late, tt.Want.Late)
				want := []map[string]any{
					{"level": levelError, "component": "a"},
					{"level": levelWarn, "component": "b", "followup": true},
				}
				got := picked(under.logs.all(t, eventComponentExit), "level", "component", "followup")
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("component_exit lines mismatch (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func assertOnlyTheFirst(t *testing.T, side string, err error, seen bool) {
	t.Helper()
	if !seen {
		if err != nil {
			t.Errorf("%s = %v, want nil", side, err)
		}
		return
	}
	if !errors.Is(err, errA) || errors.Is(err, errB) {
		t.Errorf("%s = %v, want only %v", side, err, errA)
	}
}

func firstExitSeen(sup *supervisor, snapshotFirst bool) (error, error) {
	var now error
	if snapshotFirst {
		now = decided(sup, slog.LevelInfo)
	}
	exitNow(sup, "a", errA)
	exitNow(sup, "b", errB)
	if !snapshotFirst {
		now = decided(sup, slog.LevelInfo)
	}
	return now, sup.late()
}

func TestTheFirstExitRacingTheSnapshotIsSeenOnce(t *testing.T) {
	branches := map[string]int{}
	for run := range concurrentRuns {
		seen, late := raceTheSnapshot(t, run%raceJitters)
		if (seen != nil) == (late != nil) {
			t.Fatalf("run %d: decide saw %v and late is %v, want exactly one", run, seen, late)
		}
		branches[branchOf(seen)]++
	}
	if branches[branchDecide] == 0 || branches[branchLate] == 0 {
		t.Errorf("branches = %v, want both %s and %s hit", branches, branchDecide, branchLate)
	}
}

func raceTheSnapshot(t *testing.T, jitter int) (error, error) {
	t.Helper()
	under := newTestSupervisor(t)
	gate := make(chan struct{})
	spawnGated(under.sup, "a", gate, errA)
	close(gate)
	for range jitter {
		runtime.Gosched()
	}
	seen := decided(under.sup, slog.LevelInfo)
	if exited, _ := under.sup.wait(t.Context(), "a"); !exited {
		t.Fatal("a did not exit")
	}
	return seen, under.sup.late()
}

func branchOf(seen error) string {
	if seen != nil {
		return branchDecide
	}
	return branchLate
}

func TestAnExitAfterAnErrorOutcomeIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		if seen := decided(under.sup, slog.LevelError); seen != nil {
			t.Fatalf("decide saw %v before any exit", seen)
		}
		exitNow(under.sup, "a", errA)
		want := []map[string]any{
			{"level": levelWarn, "component": "a", "followup": true},
		}
		got := picked(under.logs.all(t, eventComponentExit), "level", "component", "followup")
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("component_exit lines mismatch (-want +got):\n%s", diff)
		}
		if found := errorLines(t, under.logs); len(found) != 0 {
			t.Errorf("error lines = %v, want none", found)
		}
		if late := under.sup.late(); !errors.Is(late, errA) {
			t.Errorf("late = %v, want %v", late, errA)
		}
	})
}

func TestAnExitWhileTheOutcomeIsDecidedIsAFollowup(t *testing.T) {
	under := newTestSupervisor(t)
	gate := make(chan struct{})
	spawnGated(under.sup, "x/run", gate, errX)
	outcome := under.sup.snapshot(func(unit error) exitOutcome {
		close(gate)
		time.Sleep(decisionPause)
		return outcomeOf(&observed{
			err: &stageError{
				stage: "c/step",
				err:   errC,
			},
			stage: "c",
			unit:  unit,
		})
	})
	<-under.failed
	if outcome.level != slog.LevelError {
		t.Fatalf("outcome level = %v, want %v", outcome.level, slog.LevelError)
	}
	want := []map[string]any{
		{"level": levelWarn, "component": "x/run", "followup": true},
	}
	got := picked(under.logs.all(t, eventComponentExit), "level", "component", "followup")
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("component_exit lines mismatch (-want +got):\n%s", diff)
	}
	if late := under.sup.late(); !errors.Is(late, errX) {
		t.Errorf("late = %v, want %v", late, errX)
	}
}

func correlatedRollback() *stack {
	exiting := &fakeLayer{
		name: "x",
		units: []fakeUnit{
			{
				name: "x/run",
				err:  errX,
				exit: exitOnRelease,
			},
		},
	}
	failing := &fakeLayer{
		name: "c",
		startErr: &stageError{
			stage: "c/step",
			err:   errC,
		},
		onStart: exiting.free,
	}
	return fakeStack(&eventLog{}, exiting, failing)
}

func TestACorrelatedRollbackLogsOneError(t *testing.T) {
	for _, procs := range []int{runtime.GOMAXPROCS(0), 1} {
		t.Run(fmt.Sprintf("GOMAXPROCS=%d", procs), func(t *testing.T) {
			defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(procs))
			for run := range stressRuns {
				logs := &logBuffer{}
				code := launchStack(t.Context(), newFakeNotify().notify, correlatedRollback(), newLifecycle(testLogger(logs)))
				if diff := cmp.Diff(exitFailure, code); diff != "" {
					t.Fatalf("run %d: code mismatch (-want +got):\n%s", run, diff)
				}
				if found := errorLines(t, logs); len(found) != 1 {
					t.Fatalf("run %d: error lines = %v, want one", run, messages(found))
				}
			}
		})
	}
}

func TestNoLineIsWrittenAfterLaunchStackReturns(t *testing.T) {
	for run := range stressRuns {
		logs := &logBuffer{}
		launchStack(t.Context(), newFakeNotify().notify, correlatedRollback(), newLifecycle(testLogger(logs)))
		returned := len(logs.records(t))
		time.Sleep(lateLineInterval)
		if diff := cmp.Diff(returned, len(logs.records(t))); diff != "" {
			t.Fatalf("run %d: lines at return and a moment later (-want +got):\n%s\n%s", run, diff, logs.String())
		}
	}
}

func TestSupervisorWaitPrefersAnExitOverAnExpiredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		gate := make(chan struct{})
		spawnGated(under.sup, "a", gate, nil)
		under.sup.stopping("a")
		close(gate)
		synctest.Wait()
		expired, cancel := context.WithCancel(t.Context())
		cancel()
		for attempt := range waitAttempts {
			if exited, _ := under.sup.wait(expired, "a"); !exited {
				t.Fatalf("attempt %d: wait reported a unit that already exited as stuck", attempt)
			}
		}
	})
}

func TestWaitGivesUpAtItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		under := newTestSupervisor(t)
		gate := make(chan struct{})
		spawnGated(under.sup, "a", gate, nil)
		under.sup.stopping("a")
		ctx, cancel := context.WithTimeout(t.Context(), shortWait)
		defer cancel()
		began := time.Now()
		exited, err := under.sup.wait(ctx, "a")
		elapsed := time.Since(began)
		close(gate)
		synctest.Wait()
		if exited || err != nil {
			t.Fatalf("wait = (%v, %v), want (false, nil)", exited, err)
		}
		if diff := cmp.Diff(shortWait, elapsed); diff != "" {
			t.Errorf("wait gave up after (-want +got):\n%s", diff)
		}
	})
}
