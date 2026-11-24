package main

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
)

const (
	signalOrderRuns  = 500
	signalOrderPause = 20 * time.Microsecond
	abandonedStep    = 100 * time.Millisecond
	lateUnitExit     = 500 * time.Millisecond
	rollbackBudget   = 2 * time.Second
	signalLogDelay   = time.Second
)

func signalServed(t *testing.T, role *stack, logs *logBuffer) int {
	t.Helper()
	notify := newFakeNotify()
	done := launchRole(t, role, notify.notify, logs)
	synctest.Wait()
	notify.send()
	return <-done
}

func assertCode(t *testing.T, got, want int, logs *logBuffer) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("code mismatch (-want +got):\n%s\n%s", diff, logs.String())
	}
	if diff := cmp.Diff(want, stoppedCode(t, logs)); diff != "" {
		t.Fatalf("stopped line code mismatch (-want +got):\n%s", diff)
	}
}

func assertDiff(t *testing.T, what string, want, got any) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("%s mismatch (-want +got):\n%s", what, diff)
	}
}

func assertNoLine(t *testing.T, logs *logBuffer, event string) {
	t.Helper()
	if found := logs.all(t, event); len(found) != 0 {
		t.Errorf("%s lines = %v, want none", event, found)
	}
}

func assertErrors(t *testing.T, logs *logBuffer, want ...string) {
	t.Helper()
	assertDiff(t, "error lines", want, messages(errorLines(t, logs)))
}

func TestAServedStackStopsInReverseOnASignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events, logs := &eventLog{}, &logBuffer{}
		role := fakeStack(events, &fakeLayer{name: "a"}, &fakeLayer{name: "b"}, &fakeLayer{name: "c"})
		assertCode(t, signalServed(t, role, logs), exitOK, logs)
		synctest.Wait()
		assertDiff(t, "events", []string{"start a", "start b", "start c", "stop c", "stop b", "stop a"}, events.list())
		assertDiff(t, "timeline", []string{"starting", "serving", "draining", "stopped"}, logs.timeline(t))
		reasons := []map[string]any{
			{
				"state":  string(stateStarting),
				"reason": reasonLaunched,
			},
			{
				"state":  string(stateServing),
				"reason": reasonStarted,
			},
			{
				"state":  string(stateDraining),
				"reason": reasonSignal,
			},
			{
				"state":  string(stateStopped),
				"reason": reasonShutdown,
			},
		}
		assertDiff(t, "state reasons", reasons, picked(logs.all(t, eventState), "state", "reason"))
		assertDiff(t, "stop lines", []string{"layer_stopped c", "layer_stopped b", "layer_stopped a"}, stopLines(t, logs))
		assertDiff(t, "signal lines", []map[string]any{{"level": levelInfo}},
			picked(logs.all(t, eventSignal), "level", "state"))
		assertErrors(t, logs)
	})
}

func TestAFailedLayerIsStoppedWithTheStartedOnes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events, logs := &eventLog{}, &logBuffer{}
		role := fakeStack(events,
			&fakeLayer{name: "a"},
			&fakeLayer{
				name: "b",
				startErr: &stageError{
					stage: "b/step",
					err:   errB,
				},
			},
			&fakeLayer{name: "c"},
		)
		notify := newFakeNotify()
		assertCode(t, <-launchRole(t, role, notify.notify, logs), exitFailure, logs)
		assertDiff(t, "signals unregistered", true, notify.stops.Load() > 0)
		assertDiff(t, "events", []string{"start a", "start b", "stop b", "stop a"}, events.list())
		assertDiff(t, "timeline", []string{"starting", "aborting", "stopped"}, logs.timeline(t))
		assertDiff(t, "start_failed lines", []map[string]any{{"level": levelError, "stage": "b/step"}},
			picked(logs.all(t, eventStartFailed), "level", "stage"))
	})
}

func TestRollbackGetsTheFullBudgetAfterASignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		lower := &fakeLayer{
			name:   "a",
			budget: rollbackBudget,
		}
		waiting := &fakeLayer{
			name:         "b",
			starting:     startReturnsTheCause,
			stopWaitsCtx: true,
		}
		role := fakeStack(&eventLog{}, lower, waiting)
		assertCode(t, signalServed(t, role, logs), exitOK, logs)
		for _, current := range []*fakeLayer{waiting, lower} {
			entry := current.entered()
			if entry.err != nil {
				t.Errorf("stop of %s began with %v, want nil", current.name, entry.err)
			}
			assertDiff(t, "budget left at the stop of "+current.name, current.stopBudget(), entry.remaining)
		}
		want := []map[string]any{
			{"layer": "b", "elapsed_seconds": testBudget.Seconds()},
			{"layer": "a", "elapsed_seconds": float64(0)},
		}
		assertDiff(t, "layer_stopped lines", want, picked(logs.all(t, eventLayerStopped), "layer", "elapsed_seconds"))
		assertDiff(t, "start_canceled lines", []map[string]any{{"stage": "b"}},
			picked(logs.all(t, eventStartCanceled), "stage"))
	})
}

func TestATwoComponentLayerStopsCleanly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		role := fakeStack(
			&eventLog{},
			&fakeLayer{
				name:  "a",
				units: []fakeUnit{{name: "a/x"}, {name: "a/y"}},
			},
		)
		assertCode(t, signalServed(t, role, logs), exitOK, logs)
		assertNoLine(t, logs, eventComponentExit)
		assertNoLine(t, logs, eventLayerStopFailed)
	})
}

func TestASignalWhileAnyLayerStartsIsCodeZero(t *testing.T) {
	names := []string{"a", "b", "c"}
	tests := []struct {
		Name string
		Args int
		Want []string
	}{
		{
			Name: "a signal while the first layer starts stops only it",
			Args: 0,
			Want: []string{"start a", "stop a"},
		},
		{
			Name: "a signal while the middle layer starts stops it and the one below",
			Args: 1,
			Want: []string{"start a", "start b", "stop b", "stop a"},
		},
		{
			Name: "a signal while the last layer starts stops them all",
			Args: 2,
			Want: []string{"start a", "start b", "start c", "stop c", "stop b", "stop a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				events, logs := &eventLog{}, &logBuffer{}
				layers := make([]*fakeLayer, 0, len(names))
				for _, name := range names {
					layers = append(layers, &fakeLayer{name: name})
				}
				layers[tt.Args].starting = startReturnsTheCause
				assertCode(t, signalServed(t, fakeStack(events, layers...), logs), exitOK, logs)
				assertDiff(t, "events", tt.Want, events.list())
				assertDiff(t, "timeline", []string{"starting", "draining", "stopped"}, logs.timeline(t))
				assertDiff(t, "start_canceled lines", []map[string]any{{"stage": names[tt.Args]}},
					picked(logs.all(t, eventStartCanceled), "stage"))
			})
		})
	}
}

func TestASignalBeforeTheFirstLayerStartsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events, logs := &eventLog{}, &logBuffer{}
		role := fakeStack(events, &fakeLayer{name: "a"}, &fakeLayer{name: "b"})
		assertCode(t, <-launchRole(t, role, alreadySignalled, logs), exitOK, logs)
		assertDiff(t, "events", []string(nil), events.list())
		assertDiff(t, "start_canceled lines", []map[string]any{{"stage": stageStarting}},
			picked(logs.all(t, eventStartCanceled), "stage"))
		assertNoLine(t, logs, eventLayerStopped)
	})
}

func TestASignalAfterTheLastLayerNamesIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		logger := testLogger(logs)
		life := newLifecycle(logger)
		role := &stack{}
		last := &fakeLayer{
			name:     "last",
			starting: startIgnoresTheSignal,
			onStart:  notify.send,
		}
		role.layers = []layer{
			statsLayer(role.hooks, life.Served, selfalert.Thresholds{WatermarkLag: time.Minute}, logger),
			last.layer(&eventLog{}),
		}
		assertCode(t, <-launchRoleWith(t, role, notify.notify, life), exitOK, logs)
		assertDiff(t, "start_canceled lines", []map[string]any{{"stage": "last"}},
			picked(logs.all(t, eventStartCanceled), "stage"))
		assertDiff(t, "timeline", []string{"starting", "draining", "stopped"}, logs.timeline(t))
		assertDiff(t, "final reports", []map[string]any(nil), finalReports(t, logs))
	})
}

func TestALayerThatIgnoresTheSignalEndsTheStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events, logs := &eventLog{}, &logBuffer{}
		role := fakeStack(events,
			&fakeLayer{name: "a"},
			&fakeLayer{
				name:     "b",
				starting: startIgnoresTheSignal,
			},
			&fakeLayer{name: "c"},
		)
		assertCode(t, signalServed(t, role, logs), exitOK, logs)
		assertDiff(t, "events", []string{"start a", "start b", "stop b", "stop a"}, events.list())
		assertDiff(t, "start_canceled lines", []map[string]any{{"stage": "b"}},
			picked(logs.all(t, eventStartCanceled), "stage"))
	})
}

func failingOnRelease() *fakeLayer {
	return &fakeLayer{
		name: "a",
		units: []fakeUnit{
			{
				name: "a/run",
				err:  errA,
				exit: exitOnRelease,
			},
		},
	}
}

func TestAUnitFailureAbortsTheWaitingStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events, logs := &eventLog{}, &logBuffer{}
		lower := failingOnRelease()
		role := fakeStack(events,
			lower,
			&fakeLayer{
				name:     "b",
				starting: startReturnsTheCause,
				onStart:  lower.free,
			},
		)
		assertCode(t, <-launchRole(t, role, newFakeNotify().notify, logs), exitFailure, logs)
		synctest.Wait()
		assertDiff(t, "timeline", []string{"starting", "aborting", "stopped"}, logs.timeline(t))
		assertDiff(t, "error lines", []map[string]any{{"msg": eventComponentExit, "component": "a/run"}},
			picked(errorLines(t, logs), "msg", "component"))
		assertNoLine(t, logs, eventStartFailed)
		assertNoLine(t, logs, eventSignal)
		assertDiff(t, "events", []string{"start a", "start b", "stop b", "stop a"}, events.list())
	})
}

func TestAUnitExitKeepsAnIndependentStartFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		lower := failingOnRelease()
		role := fakeStack(&eventLog{},
			lower,
			&fakeLayer{
				name:       "b",
				onStart:    lower.free,
				startDelay: time.Second,
				startErr: &stageError{
					stage: "b/step",
					err:   errB,
				},
			},
		)
		assertCode(t, <-launchRole(t, role, newFakeNotify().notify, logs), exitFailure, logs)
		assertErrors(t, logs, eventComponentExit)
		want := []map[string]any{
			{"level": levelWarn, "stage": "b/step", "followup": true, "error": errB.Error()},
		}
		assertDiff(t, "start_failed lines", want, picked(logs.all(t, eventStartFailed), "level", "stage", "followup", "error"))
	})
}

func TestASignalAfterAUnitFailureKeepsCodeOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		lower := failingOnRelease()
		role := fakeStack(&eventLog{},
			lower,
			&fakeLayer{
				name: "b",
				onStart: func() {
					lower.free()
					notify.send()
				},
				startDelay: time.Second,
			},
		)
		assertCode(t, <-launchRole(t, role, notify.notify, logs), exitFailure, logs)
		synctest.Wait()
		assertDiff(t, "timeline", []string{"starting", "aborting", "stopped"}, logs.timeline(t))
		assertDiff(t, "signal lines", []map[string]any{{"level": levelInfo, "state": string(stateAborting)}},
			picked(logs.all(t, eventSignal), "level", "state"))
	})
}

func TestASignalPendingAtTheAbortIsLoggedAfterTheAbortingState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		lower := failingOnRelease()
		role := fakeStack(&eventLog{},
			lower,
			&fakeLayer{
				name: "b",
				onStart: func() {
					lower.free()
					notify.send()
				},
				startDelay: time.Second,
			},
		)
		life := newLifecycle(slowLineLogger(logs, eventState))
		assertCode(t, <-launchRoleWith(t, role, notify.notify, life), exitFailure, logs)
		want := []map[string]any{
			{
				"msg":   eventState,
				"state": string(stateAborting),
			},
			{
				"msg":   eventSignal,
				"state": string(stateAborting),
			},
		}
		assertDiff(t, "aborting and signal lines", want, abortingLines(t, logs))
	})
}

func abortingLines(t *testing.T, logs *logBuffer) []map[string]any {
	t.Helper()
	return lo.Filter(
		picked(logs.records(t), "msg", "state"),
		func(line map[string]any, _ int) bool {
			return line["state"] == string(stateAborting)
		},
	)
}

func TestASignalWhileAbortingIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		held := make(chan struct{})
		role := fakeStack(&eventLog{},
			&fakeLayer{
				name: "a",
				units: []fakeUnit{
					{
						name: "a/run",
						err:  errA,
						exit: exitAtOnce,
					},
				},
				stopHold: held,
			},
			&fakeLayer{
				name:     "b",
				starting: startReturnsTheCause,
			},
		)
		done := launchRole(t, role, notify.notify, logs)
		synctest.Wait()
		notify.send()
		synctest.Wait()
		if notify.stops.Load() == 0 {
			t.Error("a signal while aborting left the signals registered")
		}
		assertDiff(t, "signal lines", []map[string]any{{"level": levelInfo, "state": string(stateAborting)}},
			picked(logs.all(t, eventSignal), "level", "state"))
		close(held)
		assertCode(t, <-done, exitFailure, logs)
	})
}

type slowLineHandler struct {
	slog.Handler
	message string
}

func (h slowLineHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == h.message {
		time.Sleep(signalLogDelay)
	}
	return h.Handler.Handle(ctx, record)
}

func slowLineLogger(logs *logBuffer, message string) *slog.Logger {
	return slog.New(slowLineHandler{
		Handler: testLogger(logs).Handler(),
		message: message,
	})
}

func TestASignalWhileAbortingIsLoggedBeforeStopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		held := make(chan struct{})
		role := fakeStack(&eventLog{},
			&fakeLayer{
				name: "a",
				units: []fakeUnit{
					{
						name: "a/run",
						err:  errA,
						exit: exitAtOnce,
					},
				},
				stopHold: held,
			},
		)
		life := newLifecycle(slowLineLogger(logs, eventSignal))
		done := launchRoleWith(t, role, notify.notify, life)
		synctest.Wait()
		notify.send()
		synctest.Wait()
		close(held)
		assertCode(t, <-done, exitFailure, logs)
		time.Sleep(signalLogDelay)
		synctest.Wait()
		records := logs.records(t)
		last := picked(records[len(records)-1:], "msg", "state")
		assertDiff(t, "last line", []map[string]any{{"msg": eventState, "state": string(stateStopped)}}, last)
		assertDiff(t, "signal lines", 1, len(logs.all(t, eventSignal)))
	})
}

func TestAFailureOnTheLastStartStepIsNotServing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		logger := testLogger(logs)
		life := newLifecycle(logger)
		role := &stack{}
		last := &fakeLayer{
			name: "c",
			units: []fakeUnit{
				{
					name: "c/run",
					err:  errC,
					exit: exitAtOnce,
				},
			},
			startDelay: time.Millisecond,
		}
		role.layers = []layer{
			statsLayer(role.hooks, life.Served, selfalert.Thresholds{WatermarkLag: time.Minute}, logger),
			last.layer(&eventLog{}),
		}
		assertCode(t, <-launchRoleWith(t, role, newFakeNotify().notify, life), exitFailure, logs)
		if life.Served() {
			t.Error("a stack whose last unit failed during the start was served")
		}
		assertDiff(t, "timeline", []string{"starting", "aborting", "stopped"}, logs.timeline(t))
		assertDiff(t, "final reports", []map[string]any(nil), finalReports(t, logs))
	})
}

func freeAndSettle(current *fakeLayer) func() {
	return func() {
		current.free()
		time.Sleep(settleTime)
	}
}

func TestALateUnitFailureIsCountedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		lower := &fakeLayer{
			name: "a",
			units: []fakeUnit{
				{
					name: "a/run",
					err:  errA,
					exit: exitOnRelease,
				},
			},
		}
		upper := &fakeLayer{
			name:   "c",
			onStop: freeAndSettle(lower),
		}
		assertCode(t, signalServed(t, fakeStack(&eventLog{}, lower, upper), logs), exitFailure, logs)
		assertDiff(t, "timeline", []string{"starting", "serving", "draining", "stopped"}, logs.timeline(t))
		assertDiff(t, "signal lines", []map[string]any{{"level": levelInfo}},
			picked(logs.all(t, eventSignal), "level", "state"))
		assertErrors(t, logs, eventComponentExit)
		assertNoLine(t, logs, eventStopFailed)
	})
}

func TestALateExitAfterAFailedStartIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		lower := &fakeLayer{
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
			onStop: freeAndSettle(lower),
		}
		code := <-launchRole(t, fakeStack(&eventLog{}, lower, failing), newFakeNotify().notify, logs)
		assertCode(t, code, exitFailure, logs)
		assertErrors(t, logs, eventStartFailed)
		assertDiff(t, "component_exit lines", []map[string]any{{"level": levelWarn, "followup": true}},
			picked(logs.all(t, eventComponentExit), "level", "followup"))
	})
}

type stopFailureWant struct {
	Stops  []string
	Failed []map[string]any
}

func TestAStopFailureIsCodeOne(t *testing.T) {
	tests := []struct {
		Name string
		Args map[string]error
		Want stopFailureWant
	}{
		{
			Name: "the top layer fails its stop",
			Args: map[string]error{
				"c": errC,
			},
			Want: stopFailureWant{
				Stops: []string{"layer_stop_failed c", "layer_stopped b", "layer_stopped a"},
				Failed: []map[string]any{
					{"level": levelError, "layers": []any{"c"}, "error": "c: " + errC.Error()},
				},
			},
		},
		{
			Name: "the middle layer abandons a step",
			Args: map[string]error{
				"b": errStepAbandoned,
			},
			Want: stopFailureWant{
				Stops: []string{"layer_stopped c", "layer_stop_failed b", "layer_stopped a"},
				Failed: []map[string]any{
					{"level": levelError, "layers": []any{"b"}, "error": "b: " + errStepAbandoned.Error()},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &logBuffer{}
				role := fakeStack(&eventLog{},
					&fakeLayer{
						name:    "a",
						stopErr: tt.Args["a"],
					},
					&fakeLayer{
						name:    "b",
						stopErr: tt.Args["b"],
					},
					&fakeLayer{
						name:    "c",
						stopErr: tt.Args["c"],
					},
				)
				assertCode(t, signalServed(t, role, logs), exitFailure, logs)
				assertDiff(t, "stop lines", tt.Want.Stops, stopLines(t, logs))
				assertDiff(t, "stop_failed lines", tt.Want.Failed,
					picked(logs.all(t, eventStopFailed), "level", "layers", "error"))
			})
		})
	}
}

func TestAStuckLayerStopIsAbandonedAndNamed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		held := make(chan struct{})
		lower := &fakeLayer{name: "a"}
		role := fakeStack(
			&eventLog{},
			lower,
			&fakeLayer{
				name:     "b",
				stopHold: held,
			},
		)
		done := launchRole(t, role, notify.notify, logs)
		synctest.Wait()
		began := time.Now()
		notify.send()
		code := <-done
		close(held)
		synctest.Wait()
		assertCode(t, code, exitFailure, logs)
		assertDiff(t, "next layer stop after the signal", testBudget+exitGrace, lower.entered().at.Sub(began))
		want := []map[string]any{
			{"layer": "b", "elapsed_seconds": (testBudget + exitGrace).Seconds(), "error": errStopAbandoned.Error()},
		}
		assertDiff(t, "layer_stop_failed lines", want,
			picked(logs.all(t, eventLayerStopFailed), "layer", "elapsed_seconds", "error"))
		assertDiff(t, "stop_failed lines", []map[string]any{{"level": levelError, "layers": []any{"b"}}},
			picked(logs.all(t, eventStopFailed), "level", "layers"))
	})
}

func TestANameDeclaredByTwoLayersFailsTheStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events, logs := &eventLog{}, &logBuffer{}
		role := fakeStack(events,
			&fakeLayer{
				name:       "a",
				components: []string{"dup"},
			},
			&fakeLayer{
				name:       "b",
				components: []string{"dup"},
			},
		)
		assertCode(t, <-launchRole(t, role, newFakeNotify().notify, logs), exitFailure, logs)
		assertDiff(t, "events", []string(nil), events.list())
		want := []map[string]any{
			{"msg": eventStartFailed, "stage": stageStarting, "error": "dup: " + errDuplicateComponent.Error()},
		}
		assertDiff(t, "error lines", want, picked(errorLines(t, logs), "msg", "stage", "error"))
	})
}

func TestARespawnedNameIsAnUnexpectedExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		respawning := &fakeLayer{
			name:       "a",
			components: []string{"a/run"},
			units:      []fakeUnit{{name: "a/run"}, {name: "a/run"}},
			waitFor:    []string{"a/run"},
		}
		code := <-launchRole(t, fakeStack(&eventLog{}, respawning), newFakeNotify().notify, logs)
		synctest.Wait()
		assertCode(t, code, exitFailure, logs)
		want := []map[string]any{
			{"msg": eventComponentExit, "error": "a/run: component exited unexpectedly: " + errDuplicateComponent.Error()},
		}
		assertDiff(t, "error lines", want, picked(errorLines(t, logs), "msg", "error"))
		assertNoLine(t, logs, eventStopFailed)
		assertDiff(t, "a/run runs", int32(1), respawning.runs.Load())
	})
}

func TestAnExitErrorAfterStoppingIsReportedOnce(t *testing.T) {
	tests := []struct {
		Name string
		Args []string
		Want string
	}{
		{
			Name: "the layer returns the error it waited for",
			Args: []string{"a/run"},
			Want: "a: " + errA.Error(),
		},
		{
			Name: "the stack names the error the layer forgot to wait for",
			Args: nil,
			Want: "a: a/run: " + errA.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &logBuffer{}
				failing := &fakeLayer{
					name: "a",
					units: []fakeUnit{
						{
							name: "a/run",
							err:  errA,
						},
					},
					waitFor: tt.Args,
				}
				assertCode(t, signalServed(t, fakeStack(&eventLog{}, failing), logs), exitFailure, logs)
				want := []map[string]any{
					{"level": levelError, "state": string(stateDraining), "error": tt.Want},
				}
				assertDiff(t, "stop_failed lines", want,
					picked(logs.all(t, eventStopFailed), "level", "state", "error", "followup"))
				assertDiff(t, "stop lines", []string{"layer_stop_failed a"}, stopLines(t, logs))
				assertNoLine(t, logs, eventComponentExit)
			})
		})
	}
}

func TestAForgottenWaitDuringARollbackIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		role := fakeStack(&eventLog{},
			&fakeLayer{
				name: "x",
				units: []fakeUnit{
					{
						name: "x/run",
						err:  errX,
					},
				},
			},
			&fakeLayer{
				name: "c",
				startErr: &stageError{
					stage: "c/step",
					err:   errC,
				},
			},
		)
		assertCode(t, <-launchRole(t, role, newFakeNotify().notify, logs), exitFailure, logs)
		want := []map[string]any{
			{
				"level":    levelWarn,
				"followup": true,
				"state":    string(stateAborting),
				"error":    "x: x/run: " + errX.Error(),
			},
		}
		assertDiff(t, "stop_failed lines", want, picked(logs.all(t, eventStopFailed), "level", "followup", "state", "error"))
		assertErrors(t, logs, eventStartFailed)
	})
}

func TestAUnitExitAndAStopFailureLogOneError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		role := fakeStack(&eventLog{},
			&fakeLayer{
				name: "x",
				units: []fakeUnit{
					{
						name: "x/run",
						err:  errX,
					},
				},
			},
			&fakeLayer{
				name: "a",
				units: []fakeUnit{
					{
						name: "a/run",
						err:  errA,
						exit: exitAtOnce,
					},
				},
			},
		)
		assertCode(t, <-launchRole(t, role, newFakeNotify().notify, logs), exitFailure, logs)
		want := []map[string]any{
			{"level": levelWarn, "followup": true, "state": string(stateAborting)},
		}
		assertDiff(t, "stop_failed lines", want, picked(logs.all(t, eventStopFailed), "level", "followup", "state"))
		assertErrors(t, logs, eventComponentExit)
	})
}

func TestTwoFailedLayersOnACleanSignalLogOneError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		role := fakeStack(
			&eventLog{},
			&fakeLayer{
				name:    "a",
				stopErr: errA,
			},
			&fakeLayer{
				name:    "b",
				stopErr: errB,
			},
		)
		assertCode(t, signalServed(t, role, logs), exitFailure, logs)
		want := []map[string]any{
			{
				"level":  levelError,
				"layers": []any{"b", "a"},
				"error":  "b: " + errB.Error() + "\na: " + errA.Error(),
			},
		}
		assertDiff(t, "stop_failed lines", want, picked(logs.all(t, eventStopFailed), "level", "layers", "error", "followup"))
		assertErrors(t, logs, eventStopFailed)
	})
}

func TestAnIncidentDuringTheStopMakesTheStopFailureAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		lower := &fakeLayer{
			name: "a",
			units: []fakeUnit{
				{
					name: "a/run",
					err:  errA,
					exit: exitOnRelease,
				},
			},
		}
		upper := &fakeLayer{
			name:    "c",
			onStop:  freeAndSettle(lower),
			stopErr: errC,
		}
		assertCode(t, signalServed(t, fakeStack(&eventLog{}, lower, upper), logs), exitFailure, logs)
		want := []map[string]any{
			{
				"level":    levelWarn,
				"followup": true,
				"state":    string(stateDraining),
				"error":    "c: " + errC.Error(),
			},
		}
		assertDiff(t, "stop_failed lines", want, picked(logs.all(t, eventStopFailed), "level", "followup", "state", "error"))
		assertErrors(t, logs, eventComponentExit)
	})
}

func pauseFor(pause time.Duration) func() {
	return func() {
		time.Sleep(pause)
	}
}

func TestAnExitAfterAnAbandonedStepNamesItsOwnError(t *testing.T) {
	lateExit := fakeUnit{
		name:  "a/run",
		err:   errA,
		delay: lateUnitExit,
	}
	tests := []struct {
		Name string
		Args *fakeLayer
	}{
		{
			Name: "a step that gives up without waiting",
			Args: &fakeLayer{
				name:    "a",
				units:   []fakeUnit{lateExit},
				onStop:  pauseFor(abandonedStep),
				stopErr: errStepAbandoned,
			},
		},
		{
			Name: "a step that waits on a shorter deadline",
			Args: &fakeLayer{
				name:       "a",
				units:      []fakeUnit{lateExit},
				waitFor:    []string{"a/run"},
				waitBudget: abandonedStep,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &logBuffer{}
				assertCode(t, signalServed(t, fakeStack(&eventLog{}, tt.Args), logs), exitFailure, logs)
				want := []map[string]any{
					{
						"layer":           "a",
						"elapsed_seconds": lateUnitExit.Seconds(),
						"error":           errStepAbandoned.Error() + "\na/run: " + errA.Error(),
					},
				}
				assertDiff(t, "layer_stop_failed lines", want,
					picked(logs.all(t, eventLayerStopFailed), "layer", "elapsed_seconds", "error"))
			})
		})
	}
}

func TestAComponentOutlivingItsStopIsAStopFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		stuck := &fakeLayer{
			name: "a",
			units: []fakeUnit{
				{
					name: "a/run",
					exit: exitOnRelease,
				},
			},
		}
		code := signalServed(t, fakeStack(&eventLog{}, stuck), logs)
		stuck.free()
		synctest.Wait()
		assertCode(t, code, exitFailure, logs)
		want := []map[string]any{
			{"layer": "a", "elapsed_seconds": testBudget.Seconds(), "error": "a/run: " + errComponentOutlivedStop.Error()},
		}
		assertDiff(t, "layer_stop_failed lines", want,
			picked(logs.all(t, eventLayerStopFailed), "layer", "elapsed_seconds", "error"))
	})
}

type preparedTracing struct {
	*tracingUnit
}

func (p preparedTracing) start(context.Context, unitSupervisor) error {
	return nil
}

func TestAFailedTracingFlushIsCodeZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		traced := &tracingUnit{
			logger: testLogger(logs),
			tracing: &fakeTracing{
				events: &eventLog{},
				hang:   true,
			},
		}
		role := &stack{
			layers: []layer{
				{
					name:       layerTracing,
					unit:       preparedTracing{traced},
					stopBudget: traceFlush,
				},
			},
		}
		assertCode(t, signalServed(t, role, logs), exitOK, logs)
		assertDiff(t, "forced_stop lines", []map[string]any{{"level": levelWarn, "layer": layerTracing}},
			picked(logs.all(t, eventForcedStop), "level", "layer"))
		assertNoLine(t, logs, eventLayerStopFailed)
		assertNoLine(t, logs, eventStopFailed)
	})
}

func undeclaredExit(t *testing.T, role *stack) []map[string]any {
	t.Helper()
	logs := &logBuffer{}
	assertCode(t, <-launchRole(t, role, newFakeNotify().notify, logs), exitFailure, logs)
	assertNoLine(t, logs, eventStopFailed)
	return picked(logs.all(t, eventComponentExit), "level", "error")
}

func TestAnUndeclaredSpawnFailsTheStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spawning := &fakeLayer{
			name:       "a",
			components: []string{},
			units:      []fakeUnit{{name: "z/run"}},
		}
		got := undeclaredExit(t, fakeStack(&eventLog{}, spawning))
		want := []map[string]any{
			{"level": levelError, "error": "z/run: component exited unexpectedly: layer a: " + errUndeclaredComponent.Error()},
		}
		assertDiff(t, "component_exit lines", want, got)
		assertDiff(t, "undeclared runs", int32(0), spawning.runs.Load())
	})
}

func TestAnUndeclaredSpawnNamesItsStackLayer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spawning := &fakeLayer{
			name:       "a",
			components: []string{"a/run"},
			units:      []fakeUnit{{name: "z/run"}},
		}
		got := undeclaredExit(t, fakeStack(&eventLog{}, spawning))
		want := []map[string]any{
			{"level": levelError, "error": "z/run: component exited unexpectedly: layer a: " + errUndeclaredComponent.Error()},
		}
		assertDiff(t, "component_exit lines", want, got)
	})
}

func TestAnUndeclaredSpawnOfATakenNameIsUndeclared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		role := fakeStack(&eventLog{},
			&fakeLayer{
				name:    "a",
				units:   []fakeUnit{{name: "a/run"}},
				waitFor: []string{"a/run"},
			},
			&fakeLayer{
				name:       "b",
				components: []string{},
				units:      []fakeUnit{{name: "a/run"}},
			},
		)
		want := []map[string]any{
			{"level": levelError, "error": "a/run: component exited unexpectedly: layer b: " + errUndeclaredComponent.Error()},
		}
		assertDiff(t, "component_exit lines", want, undeclaredExit(t, role))
	})
}

func TestWaitingForAnUndeclaredNameIsAStopFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		role := fakeStack(
			&eventLog{},
			&fakeLayer{
				name:    "a",
				waitFor: []string{"misspelled"},
			},
		)
		assertCode(t, signalServed(t, role, logs), exitFailure, logs)
		want := []map[string]any{
			{"level": levelError, "error": "a: wait misspelled: " + errUndeclaredComponent.Error()},
		}
		assertDiff(t, "stop_failed lines", want, picked(logs.all(t, eventStopFailed), "level", "error"))
	})
}

type drainingProbe struct {
	logs        *logBuffer
	notify      *fakeNotify
	atDraining  atomic.Int32
	sawDraining atomic.Bool
}

func (p *drainingProbe) Write(line []byte) (int, error) {
	if strings.Contains(string(line), `"state":"draining"`) {
		p.atDraining.Store(p.notify.stops.Load())
		p.sawDraining.Store(true)
	}
	return p.logs.Write(line)
}

func TestTheSignalIsUnregisteredBeforeTheStackStops(t *testing.T) {
	for run := range signalOrderRuns {
		notify := newFakeNotify()
		probe := &drainingProbe{
			logs:   &logBuffer{},
			notify: notify,
		}
		var atStop atomic.Int32
		first := &fakeLayer{
			name: "a",
			onStop: func() {
				atStop.Store(notify.stops.Load())
			},
		}
		life := newLifecycle(slog.New(slog.NewJSONHandler(probe, nil)))
		done := launchRoleWith(t, fakeStack(&eventLog{}, first), notify.notify, life)
		for !life.Ready() {
			time.Sleep(signalOrderPause)
		}
		notify.send()
		<-done
		if atStop.Load() == 0 || !probe.sawDraining.Load() || probe.atDraining.Load() == 0 {
			t.Fatalf("run %d: the stack stopped with the signals still registered (stop: %d, draining: %d)",
				run, atStop.Load(), probe.atDraining.Load())
		}
	}
}

type valueLayer struct {
	value atomic.Int64
}

func (v *valueLayer) start(context.Context, unitSupervisor) error {
	v.value.Store(1)
	return nil
}

func (v *valueLayer) stop(context.Context) error {
	v.value.Store(2)
	return nil
}

func TestTheFinalReportCarriesAStoppedLayersValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		logger := testLogger(logs)
		life := newLifecycle(logger)
		role := &stack{}
		upper := &valueLayer{}
		role.layers = []layer{
			statsLayer(role.hooks, life.Served, selfalert.Thresholds{WatermarkLag: time.Minute}, logger),
			{
				name: "upper",
				unit: upper,
				statsGroups: func(context.Context) []slog.Attr {
					return []slog.Attr{slog.Int64("upper", upper.value.Load())}
				},
				sample: func(context.Context, *selfalert.Sample) {
					upper.value.Load()
				},
				stopBudget: testBudget,
			},
		}
		done := launchRoleWith(t, role, notify.notify, life)
		time.Sleep(2 * statsInterval)
		notify.send()
		assertCode(t, <-done, exitOK, logs)
		finals := picked(finalReports(t, logs), "upper")
		assertDiff(t, "final report", []map[string]any{{"upper": float64(2)}}, finals)
	})
}

func finalReports(t *testing.T, logs *logBuffer) []map[string]any {
	t.Helper()
	var finals []map[string]any
	for _, record := range logs.all(t, eventPipelineStats) {
		if record["final"] == true {
			finals = append(finals, record)
		}
	}
	return finals
}
