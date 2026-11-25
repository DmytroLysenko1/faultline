package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const leaseStress = 20000

var errStakeLease = &stageError{
	stage: "p0/stake",
	err:   fmt.Errorf("stake: %w", processing.ErrLeaseLost),
}

func checkLeaseLine(t *testing.T, logs *logBuffer, level string, followup bool) {
	t.Helper()
	lines := logs.all(t, eventLeaseLost)
	if len(lines) != 1 {
		t.Fatalf("lease.lost lines %d\n%s", len(lines), logs.String())
	}
	line := lines[0]
	text, _ := line["error"].(string)
	if line["level"] != level || (line["followup"] == true) != followup || line["partition"] != "p0" ||
		!strings.Contains(text, processing.ErrLeaseLost.Error()) {
		t.Fatalf("lease.lost %v", line)
	}
}

func checkOneError(t *testing.T, logs *logBuffer, msg string) {
	t.Helper()
	found := errorLines(t, logs)
	if len(found) != 1 || found[0]["msg"] != msg {
		t.Fatalf("errors %v\n%s", found, logs.String())
	}
}

type exitsThenLosesLease struct{}

func (exitsThenLosesLease) start(ctx context.Context, sup unitSupervisor) error {
	sup.spawn("a/run", func() error { return errA })
	<-ctx.Done()
	return errStakeLease
}

func (exitsThenLosesLease) stop(context.Context) error { return nil }

func TestAUnitExitThenAStakeLeaseLossLogsOneError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		role := &stack{layers: []layer{
			{
				name: "p0",
				unit: &leaseUnit{
					waits: true,
				},
				components: []string{"p0/lease"},
				stopBudget: time.Second,
			},
			{
				name:       "a",
				unit:       exitsThenLosesLease{},
				components: []string{"a/run"},
				stopBudget: time.Second,
			},
		}}
		if code := <-launchRole(t, role, signal.notify, logs); code != 1 {
			t.Fatalf("code %d", code)
		}
		checkOneError(t, logs, eventComponentExit)
		checkLeaseLine(t, logs, "WARN", true)
		if got := logs.event(t, eventLeaseLost)["stage"]; got != "p0/stake" {
			t.Fatalf("stage %v", got)
		}
	})
}

type exitsThenReleasesLease struct {
	lease chan struct{}
}

func (u *exitsThenReleasesLease) start(ctx context.Context, sup unitSupervisor) error {
	sup.spawn("a/run", func() error { return errA })
	<-ctx.Done()
	close(u.lease)
	time.Sleep(10 * time.Millisecond)
	return ctx.Err()
}

func (u *exitsThenReleasesLease) stop(context.Context) error { return nil }

func TestALeaseExitAfterAUnitExitBeforeTheSnapshotIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		trigger := make(chan struct{})
		role := &stack{layers: []layer{
			{
				name: "p0",
				unit: &leaseUnit{
					trigger: trigger,
				},
				components: []string{"p0/lease"},
				stopBudget: time.Second,
			},
			{
				name: "a",
				unit: &exitsThenReleasesLease{
					lease: trigger,
				},
				components: []string{"a/run"},
				stopBudget: time.Second,
			},
		}}
		if code := <-launchRole(t, role, signal.notify, logs); code != 1 {
			t.Fatalf("code %d", code)
		}
		checkOneError(t, logs, eventComponentExit)
		checkLeaseLine(t, logs, "WARN", true)
		checkLeaseStage(t, logs, "a")
	})
}

func TestTwoExitsBeforeTheSnapshotLogOneErrorAndOneLeaseLine(t *testing.T) {
	forEachScheduler(t, func(t *testing.T) {
		firsts := map[string]int{}
		for run := range leaseStress {
			firsts[twoExitsRun(t, run)]++
		}
		t.Logf("first exits %v", firsts)
		if runtime.GOMAXPROCS(0) > 1 && (firsts["a/run"] == 0 || firsts["p0/lease"] == 0) {
			t.Errorf("first exits %v, want both orders met", firsts)
		}
	})
}

func twoExitsRun(t *testing.T, run int) string {
	logs := &logBuffer{}
	ctx, fail := context.WithCancelCause(t.Context())
	sup := newSupervisor(fail, testLogger(logs))
	gate := make(chan struct{})
	sup.spawn("a/run", func() error { <-gate; return errA })
	sup.spawn("p0/lease", func() error { <-gate; return errLeaseGone })
	close(gate)
	<-ctx.Done()
	<-sup.units["a/run"].done
	<-sup.units["p0/lease"].done
	outcome := sup.snapshot(func(unit error) exitOutcome {
		return outcomeOf(&observed{
			stage: "a",
			cause: context.Cause(ctx),
			unit:  unit,
		})
	})
	logger := testLogger(logs)
	logOutcome(ctx, logger, &outcome)
	late := lateOutcome(&outcome, sup.late(), leaseLossIn(sup.lostLease(), "a", nil), sup.claimLeaseIncident)
	logOutcome(ctx, logger, &late)
	errs := errorLines(t, logs)
	lines := logs.all(t, eventLeaseLost)
	if len(errs) != 1 || len(lines) != 1 || lines[0]["partition"] != "p0" || lines[0]["stage"] != "a" ||
		sup.incident != incidentWritten {
		t.Fatalf("run %d: errors %d, lease.lost %v, mark %d\n%s", run, len(errs), lines, sup.incident, logs.String())
	}
	first, _ := errors.AsType[*unitExitError](sup.first)
	return first.name
}

func TestALeaseLostWhileDrainingIsCodeOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		trigger := make(chan struct{})
		role := leaseStack(&leaseUnit{
			trigger: trigger,
		}, &triggersThenFails{
			trigger: trigger,
		})
		done := launchRole(t, role, signal.notify, logs)
		synctest.Wait()
		signal.send()
		if code := <-done; code != 1 {
			t.Fatalf("code %d\n%s", code, logs.String())
		}
		checkOneError(t, logs, eventLeaseLost)
		checkLeaseLine(t, logs, "ERROR", false)
		checkLeaseStage(t, logs, stageStopping)
		if failed := logs.all(t, eventStopFailed); len(failed) != 0 {
			t.Fatalf("stop_failed %v", failed)
		}
	})
}

type releasesInOrder struct {
	first  chan struct{}
	second chan struct{}
}

func (u *releasesInOrder) start(context.Context, unitSupervisor) error { return nil }

func (u *releasesInOrder) stop(context.Context) error {
	close(u.first)
	time.Sleep(10 * time.Millisecond)
	close(u.second)
	time.Sleep(10 * time.Millisecond)
	return nil
}

func TestALateLeaseExitAfterALateUnitExitIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		lease, unit := make(chan struct{}), &exitsOnTrigger{
			trigger: make(chan struct{}),
		}
		role := &stack{layers: []layer{
			{
				name: "p0",
				unit: &leaseUnit{
					trigger: lease,
				},
				components: []string{"p0/lease"},
				stopBudget: time.Second,
			},
			{
				name:       "a",
				unit:       unit,
				components: []string{"a/run"},
				stopBudget: time.Second,
			},
			{
				name: "c",
				unit: &releasesInOrder{
					first:  unit.trigger,
					second: lease,
				},
				stopBudget: time.Second,
			},
		}}
		done := launchRole(t, role, signal.notify, logs)
		synctest.Wait()
		signal.send()
		if code := <-done; code != 1 {
			t.Fatalf("code %d", code)
		}
		checkOneError(t, logs, eventComponentExit)
		checkLeaseLine(t, logs, "WARN", true)
		checkLeaseStage(t, logs, stageStopping)
	})
}

func TestALateLossAfterRuleOneWritesNothing(t *testing.T) {
	first := outcomeOf(&observed{
		err:   errStakeLease,
		stage: "p0",
	})
	lateExit := &unitExitError{
		name: "p0/lease",
		err:  errLeaseGone,
	}
	late := lateOutcome(&first, lateExit, leaseLossIn(keptLeaseExit{
		err: lateExit,
	}, "p0", nil), func() bool {
		t.Fatal("owes asked after rule 1")
		return false
	})
	if diff := cmp.Diff(projected{
		code: exitFailure,
	}, project(late), cmp.AllowUnexported(projected{})); diff != "" {
		t.Errorf("late (-want +got):\n%s", diff)
	}
}

type projected struct {
	code   int
	state  lifecycleState
	reason string
	level  slog.Level
	event  string
	attrs  string
}

func project(outcome exitOutcome) projected {
	parts := make([]string, 0, len(outcome.attrs))
	for _, attr := range outcome.attrs {
		parts = append(parts, attr.Key+"="+attr.Value.String())
	}
	return projected{
		code:   outcome.code,
		state:  outcome.state,
		reason: outcome.reason,
		level:  outcome.level,
		event:  outcome.event,
		attrs:  strings.Join(parts, " "),
	}
}

func TestOutcomeOfLeaseRows(t *testing.T) {
	leaseExit := &unitExitError{
		name: "p0/lease",
		err:  errLeaseGone,
	}
	unitExit := &unitExitError{
		name: "a/run",
		err:  errA,
	}
	awaitLease := &stageError{
		stage: "journal/await_streams",
		err:   fmt.Errorf("await streams: %w", leaseExit),
	}
	resumeLease := errors.Join(&stageError{
		stage: "p0/resume",
		err:   errStakeLease.err,
	}, unitExit)
	independentStep := &stageError{
		stage: "a/step",
		err:   errB,
	}
	canceledStep := fmt.Errorf("a step: %w", context.Canceled)
	tests := []struct {
		name string
		seen observed
		want projected
	}{
		{"a lease exit is rule 1", observed{
			stage: "serving",
			cause: leaseExit,
			unit:  leaseExit,
		},
			projected{exitFailure, stateAborting, reasonLeaseLost, slog.LevelError, eventLeaseLost,
				"stage=serving partition=p0 error=" + leaseExit.Error()}},
		{"a lease step error is rule 1", observed{
			stage: "p0",
			err:   errStakeLease,
		},
			projected{exitFailure, stateAborting, reasonLeaseLost, slog.LevelError, eventLeaseLost,
				"stage=p0/stake partition=p0 error=" + errStakeLease.Error()}},
		{"a lease step error after a unit exit is a followup",
			observed{
				stage: "a",
				err:   errStakeLease,
				cause: unitExit,
				unit:  unitExit,
			},
			projected{exitFailure, stateAborting, reasonComponentExit, slog.LevelWarn, eventLeaseLost,
				"stage=p0/stake partition=p0 error=" + errStakeLease.Error() + " followup=true"}},
		{"a lease step error joined with the unit exit is a followup",
			observed{
				stage: "a",
				err:   errors.Join(errStakeLease, unitExit),
				cause: unitExit,
				unit:  unitExit,
			},
			projected{exitFailure, stateAborting, reasonComponentExit, slog.LevelWarn, eventLeaseLost,
				"stage=p0/stake partition=p0 error=" + errors.Join(errStakeLease, unitExit).Error() + " followup=true"}},
		{"a later layer's wait carrying the lease exit names the lease's partition",
			observed{
				stage: "journal",
				err:   awaitLease,
				cause: leaseExit,
				unit:  unitExit,
			},
			projected{exitFailure, stateAborting, reasonComponentExit, slog.LevelWarn, eventLeaseLost,
				"stage=journal/await_streams partition=p0 error=" + awaitLease.Error() + " followup=true"}},
		{"a resume lease loss joined with the unit exit names the partition, not the unit",
			observed{
				stage: "p0",
				err:   resumeLease,
				cause: unitExit,
				unit:  unitExit,
			},
			projected{exitFailure, stateAborting, reasonComponentExit, slog.LevelWarn, eventLeaseLost,
				"stage=p0/resume partition=p0 error=" + resumeLease.Error() + " followup=true"}},
		{"a lease cause after a unit exit is rule 2", observed{
			stage: "a",
			cause: leaseExit,
			unit:  unitExit,
		},
			projected{
				code:   exitFailure,
				state:  stateAborting,
				reason: reasonComponentExit,
			}},
		{"an independent step failure next to a lease exit is kept on the lease line",
			observed{
				stage: "a",
				err:   independentStep,
				cause: leaseExit,
				unit:  leaseExit,
			},
			projected{exitFailure, stateAborting, reasonLeaseLost, slog.LevelError, eventLeaseLost,
				"stage=a/step partition=p0 error=" + leaseExit.Error() + " start_error=" + independentStep.Error()}},
		{"a canceled step next to a lease exit adds no start error",
			observed{
				stage: "a",
				err:   canceledStep,
				cause: leaseExit,
				unit:  leaseExit,
			},
			projected{exitFailure, stateAborting, reasonLeaseLost, slog.LevelError, eventLeaseLost,
				"stage=a partition=p0 error=" + leaseExit.Error()}},
		{"a lease step error after a lease exit is rule 1",
			observed{
				stage: "a",
				err:   errStakeLease,
				cause: leaseExit,
				unit:  leaseExit,
			},
			projected{exitFailure, stateAborting, reasonLeaseLost, slog.LevelError, eventLeaseLost,
				"stage=p0/stake partition=p0 error=" + leaseExit.Error()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, project(outcomeOf(&tt.seen)), cmp.AllowUnexported(projected{})); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
		})
	}
}

func checkLeaseStage(t *testing.T, logs *logBuffer, stage string) {
	t.Helper()
	if got := logs.event(t, eventLeaseLost)["stage"]; got != stage {
		t.Fatalf("lease.lost stage %v, want %s\n%s", got, stage, logs.String())
	}
}
