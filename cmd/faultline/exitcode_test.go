package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

type outcomeView struct {
	Code     int
	State    lifecycleState
	Reason   string
	Level    slog.Level
	Event    string
	Stage    string
	Followup bool
}

func viewOf(outcome exitOutcome) outcomeView {
	view := outcomeView{
		Code:   outcome.code,
		State:  outcome.state,
		Reason: outcome.reason,
		Level:  outcome.level,
		Event:  outcome.event,
	}
	for _, attr := range outcome.attrs {
		switch attr.Key {
		case "stage":
			view.Stage = attr.Value.String()
		case "followup":
			view.Followup = attr.Value.Bool()
		}
	}
	return view
}

func TestOutcomeOf(t *testing.T) {
	exit := &unitExitError{
		name: "a/run",
		err:  errA,
	}
	drained := outcomeView{
		Code:   exitOK,
		State:  stateDraining,
		Reason: reasonSignal,
		Level:  slog.LevelInfo,
		Event:  eventStartCanceled,
		Stage:  "b",
	}
	aborted := outcomeView{
		Code:   exitFailure,
		State:  stateAborting,
		Reason: reasonComponentExit,
	}
	failedAtB := outcomeView{
		Code:   exitFailure,
		State:  stateAborting,
		Reason: reasonStartFailed,
		Level:  slog.LevelError,
		Event:  eventStartFailed,
		Stage:  "b",
	}
	tests := []struct {
		Name string
		Args observed
		Want outcomeView
	}{
		{
			Name: "a signal while serving drains with code 0",
			Args: observed{
				stage:  stageServing,
				cause:  errTestSignal,
				signal: errTestSignal,
			},
			Want: outcomeView{
				Code:   exitOK,
				State:  stateDraining,
				Reason: reasonSignal,
				Level:  slog.LevelInfo,
				Event:  eventSignal,
			},
		},
		{
			Name: "a signal during a start stage names the stage",
			Args: observed{
				stage:  "conn",
				cause:  errTestSignal,
				signal: errTestSignal,
			},
			Want: outcomeView{
				Code:   exitOK,
				State:  stateDraining,
				Reason: reasonSignal,
				Level:  slog.LevelInfo,
				Event:  eventStartCanceled,
				Stage:  "conn",
			},
		},
		{
			Name: "a unit exit aborts with code 1 and leaves the line to the supervisor",
			Args: observed{
				stage: "b",
				cause: exit,
				unit:  exit,
			},
			Want: aborted,
		},
		{
			Name: "a unit exit wins over a later signal",
			Args: observed{
				stage:  "b",
				cause:  exit,
				signal: errTestSignal,
				unit:   exit,
			},
			Want: aborted,
		},
		{
			Name: "a start step returning the unit exit adds no line",
			Args: observed{
				err:   exit,
				stage: "b",
				cause: exit,
				unit:  exit,
			},
			Want: aborted,
		},
		{
			Name: "a start step canceled by the unit exit adds no line",
			Args: observed{
				err:   context.Canceled,
				stage: "b",
				cause: exit,
				unit:  exit,
			},
			Want: aborted,
		},
		{
			Name: "an independent start failure next to a unit exit is a followup",
			Args: observed{
				err: &stageError{
					stage: "b/step",
					err:   errB,
				},
				stage: "b",
				cause: exit,
				unit:  exit,
			},
			Want: outcomeView{
				Code:     exitFailure,
				State:    stateAborting,
				Reason:   reasonComponentExit,
				Level:    slog.LevelWarn,
				Event:    eventStartFailed,
				Stage:    "b/step",
				Followup: true,
			},
		},
		{
			Name: "a failed start step names its stage",
			Args: observed{
				err: &stageError{
					stage: stageHTTPListen,
					err:   errB,
				},
				stage: layerHTTP,
			},
			Want: outcomeView{
				Code:   exitFailure,
				State:  stateAborting,
				Reason: reasonStartFailed,
				Level:  slog.LevelError,
				Event:  eventStartFailed,
				Stage:  stageHTTPListen,
			},
		},
		{
			Name: "a start step canceled by the signal is code 0",
			Args: observed{
				err:    context.Canceled,
				stage:  "b",
				cause:  errTestSignal,
				signal: errTestSignal,
			},
			Want: drained,
		},
		{
			Name: "a moved checkpoint joined with the signal is code 0",
			Args: observed{
				err:    errors.Join(processing.ErrCheckpointMoved, errTestSignal),
				stage:  "b",
				cause:  errTestSignal,
				signal: errTestSignal,
			},
			Want: drained,
		},
		{
			Name: "a step deadline during a signal is a start failure",
			Args: observed{
				err:    context.DeadlineExceeded,
				stage:  "b",
				cause:  errTestSignal,
				signal: errTestSignal,
			},
			Want: failedAtB,
		},
		{
			Name: "a cancel without a signal is a start failure",
			Args: observed{
				err:   context.Canceled,
				stage: "b",
			},
			Want: failedAtB,
		},
		{
			Name: "a signal that did not end the start is unexplained",
			Args: observed{
				stage:  "b",
				signal: errTestSignal,
			},
			Want: outcomeView{
				Code:   exitFailure,
				State:  stateAborting,
				Reason: reasonUnexplained,
				Level:  slog.LevelError,
				Event:  eventStartFailed,
				Stage:  "b",
			},
		},
		{
			Name: "a stop nobody explains is code 1",
			Args: observed{stage: "b"},
			Want: outcomeView{
				Code:   exitFailure,
				State:  stateAborting,
				Reason: reasonUnexplained,
				Level:  slog.LevelError,
				Event:  eventStartFailed,
				Stage:  "b",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, viewOf(outcomeOf(&tt.Args))); diff != "" {
				t.Errorf("outcome mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLateOutcome(t *testing.T) {
	tests := []struct {
		Name string
		Args error
		Want int
	}{
		{
			Name: "no late exit keeps code 0",
			Args: nil,
			Want: exitOK,
		},
		{
			Name: "a late exit is code 1",
			Args: &unitExitError{
				name: "a/run",
				err:  errA,
			},
			Want: exitFailure,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, lateOutcome(&exitOutcome{}, tt.Args, nil, nil).code); diff != "" {
				t.Errorf("code mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCodeOf(t *testing.T) {
	tests := []struct {
		Name string
		Args []stopFailure
		Want int
	}{
		{
			Name: "a clean stop is code 0",
			Args: nil,
			Want: exitOK,
		},
		{
			Name: "one failed layer stop is code 1",
			Args: []stopFailure{
				{
					layer: "a",
					err:   errA,
				},
			},
			Want: exitFailure,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, codeOf(tt.Args)); diff != "" {
				t.Errorf("code mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheStageOfAWrappedStageErrorWins(t *testing.T) {
	seen := &observed{
		err: errors.Join(
			errX,
			&stageError{
				stage: "conn/dial",
				err:   errB,
			},
		),
		stage: "conn",
	}
	if diff := cmp.Diff("conn/dial", stageOf(seen)); diff != "" {
		t.Errorf("stage mismatch (-want +got):\n%s", diff)
	}
}

func TestAnOutcomeWithoutAnEventWritesNoLine(t *testing.T) {
	logs := &logBuffer{}
	logOutcome(t.Context(), testLogger(logs), &exitOutcome{code: exitFailure})
	if diff := cmp.Diff("", logs.String()); diff != "" {
		t.Errorf("logged (-want +got):\n%s", diff)
	}
}

func errorAttrOf(outcome exitOutcome) any {
	for _, attr := range outcome.attrs {
		if attr.Key == "error" {
			return attr.Value.Any()
		}
	}
	return "no error attr"
}

func TestTheStartFailureLogsTheStagesOwnError(t *testing.T) {
	tests := []struct {
		Name string
		Args observed
		Want any
	}{
		{
			Name: "a stage error logs only its own error",
			Args: observed{
				err: &stageError{
					stage: "b/step",
					err:   errB,
				},
				stage: "b",
			},
			Want: errB.Error(),
		},
		{
			Name: "a stage error joined with another keeps the other",
			Args: observed{
				err: errors.Join(
					&stageError{
						stage: "b/step",
						err:   errB,
					},
					errC,
				),
				stage: "b",
			},
			Want: errB.Error() + "\n" + errC.Error(),
		},
		{
			Name: "a stage error behind another keeps its stage in the text",
			Args: observed{
				err: errors.Join(
					errX,
					&stageError{
						stage: "b/step",
						err:   errB,
					},
				),
				stage: "b",
			},
			Want: errX.Error() + "\nb/step: " + errB.Error(),
		},
		{
			Name: "an error that only starts like the stage keeps its text",
			Args: observed{
				err:   fmt.Errorf("b: %w", errB),
				stage: "b",
			},
			Want: "b: " + errB.Error(),
		},
		{
			Name: "a cause that only starts like the stage keeps its text",
			Args: observed{
				err: &stageError{
					stage: "b/step",
					err:   context.Canceled,
				},
				signal: errX,
				cause: fmt.Errorf(
					"b/step: %w",
					&stageError{
						stage: "a",
						err:   errA,
					},
				),
				stage: "b",
			},
			Want: "b/step: a: " + errA.Error(),
		},
		{
			Name: "an unexplained stop names its cause",
			Args: observed{
				stage: "b",
				cause: errA,
			},
			Want: errA.Error(),
		},
		{
			Name: "an unexplained stop without a cause logs no error",
			Args: observed{
				stage: "b",
			},
			Want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, errorAttrOf(outcomeOf(&tt.Args))); diff != "" {
				t.Errorf("error attr mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
