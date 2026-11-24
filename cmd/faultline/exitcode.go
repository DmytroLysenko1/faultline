package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
)

const (
	eventStartFailed   = "lifecycle.start_failed"
	eventStartCanceled = "lifecycle.start_canceled"
	eventSignal        = "lifecycle.signal"
)

type observed struct {
	err    error
	stage  string
	cause  error
	signal error
	unit   error
}

type exitOutcome struct {
	code   int
	state  lifecycleState
	reason string
	level  slog.Level
	event  string
	attrs  []slog.Attr
}

type exitRule struct {
	applies func(seen *observed) bool
	outcome func(seen *observed) exitOutcome
}

func exitRules() []exitRule {
	return []exitRule{
		{applies: unitExited, outcome: unitOutcome},
		{applies: startFailed, outcome: startFailureOutcome},
		{applies: stoppedBySignal, outcome: signalOutcome},
	}
}

func outcomeOf(seen *observed) exitOutcome {
	for _, rule := range exitRules() {
		if rule.applies(seen) {
			return rule.outcome(seen)
		}
	}
	return unexplainedOutcome(seen)
}

func unitExited(seen *observed) bool {
	return seen.unit != nil
}

func startFailed(seen *observed) bool {
	if seen.err == nil {
		return false
	}
	return seen.signal == nil ||
		!errors.Is(seen.err, context.Canceled)
}

func stoppedBySignal(seen *observed) bool {
	return seen.signal != nil &&
		errors.Is(seen.cause, seen.signal)
}

func unitOutcome(seen *observed) exitOutcome {
	outcome := exitOutcome{
		code:   exitFailure,
		state:  stateAborting,
		reason: reasonComponentExit,
	}
	if !independentStartFailure(seen) {
		return outcome
	}
	outcome.level = slog.LevelWarn
	outcome.event = eventStartFailed
	outcome.attrs = append(stageAttrs(seen, seen.err), slog.Bool("followup", true))
	return outcome
}

func independentStartFailure(seen *observed) bool {
	if seen.err == nil {
		return false
	}
	return !errors.Is(seen.err, context.Canceled) &&
		!errors.Is(seen.err, seen.cause)
}

func startFailureOutcome(seen *observed) exitOutcome {
	return failureOutcome(seen, reasonStartFailed, seen.err)
}

func unexplainedOutcome(seen *observed) exitOutcome {
	return failureOutcome(seen, reasonUnexplained, seen.cause)
}

func failureOutcome(seen *observed, reason string, err error) exitOutcome {
	return exitOutcome{
		code:   exitFailure,
		state:  stateAborting,
		reason: reason,
		level:  slog.LevelError,
		event:  eventStartFailed,
		attrs:  stageAttrs(seen, err),
	}
}

func stageAttrs(seen *observed, err error) []slog.Attr {
	stage := stageOf(seen)
	return []slog.Attr{
		slog.String("stage", stage),
		errorAfterStage(stage, err),
	}
}

func errorAfterStage(stage string, err error) slog.Attr {
	if err == nil {
		return slog.Any("error", err)
	}
	failure, found := errors.AsType[*stageError](err)
	if !found || failure.stage != stage {
		return slog.String("error", err.Error())
	}
	message := strings.TrimPrefix(err.Error(), stage+": ")
	return slog.String("error", message)
}

func signalOutcome(seen *observed) exitOutcome {
	outcome := exitOutcome{
		code:   exitOK,
		state:  stateDraining,
		reason: reasonSignal,
		level:  slog.LevelInfo,
		event:  eventSignal,
	}
	if seen.stage == stageServing {
		return outcome
	}
	outcome.event = eventStartCanceled
	outcome.attrs = []slog.Attr{slog.String("stage", stageOf(seen))}
	return outcome
}

func stageOf(seen *observed) string {
	if failure, found := errors.AsType[*stageError](seen.err); found {
		return failure.stage
	}
	return seen.stage
}

func lateOutcome(late error) exitOutcome {
	if late == nil {
		return exitOutcome{}
	}
	return exitOutcome{
		code: exitFailure,
	}
}

func codeOf(failures []stopFailure) int {
	if len(failures) == 0 {
		return exitOK
	}
	return exitFailure
}

func logOutcome(ctx context.Context, logger *slog.Logger, outcome *exitOutcome) {
	if outcome.event == "" {
		return
	}
	logger.LogAttrs(
		context.WithoutCancel(ctx),
		outcome.level, outcome.event, outcome.attrs...,
	)
}
