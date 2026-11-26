package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	eventStartFailed   = "lifecycle.start_failed"
	eventStartCanceled = "lifecycle.start_canceled"
	eventSignal        = "lifecycle.signal"
	eventLeaseLost     = "lease.lost"
	eventRestoreFailed = "checkpoint.restore_failed"
	reasonLeaseLost    = "lease_lost"
	stageStopping      = "stopping"
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
		{
			applies: leaseLost,
			outcome: leaseOutcome,
		},
		{
			applies: leaseLostAfterExit,
			outcome: leaseFollowupOutcome,
		},
		{
			applies: unitExited,
			outcome: unitOutcome,
		},
		{
			applies: startFailed,
			outcome: startFailureOutcome,
		},
		{
			applies: stoppedBySignal,
			outcome: signalOutcome,
		},
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
	outcome := failureOutcome(seen, reasonStartFailed, seen.err)
	failure, found := errors.AsType[*restoreError](seen.err)
	if !found {
		return outcome
	}
	outcome.event = eventRestoreFailed
	outcome.attrs = restoreAttrs(stageOf(seen), failure, seen.err)
	return outcome
}

func restoreAttrs(stage string, failure *restoreError, err error) []slog.Attr {
	return append([]slog.Attr{
		slog.String("stage", stage),
		causeAfterStage(stage, err),
		slog.Bool("discardable", failure.discardable),
		slog.Int("attempts", failure.attempts),
	}, hintAttrs(failure.hint)...)
}

func causeAfterStage(stage string, err error) slog.Attr {
	cause := errorAfterStage(stage, err)
	cause.Key = "cause"
	return cause
}

func hintAttrs(hint string) []slog.Attr {
	if hint == "" {
		return nil
	}
	return []slog.Attr{slog.String("hint", hint)}
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
		attrs:  append(stageAttrs(seen, err), hintAttrs(stageHint(err))...),
	}
}

func stageHint(err error) string {
	failure, found := errors.AsType[*stageError](err)
	if !found {
		return ""
	}
	return failure.hint
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

type leaseLoss struct {
	stage     string
	partition string
	err       error
}

func leaseLost(seen *observed) bool {
	return errors.Is(lossOf(seen), processing.ErrLeaseLost)
}

func leaseOutcome(seen *observed) exitOutcome {
	loss := lossOf(seen)
	attrs := leaseAttrs(stageOf(seen), partitionOf(loss), loss)
	if seen.unit != nil && independentStartFailure(seen) && !carriesLeaseLoss(seen.err) {
		attrs = append(attrs, slog.String("start_error", seen.err.Error()))
	}
	return exitOutcome{
		code:   exitFailure,
		state:  stateAborting,
		reason: reasonLeaseLost,
		level:  slog.LevelError,
		event:  eventLeaseLost,
		attrs:  attrs,
	}
}

func lossOf(seen *observed) error {
	if seen.unit != nil {
		return seen.unit
	}
	return seen.err
}

func leaseLostAfterExit(seen *observed) bool {
	return seen.unit != nil && errors.Is(seen.err, processing.ErrLeaseLost)
}

func leaseFollowupOutcome(seen *observed) exitOutcome {
	return exitOutcome{
		code:   exitFailure,
		state:  stateAborting,
		reason: reasonComponentExit,
		level:  slog.LevelWarn,
		event:  eventLeaseLost,
		attrs:  append(leaseAttrs(stageOf(seen), partitionOf(seen.err), seen.err), slog.Bool("followup", true)),
	}
}

func leaseAttrs(stage, partition string, loss error) []slog.Attr {
	return []slog.Attr{
		slog.String("stage", stage),
		slog.String("partition", partition),
		slog.Any("error", loss),
	}
}

type leaseSource interface {
	sourceName() string
}

func (e *stageError) sourceName() string {
	return e.stage
}

func (e *unitExitError) sourceName() string {
	return e.name
}

func partitionOf(loss error) string {
	return layerOf(nearestSourceOf(loss))
}

func nearestSourceOf(err error) string {
	if inner := nearestSourceIn(unwrapped(err)); inner != "" {
		return inner
	}
	if source, named := err.(leaseSource); named {
		return source.sourceName()
	}
	return ""
}

func nearestSourceIn(wrapped []error) string {
	index := slices.IndexFunc(wrapped, carriesLeaseLoss)
	if index < 0 {
		return ""
	}
	return nearestSourceOf(wrapped[index])
}

func unwrapped(err error) []error {
	if joined, found := err.(interface{ Unwrap() []error }); found {
		return joined.Unwrap()
	}
	if inner := errors.Unwrap(err); inner != nil {
		return []error{inner}
	}
	return nil
}

func carriesLeaseLoss(err error) bool {
	return errors.Is(err, processing.ErrLeaseLost)
}

func layerOf(name string) string {
	layer, _, _ := strings.Cut(name, "/")
	return layer
}

func lateOutcome(first *exitOutcome, late error, loss *leaseLoss, claimIncident func() bool) exitOutcome {
	if first.event == eventLeaseLost || loss == nil {
		return lateExitOutcome(late)
	}
	return leaseLostOutcome(loss, claimIncident())
}

func leaseLossIn(exit keptLeaseExit, snapshotStage string, failures []stopFailure) *leaseLoss {
	if exit.err != nil {
		return &leaseLoss{
			stage:     exit.stage(snapshotStage),
			partition: partitionOf(exit.err),
			err:       exit.err,
		}
	}
	index := slices.IndexFunc(failures, lostItsLease)
	if index < 0 {
		return nil
	}
	return &leaseLoss{
		stage:     stageStopping,
		partition: failures[index].layer,
		err:       failureCause(failures[index], index),
	}
}

func (exit keptLeaseExit) stage(snapshotStage string) string {
	if exit.beforeSnapshot {
		return snapshotStage
	}
	return stageStopping
}

func lostItsLease(failure stopFailure) bool {
	return carriesLeaseLoss(failure.err)
}

func leaseLostOutcome(loss *leaseLoss, owed bool) exitOutcome {
	outcome := exitOutcome{
		code:  exitFailure,
		level: slog.LevelError,
		event: eventLeaseLost,
		attrs: leaseAttrs(loss.stage, loss.partition, loss.err),
	}
	if owed {
		return outcome
	}
	outcome.level = slog.LevelWarn
	outcome.attrs = append(outcome.attrs, slog.Bool("followup", true))
	return outcome
}

func lateExitOutcome(late error) exitOutcome {
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
