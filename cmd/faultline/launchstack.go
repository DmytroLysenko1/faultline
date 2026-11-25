package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/samber/lo"
)

const eventStopFailed = "lifecycle.stop_failed"

type notifyFunc func(parent context.Context) (context.Context, context.CancelFunc)

func signalNotify(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

func unwatched() {}

type stopReport struct {
	state    lifecycleState
	followup bool
}

func launchStack(
	parent context.Context,
	notify notifyFunc,
	role *stack,
	life *lifecycle,
) int {
	signalCtx, stopSignals := notify(parent)
	defer stopSignals()
	context.AfterFunc(signalCtx, stopSignals)
	startCtx, failStart := context.WithCancelCause(signalCtx)
	defer failStart(nil)
	sup := newSupervisor(failStart, life.logger)
	outcome := new(startAndServe(startCtx, signalCtx, role, sup, life))
	if signalCtx.Err() != nil {
		stopSignals()
	}
	return stopStack(signalCtx, role, sup, life, outcome)
}

func stopStack(
	signalCtx context.Context,
	role *stack,
	sup *supervisor,
	life *lifecycle,
	outcome *exitOutcome,
) int {
	settle(signalCtx, outcome, life)
	unwatch := watchAbortSignal(signalCtx, life.logger, outcome)
	failures := role.stop(context.WithoutCancel(signalCtx), sup, life.logger)
	unwatch()
	loss := leaseLossIn(sup.lostLease(), role.stageAt(life.Served()), failures)
	return finishStack(signalCtx, sup, life, outcome, failures, loss)
}

func finishStack(
	signalCtx context.Context,
	sup *supervisor,
	life *lifecycle,
	outcome *exitOutcome,
	failures []stopFailure,
	loss *leaseLoss,
) int {
	late := lateOutcome(outcome, sup.late(), loss, sup.claimLeaseIncident)
	logOutcome(signalCtx, life.logger, &late)
	reportStopFailures(signalCtx, life.logger, failures, stopReport{
		state:    outcome.state,
		followup: sup.incidentOpened(),
	})
	code := max(outcome.code, late.code, codeOf(failures))
	life.finish(code)
	return code
}

func startAndServe(
	startCtx, signalCtx context.Context,
	role *stack,
	sup *supervisor,
	life *lifecycle,
) exitOutcome {
	life.transition(stateStarting, reasonLaunched)
	err := role.start(startCtx, sup)
	if err == nil && startCtx.Err() == nil {
		life.serve()
		<-startCtx.Done()
	}
	stage := role.stageAt(life.Served())
	return sup.snapshot(func(unit error) exitOutcome {
		return outcomeOf(&observed{
			err:    err,
			stage:  stage,
			cause:  context.Cause(startCtx),
			signal: context.Cause(signalCtx),
			unit:   unit,
		})
	})
}

func settle(signalCtx context.Context, outcome *exitOutcome, life *lifecycle) {
	logOutcome(signalCtx, life.logger, outcome)
	life.transition(outcome.state, outcome.reason)
}

func watchAbortSignal(signalCtx context.Context, logger *slog.Logger, outcome *exitOutcome) func() {
	if outcome.state != stateAborting {
		return unwatched
	}
	logged := make(chan struct{})
	unwatch := context.AfterFunc(signalCtx, func() {
		defer close(logged)
		logger.InfoContext(context.WithoutCancel(signalCtx), eventSignal, "state", string(stateAborting))
	})
	return func() {
		if !unwatch() {
			<-logged
		}
	}
}

func reportStopFailures(
	ctx context.Context,
	logger *slog.Logger,
	failures []stopFailure,
	report stopReport,
) {
	if len(failures) == 0 {
		return
	}
	attrs := []any{
		"state", string(report.state),
		"layers", lo.Map(failures, failedLayer),
		"error", errors.Join(lo.Map(failures, failureCause)...),
	}
	if report.followup {
		logger.WarnContext(context.WithoutCancel(ctx), eventStopFailed, append(attrs, "followup", true)...)
		return
	}
	logger.ErrorContext(context.WithoutCancel(ctx), eventStopFailed, attrs...)
}

func failedLayer(failure stopFailure, _ int) string {
	return failure.layer
}

func failureCause(failure stopFailure, _ int) error {
	return fmt.Errorf("%s: %w", failure.layer, failure.err)
}
