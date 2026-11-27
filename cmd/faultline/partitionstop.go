package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
)

const (
	stopStepFeed      = "close_feed"
	stopStepPipeline  = "drain_pipeline"
	stopStepStore     = "close_store"
	stopStepFeedWait  = "await_feed"
	stopStepLease     = "release_lease"
	firstSeveringStep = 2

	pipelineDrainBudget  = 3500 * time.Millisecond
	pipelineCancelBudget = 500 * time.Millisecond
	storeStopBudget      = 6 * time.Second
	feedStopBudget       = 500 * time.Millisecond
	leaseReleaseBudget   = 600 * time.Millisecond
	severedLeaseWait     = 100 * time.Millisecond
	partitionStopBudget  = pipelineDrainBudget + pipelineCancelBudget + storeStopBudget + feedStopBudget +
		leaseReleaseBudget

	eventFeedUnprocessed   = "lifecycle.feed_unprocessed"
	eventCheckpointStopped = "checkpoint.stopped"
)

type stopStep struct {
	name   string
	run    func(ctx context.Context) error
	budget time.Duration
}

func (u *partitionUnit) stop(ctx context.Context) error {
	u.closeStopped()
	failures := u.stopWith(ctx, u.drainSteps())
	u.logRemainder(ctx)
	return errors.Join(failures...)
}

func (u *partitionUnit) stopWith(ctx context.Context, drain []stopStep) []error {
	failures := runStopSteps(ctx, drain)
	return append(failures, runStopStep(ctx, u.releaseStep(writerAbandoned(failures))))
}

func (u *partitionUnit) drainSteps() []stopStep {
	return []stopStep{
		{
			name: stopStepFeed,
			run:  u.closeFeed,
		},
		{
			name:   stopStepPipeline,
			run:    u.drainPipeline,
			budget: pipelineDrainBudget + pipelineCancelBudget,
		},
		{
			name:   stopStepStore,
			run:    u.closeStore,
			budget: storeStopBudget,
		},
		{
			name:   stopStepFeedWait,
			run:    u.awaitFeed,
			budget: feedStopBudget,
		},
	}
}

func writerAbandoned(failures []error) bool {
	return errors.Is(errors.Join(failures[firstSeveringStep:]...), errStepAbandoned)
}

func (u *partitionUnit) releaseStep(abandoned bool) stopStep {
	release := u.releaseLease
	if abandoned {
		release = u.severLease
	}
	return stopStep{
		name:   stopStepLease,
		run:    release,
		budget: leaseReleaseBudget,
	}
}

func runStopSteps(ctx context.Context, steps []stopStep) []error {
	failures := make([]error, len(steps))
	for index, step := range steps {
		failures[index] = runStopStep(ctx, step)
	}
	return failures
}

func runStopStep(ctx context.Context, step stopStep) error {
	if step.budget == 0 {
		return stepFailure(step.name, step.run(ctx))
	}
	stepCtx, cancel := context.WithTimeout(ctx, step.budget)
	defer cancel()
	return stepFailure(step.name, step.run(stepCtx))
}

func stepFailure(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", name, err)
}

func (u *partitionUnit) closeFeed(_ context.Context) error {
	if u.feed != nil {
		u.feed.Close()
	}
	u.cancelCatchUp()
	u.catchUp.Wait()
	return nil
}

func (u *partitionUnit) drainPipeline(ctx context.Context) error {
	if u.pipeline == nil {
		return nil
	}
	drainCtx, cancel := context.WithTimeout(ctx, pipelineDrainBudget)
	defer cancel()
	if exited, err := u.sup.wait(drainCtx, u.component(partitionPipeline)); exited {
		return err
	}
	u.cancelPipeline()
	return u.awaitComponent(ctx, partitionPipeline)
}

func (u *partitionUnit) closeStore(ctx context.Context) error {
	if u.store == nil {
		return nil
	}
	u.store.Close(ctx)
	if err := u.store.Wait(ctx); err != nil {
		return fmt.Errorf("%w: %w", errStepAbandoned, err)
	}
	terminal := terminalOf(u.store)
	_, watched := u.sup.wait(ctx, u.component(partitionCheckpoint))
	return errors.Join(terminal, watched)
}

func terminalOf(store *checkpoint.Store) error {
	select {
	case <-store.Done():
		return store.Err()
	default:
		return nil
	}
}

func (u *partitionUnit) awaitFeed(ctx context.Context) error {
	if u.feed == nil {
		return nil
	}
	if err := u.feed.Wait(ctx); err != nil {
		return fmt.Errorf("%w: %w", errStepAbandoned, err)
	}
	return nil
}

func (u *partitionUnit) releaseLease(ctx context.Context) error {
	if u.lease == nil {
		return nil
	}
	u.cancelRenew()
	return u.awaitComponent(ctx, partitionLease)
}

func (u *partitionUnit) severLease(ctx context.Context) error {
	if u.lease == nil {
		return nil
	}
	severed := u.source.sever(ctx)
	u.cancelRenew()
	waitCtx, cancel := context.WithTimeout(ctx, severedLeaseWait)
	defer cancel()
	return errors.Join(severed, u.awaitComponent(waitCtx, partitionLease))
}

func (u *partitionUnit) awaitComponent(ctx context.Context, part string) error {
	exited, err := u.sup.wait(ctx, u.component(part))
	if !exited {
		return errStepAbandoned
	}
	return err
}

func (u *partitionUnit) logRemainder(ctx context.Context) {
	if u.feed != nil {
		stats := u.feed.Stats()
		u.logger.InfoContext(ctx, eventFeedUnprocessed, "partition", u.label, slog.Uint64("batches", stats.Unreleased()))
	}
	if u.store != nil {
		stats := u.store.Stats()
		u.logger.InfoContext(ctx, eventCheckpointStopped,
			"partition", u.label,
			slog.Uint64("committed_through", stats.CommittedThrough),
			slog.Uint64("finals", stats.Finals),
			slog.Uint64("left_at_stop", stats.LeftAtStop),
		)
	}
}
