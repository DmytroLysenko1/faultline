package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
)

const (
	partitionLabelPrefix = "p"
	partitionLease       = "lease"
	partitionPipeline    = "pipeline"
	partitionCheckpoint  = "checkpoint"

	stepAcquire        = "acquire"
	stepEnsureTopology = "ensure_topology"
	stepOpenStore      = "open_store"
	stepResume         = "resume"
	stepPublishGraph   = "publish_graph"
	stepOpenFeed       = "open_feed"
	stepStartStore     = "start_store"
	stepBuildPipeline  = "build_pipeline"

	eventFeedResumed         = "lifecycle.feed_resumed"
	eventFeedCaughtUp        = "lifecycle.feed_caught_up"
	eventCheckpointDiscarded = "checkpoint.discarded"
	eventRecoveryUnused      = "checkpoint.recovery_unused"

	catchUpPoll = time.Second
)

var (
	errRestoreTimeout = errors.New("partition restore did not finish within its budget")
	errLeaseWaiting   = errors.New("partition lease not acquired within one attempt")
)

type severableConns interface {
	conns() *natsjs.Conn
	sever(ctx context.Context) error
}

type restoreStep struct {
	name string
	run  func(u *partitionUnit, ctx context.Context) error
}

type partitionUnit struct {
	config         *partitionConfig
	source         severableConns
	graph          *memgraph.Store
	phases         phaseLog
	logger         *slog.Logger
	clock          wallClock
	label          string
	stopped        chan struct{}
	closeStopped   func()
	sup            unitSupervisor
	lease          *lease.Lease
	store          *checkpoint.Store
	resumed        processing.Resumed
	feed           *feed.Feed
	sink           *checkpoint.Sink
	pipeline       *processing.Pipeline
	cancelRenew    context.CancelFunc
	cancelPipeline context.CancelFunc
	cancelCatchUp  context.CancelFunc
	catchUp        sync.WaitGroup
}

func partitionLabel(partition uint16) string {
	return partitionLabelPrefix + strconv.FormatUint(uint64(partition), decimalBase)
}

func partitionLayer(
	cfg *partitionConfig,
	source severableConns,
	graph *memgraph.Store,
	phases phaseLog,
	logger *slog.Logger,
) (layer, *partitionUnit) {
	part := newPartitionUnit(cfg, source, graph, phases, logger)
	return layer{
		name: part.label,
		unit: part,
		components: []string{
			part.component(partitionLease),
			part.component(partitionPipeline),
			part.component(partitionCheckpoint),
		},
		stopBudget: partitionStopBudget,
		ready:      part.ready,
	}, part
}

func newPartitionUnit(
	cfg *partitionConfig,
	source severableConns,
	graph *memgraph.Store,
	phases phaseLog,
	logger *slog.Logger,
) *partitionUnit {
	stopped := make(chan struct{})
	return &partitionUnit{
		config:         cfg,
		source:         source,
		graph:          graph,
		phases:         phases,
		logger:         logger,
		clock:          systemClock{},
		label:          partitionLabel(cfg.Partition),
		stopped:        stopped,
		closeStopped:   sync.OnceFunc(func() { close(stopped) }),
		cancelRenew:    func() {},
		cancelPipeline: func() {},
		cancelCatchUp:  func() {},
	}
}

func (u *partitionUnit) component(part string) string {
	return u.label + "/" + part
}

func (u *partitionUnit) start(ctx context.Context, sup unitSupervisor) error {
	u.sup = sup
	if err := u.acquire(ctx); err != nil {
		return err
	}
	if err := u.restore(ctx); err != nil {
		return err
	}
	u.serve(ctx)
	return nil
}

func (u *partitionUnit) acquire(ctx context.Context) error {
	u.phases.transition(stateWaitingLease, reasonAcquiring)
	held, err := u.awaitLease(ctx)
	if err != nil {
		return &stageError{
			stage: u.component(stepAcquire),
			err:   err,
		}
	}
	renewCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	u.lease = held
	u.cancelRenew = cancel
	u.sup.spawn(u.component(partitionLease), func() error {
		return held.Run(renewCtx)
	})
	return nil
}

func (u *partitionUnit) awaitLease(ctx context.Context) (*lease.Lease, error) {
	wait := newLeaseWait(time.Now(), u.label)
	for {
		held, err := u.acquireOnce(ctx)
		if !errors.Is(err, errLeaseWaiting) {
			return held, err
		}
		wait.attempted(err)
		wait.remind(ctx, u.logger)
	}
}

func (u *partitionUnit) acquireOnce(ctx context.Context) (*lease.Lease, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, waitReportPeriod)
	defer cancel()
	held, err := lease.Acquire(attemptCtx, u.source.conns(), leaseOptions(u.config, u.logger))
	if err != nil && ctx.Err() == nil && attemptCtx.Err() != nil {
		return nil, fmt.Errorf("%w: %w", errLeaseWaiting, err)
	}
	return held, err
}

type leaseWait struct {
	reminder
	partition string
	holder    string
	lastError error
}

func newLeaseWait(now time.Time, partition string) *leaseWait {
	wait := &leaseWait{
		partition: partition,
	}
	wait.reminder = reminder{
		state:  stateWaitingLease,
		level:  slog.LevelInfo,
		began:  now,
		next:   now.Add(waitReportPeriod),
		detail: wait.details,
	}
	return wait
}

func (w *leaseWait) attempted(err error) {
	held, found := errors.AsType[*lease.HeldError](err)
	if found {
		w.level, w.holder, w.lastError = slog.LevelInfo, held.Holder, nil
		return
	}
	w.level, w.holder, w.lastError = slog.LevelWarn, "", err
}

func (w *leaseWait) details() []any {
	if w.lastError != nil {
		return []any{"partition", w.partition, "last_error", w.lastError}
	}
	return []any{"partition", w.partition, "holder", w.holder}
}

func (u *partitionUnit) restore(ctx context.Context) error {
	u.phases.transition(stateRestoring, reasonLeaseAcquired)
	restoreCtx, cancel := context.WithTimeoutCause(ctx, u.config.RestoreBudget, errRestoreTimeout)
	defer cancel()
	err := u.runRestore(restoreCtx)
	if err != nil && restoreCtx.Err() != nil {
		return errors.Join(err, context.Cause(restoreCtx))
	}
	return err
}

func restoreSteps() []restoreStep {
	return []restoreStep{
		{
			name: stepEnsureTopology,
			run:  (*partitionUnit).ensureTopology,
		},
		{
			name: stepOpenStore,
			run:  (*partitionUnit).openStore,
		},
		{
			name: stepResume,
			run:  (*partitionUnit).resume,
		},
		{
			name: stepPublishGraph,
			run:  (*partitionUnit).publishGraph,
		},
		{
			name: stepOpenFeed,
			run:  (*partitionUnit).openFeed,
		},
		{
			name: stepStartStore,
			run:  (*partitionUnit).startStore,
		},
		{
			name: stepBuildPipeline,
			run:  (*partitionUnit).buildPipeline,
		},
	}
}

func (u *partitionUnit) runRestore(ctx context.Context) error {
	for _, step := range restoreSteps() {
		if err := step.run(u, ctx); err != nil {
			return &stageError{
				stage: u.component(step.name),
				err:   err,
			}
		}
	}
	return nil
}

func (u *partitionUnit) ensureTopology(ctx context.Context) error {
	return u.source.conns().EnsureTopology(ctx, u.config.Streams, u.lease)
}

func (u *partitionUnit) openStore(ctx context.Context) error {
	store, err := checkpoint.Open(ctx, u.source.conns(), u.lease, checkpointOptions(u.config, u.lease.Epoch))
	u.store = store
	return err
}

func (u *partitionUnit) resume(ctx context.Context) error {
	resumed, err := processing.Resume(ctx, u.store, resumeRequest(u.config, time.Now()))
	if err != nil {
		return &restoreError{
			err:         err,
			attempts:    resumed.Attempts,
			discardable: processing.Discardable(err) || resumed.Refused != nil,
			hint:        recoveryHint(resumed.Refused),
		}
	}
	u.resumed = resumed
	return nil
}

func (u *partitionUnit) publishGraph(ctx context.Context) error {
	return u.graph.Publish(ctx, u.resumed.Builder.Snapshot())
}

func (u *partitionUnit) openFeed(ctx context.Context) error {
	staked := u.resumed.Staked
	start := feed.Start{
		After:      staked.After,
		ReceivedAt: staked.ReceivedAt,
		Created:    staked.Created,
	}
	opened, err := feed.Open(ctx, u.source.conns(), u.lease, start, feedOptions(u.config, u.lease.Epoch))
	u.feed = opened
	return err
}

func (u *partitionUnit) startStore(ctx context.Context) error {
	sink, err := u.store.Start(ctx, u.feed)
	u.sink = sink
	return err
}

func (u *partitionUnit) buildPipeline(context.Context) error {
	built, err := processing.NewPipeline(u.feed, u.resumed.Builder, u.graph, u.sink, systemClock{}, partitionCadence(u.config))
	if err != nil {
		return fmt.Errorf("build pipeline: %w", err)
	}
	u.pipeline = built
	return nil
}

func (u *partitionUnit) serve(ctx context.Context) {
	u.logResumed(ctx)
	pipelineCtx, cancelPipeline := context.WithCancel(context.WithoutCancel(ctx))
	catchUpCtx, cancelCatchUp := context.WithCancel(context.WithoutCancel(ctx))
	u.cancelPipeline = cancelPipeline
	u.cancelCatchUp = cancelCatchUp
	u.sup.spawn(u.component(partitionPipeline), func() error {
		return u.pipeline.Run(pipelineCtx)
	})
	u.sup.spawn(u.component(partitionCheckpoint), u.watch)
	began := time.Now()
	u.catchUp.Go(func() {
		u.awaitCatchUp(catchUpCtx, began)
	})
}

func (u *partitionUnit) logResumed(ctx context.Context) {
	resumed := &u.resumed
	u.logger.InfoContext(ctx, eventFeedResumed,
		"partition", u.label,
		slog.Uint64("after", resumed.Staked.After.Sequence),
		slog.Uint64("epoch", u.lease.Epoch),
		"found", resumed.Found,
		"attempts", resumed.Attempts,
	)
	if resumed.DiscardCause != nil {
		u.logger.WarnContext(ctx, eventCheckpointDiscarded, "partition", u.label, "cause", resumed.DiscardCause)
		return
	}
	if u.config.Recovery != nil {
		u.logger.WarnContext(ctx, eventRecoveryUnused, "partition", u.label, "target", formatRecovery(*u.config.Recovery))
	}
}

func (u *partitionUnit) watch() error {
	select {
	case <-u.store.Done():
		select {
		case <-u.stopped:
			return nil
		default:
			return u.store.Err()
		}
	case <-u.stopped:
		return nil
	}
}

func (u *partitionUnit) awaitCatchUp(ctx context.Context, began time.Time) {
	ticker := time.NewTicker(catchUpPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if u.caughtUp() {
			u.logger.InfoContext(ctx, eventFeedCaughtUp,
				"partition", u.label,
				slog.Float64("elapsed_seconds", time.Since(began).Seconds()),
			)
			return
		}
	}
}

func (u *partitionUnit) caughtUp() bool {
	stats := u.feed.Stats()
	return stats.CaughtUp()
}

func (u *partitionUnit) ready() bool {
	select {
	case <-u.lease.Lost():
		return false
	default:
	}
	return u.feed.Stats().Stop == feed.StopNone
}

type restoreError struct {
	err         error
	attempts    int
	discardable bool
	hint        string
}

func (e *restoreError) Error() string {
	return e.err.Error()
}

func (e *restoreError) Unwrap() error {
	return e.err
}
