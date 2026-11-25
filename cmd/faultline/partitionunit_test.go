package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/samber/lo"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	testCheckpointName = "FLALVKZUWZFDM3XNJWSSR3FHYQ"
	laterPartition     = 7
	smallImage         = 1 << 20
	exactImage         = 16 << 20
	minFeedBytes       = 48 << 20
	movedAttempts      = 3
	quarterSecond      = 250 * time.Millisecond
)

var errPipelineExit = errors.New("pipeline failed after stopping")

type fakeConns struct {
	events *eventLog
	err    error
}

func (f fakeConns) conns() *natsjs.Conn {
	return nil
}

func (f fakeConns) sever(context.Context) error {
	f.events.add("sever")
	return f.err
}

type scriptedSupervisor struct {
	events *eventLog
	waits  func(ctx context.Context, name string) (bool, error)
}

func (s scriptedSupervisor) spawn(string, func() error) {}

func (s scriptedSupervisor) wait(ctx context.Context, name string) (bool, error) {
	s.events.add("wait " + name)
	return s.waits(ctx, name)
}

func exitsWith(err error) func(context.Context, string) (bool, error) {
	return func(context.Context, string) (bool, error) {
		return true, err
	}
}

func TestRecoveryHintRoundTrips(t *testing.T) {
	tests := []struct {
		name   string
		target processing.DiscardTarget
	}{
		{"a checkpoint", processing.DiscardTarget{
			Checkpoint: testCheckpointName,
		}},
		{"an empty stream", processing.DiscardTarget{}},
		{"a large first sequence", processing.DiscardTarget{
			FirstSeq: math.MaxUint64,
		}},
		{"a later partition", processing.DiscardTarget{
			Partition: laterPartition,
			FirstSeq:  3,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRecovery(formatRecovery(tt.target), tt.target.Partition)
			if err != nil {
				t.Fatalf("parse %q: %v", formatRecovery(tt.target), err)
			}
			if diff := cmp.Diff(&tt.target, got); diff != "" {
				t.Errorf("round trip (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseRecoveryRefusesWhatItCannotDiscard(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr error
	}{
		{"two parts", "discard:p0", errRecoveryShape},
		{"another verb", "keep:p0:none@1", errRecoveryShape},
		{"another partition", "discard:p1:none@1", errRecoveryPartition},
		{"a short name", "discard:p0:ABC", errRecoveryCheckpoint},
		{"a lower-case name", "discard:p0:" + strings.ToLower(testCheckpointName), errRecoveryCheckpoint},
		{"a sequence that is not a number", "discard:p0:none@x", errRecoveryShape},
		{"a negative sequence", "discard:p0:none@-1", errRecoveryShape},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRecovery(tt.raw, 0)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("parseRecovery(%q) = %v, %v, want %v", tt.raw, got, err, tt.wantErr)
			}
		})
	}
	if got, err := parseRecovery("", 0); got != nil || err != nil {
		t.Errorf("parseRecovery(\"\") = %v, %v, want no target", got, err)
	}
}

func TestTheLeaseLineAndTheDiscardHintNameOnePartition(t *testing.T) {
	tests := []struct {
		name      string
		partition uint16
		want      string
	}{
		{"the only partition", 0, "p0"},
		{"a later partition", laterPartition, "p7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partition, _ := partitionLayer(&partitionConfig{
				Partition: tt.partition,
			}, nil, nil, nil, nil)
			exit := &unitExitError{
				name: partition.components[0],
				err:  errLeaseGone,
			}
			outcome := outcomeOf(&observed{
				stage: stageServing,
				cause: exit,
				unit:  exit,
			})
			hint := formatRecovery(processing.DiscardTarget{
				Partition:  tt.partition,
				Checkpoint: testCheckpointName,
			})
			got := []string{partition.name, partitionAttr(outcome), strings.Split(hint, ":")[1]}
			if diff := cmp.Diff([]string{tt.want, tt.want, tt.want}, got); diff != "" {
				t.Errorf("layer, lease.lost, hint (-want +got):\n%s", diff)
			}
			components := []string{tt.want + "/lease", tt.want + "/pipeline", tt.want + "/checkpoint"}
			assertDiff(t, "components", components, partition.components)
		})
	}
}

func TestTheDiscardHintNamesTheCheckpointOrTheFirstSequence(t *testing.T) {
	tests := []struct {
		name   string
		target *processing.DiscardTarget
		want   string
	}{
		{"a checkpoint", &processing.DiscardTarget{
			Checkpoint: "N",
		}, "CHECKPOINT_RECOVERY=discard:p0:N"},
		{"no checkpoint", &processing.DiscardTarget{
			FirstSeq: 42,
		}, "CHECKPOINT_RECOVERY=discard:p0:none@42"},
		{"no target", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDiff(t, "hint", tt.want, recoveryHint(tt.target))
		})
	}
}

func TestARestoreFailureNamesItsHint(t *testing.T) {
	tests := []struct {
		name    string
		failure *restoreError
		want    map[string]any
	}{
		{
			name: "a discardable failure",
			failure: &restoreError{
				err:         processing.ErrCheckpointUnusable,
				attempts:    1,
				discardable: true,
				hint: recoveryHint(&processing.DiscardTarget{
					Checkpoint: testCheckpointName,
				}),
			},
			want: map[string]any{
				"stage":       "p0/resume",
				"cause":       processing.ErrCheckpointUnusable.Error(),
				"discardable": "true",
				"attempts":    "1",
				"hint":        "CHECKPOINT_RECOVERY=discard:p0:" + testCheckpointName,
			},
		},
		{
			name: "a failure that cannot be discarded",
			failure: &restoreError{
				err:      processing.ErrCheckpointMoved,
				attempts: movedAttempts,
			},
			want: map[string]any{
				"stage":       "p0/resume",
				"cause":       processing.ErrCheckpointMoved.Error(),
				"discardable": "false",
				"attempts":    "3",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome := outcomeOf(&observed{
				err: &stageError{
					stage: "p0/resume",
					err:   tt.failure,
				},
				stage: "p0",
			})
			got := map[string]any{}
			for _, attr := range outcome.attrs {
				got[attr.Key] = attr.Value.String()
			}
			assertDiff(t, "event", eventRestoreFailed, outcome.event)
			assertDiff(t, "code", exitFailure, outcome.code)
			assertDiff(t, "attrs", tt.want, got)
		})
	}
}

func TestARestoreTimeoutIsAFailureNotASignal(t *testing.T) {
	err := errors.Join(&stageError{
		stage: "p0/resume",
		err:   context.DeadlineExceeded,
	}, errRestoreTimeout)
	outcome := outcomeOf(&observed{
		err:    err,
		stage:  "p0",
		cause:  nil,
		signal: nil,
	})
	assertDiff(t, "code", exitFailure, outcome.code)
	assertDiff(t, "reason", reasonStartFailed, outcome.reason)
	if errors.Is(errRestoreTimeout, context.Canceled) {
		t.Errorf("the restore timeout reads as a cancel")
	}
}

func TestRestoreBudgetCoversThreeReadsOfTheWorstImage(t *testing.T) {
	cfg, err := loadGraph(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("loadGraph: %v", err)
	}
	tests := []struct {
		name  string
		image int64
		want  time.Duration
	}{
		{"the worst image at the defaults", cfg.Topology.WorstCheckpointBytes(), 80 * time.Second},
		{"a 1 MiB image", smallImage, 35 * time.Second},
		{"an image of exactly one second of reading", exactImage, 35 * time.Second},
		{"one byte past a second", exactImage + 1, 38 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDiff(t, "budget", tt.want, restoreBudget(tt.image))
		})
	}
}

func TestPartitionOptionsFollowTheConfig(t *testing.T) {
	cfg := testPartitionConfig(t)
	cfg.FeedMaxBytes = minFeedBytes
	cfg.Limits.EarlyFirePeriod = quarterSecond
	cadence := partitionCadence(cfg)
	if err := cadence.Validate(); err != nil {
		t.Errorf("cadence at the smallest feed: %v", err)
	}
	assertDiff(t, "held bytes", int64(minFeedBytes)-otlp.MaxBatchFootprint(), cadence.MaxHeldBytes)
	assertDiff(t, "checkpoint interval", testCheckpointEvery, cadence.CheckpointInterval)
	assertDiff(t, "fire period", quarterSecond, cadence.FirePeriod)
	stored := checkpointOptions(cfg, 9)
	assertDiff(t, "bucket", bucketBytes(stored.WorstImage), stored.MaxBytes)
	assertDiff(t, "bucket of ten bytes", int64(44), bucketBytes(10))
	assertDiff(t, "bucket rounds up", int64(5), bucketBytes(1))
	assertDiff(t, "checkpoint epoch", uint64(9), stored.Epoch)
	assertDiff(t, "feed epoch", uint64(9), feedOptions(cfg, 9).Epoch)
	assertDiff(t, "feed idle", testIdleAfter, feedOptions(cfg, 9).IdleAfter)
	if err := stored.Validate(); err != nil {
		t.Errorf("checkpoint options: %v", err)
	}
}

func TestTheReleaseStepSeversBeforeItCancelsTheRenewalOnlyWhenAWriterWasAbandoned(t *testing.T) {
	tests := []struct {
		name      string
		abandoned bool
		want      []string
	}{
		{"a clean drain", false, []string{"cancel renew", "wait p0/lease"}},
		{"an abandoned writer", true, []string{"sever", "cancel renew", "wait p0/lease"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &eventLog{}
			part := releasable(events, exitsWith(nil))
			if err := runStopStep(t.Context(), part.releaseStep(tt.abandoned)); err != nil {
				t.Fatalf("release: %v", err)
			}
			assertDiff(t, "order", tt.want, events.list())
		})
	}
}

func releasable(events *eventLog, waits func(context.Context, string) (bool, error)) *partitionUnit {
	part := newPartitionUnit(&partitionConfig{}, fakeConns{
		events: events,
	}, nil, nil, nil)
	part.lease = &lease.Lease{}
	part.cancelRenew = func() { events.add("cancel renew") }
	part.sup = scriptedSupervisor{
		events: events,
		waits:  waits,
	}
	return part
}

func TestTheLeaseStepReturnsTheLeaseExitAfterStopping(t *testing.T) {
	lost := fmt.Errorf("renew: %w", processing.ErrLeaseLost)
	part := releasable(&eventLog{}, exitsWith(lost))
	err := runStopStep(t.Context(), part.releaseStep(false))
	if !errors.Is(err, processing.ErrLeaseLost) || !strings.HasPrefix(err.Error(), stopStepLease+": ") {
		t.Errorf("release = %v, want the lease exit named by its step", err)
	}
}

func TestOnlyAnAbandonedStoreOrFeedWaitSeversTheConnection(t *testing.T) {
	abandoned := fmt.Errorf("x: %w", errStepAbandoned)
	tests := []struct {
		name     string
		failures []error
		want     bool
	}{
		{"nothing failed", []error{nil, nil, nil, nil}, false},
		{"the pipeline was abandoned", []error{nil, abandoned, nil, nil}, false},
		{"the store wait was abandoned", []error{nil, nil, abandoned, nil}, true},
		{"the feed wait was abandoned", []error{nil, nil, nil, abandoned}, true},
		{"the store failed without abandoning", []error{nil, nil, errA, nil}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDiff(t, "severs", tt.want, writerAbandoned(tt.failures))
		})
	}
}

func TestAStopStepIsNamedAndBoundedByItsBudget(t *testing.T) {
	var unbounded, bounded bool
	free := stopStep{name: "free", run: func(ctx context.Context) error {
		_, unbounded = ctx.Deadline()
		return nil
	}}
	timed := stopStep{name: "timed", budget: time.Second, run: func(ctx context.Context) error {
		_, bounded = ctx.Deadline()
		return errA
	}}
	if err := runStopStep(t.Context(), free); err != nil || unbounded {
		t.Errorf("a step without a budget = %v, deadline %v; want nil and the parent's context", err, unbounded)
	}
	err := runStopStep(t.Context(), timed)
	if !errors.Is(err, errA) || err.Error() != "timed: "+errA.Error() || !bounded {
		t.Errorf("a budgeted step = %v, deadline %v; want its error named and a deadline", err, bounded)
	}
}

func TestTheDrainWaitsForThePipelineThenCancelsItThenGivesUp(t *testing.T) {
	tests := []struct {
		name    string
		exit    string
		want    []string
		wantErr error
	}{
		{"it drains in time", "drain", []string{"wait p0/pipeline"}, errPipelineExit},
		{"it exits once canceled", "cancel", []string{"wait p0/pipeline", "cancel pipeline", "wait p0/pipeline"}, nil},
		{"it never exits", "never", []string{"wait p0/pipeline", "cancel pipeline", "wait p0/pipeline"}, errStepAbandoned},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				events := &eventLog{}
				canceled := make(chan struct{})
				part := releasable(events, pipelineExit(tt.exit, canceled))
				part.pipeline = &processing.Pipeline{}
				part.cancelPipeline = func() {
					events.add("cancel pipeline")
					close(canceled)
				}
				began := time.Now()
				err := runStopStep(t.Context(), part.drainSteps()[1])
				if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
					t.Errorf("drain = %v, want %v", err, tt.wantErr)
				}
				assertDiff(t, "events", tt.want, events.list())
				if tt.exit == "never" {
					assertDiff(t, "gave up after", pipelineDrainBudget+pipelineCancelBudget, time.Since(began))
				}
			})
		})
	}
}

func pipelineExit(mode string, canceled <-chan struct{}) func(context.Context, string) (bool, error) {
	return func(ctx context.Context, _ string) (bool, error) {
		switch mode {
		case "drain":
			return true, errPipelineExit
		case "cancel":
			select {
			case <-canceled:
				return true, nil
			case <-ctx.Done():
				return false, nil
			}
		}
		<-ctx.Done()
		return false, nil
	}
}

func TestAPartitionThatNeverAcquiredItsLeaseStopsWithNothingToDo(t *testing.T) {
	logs, events := &logBuffer{}, &eventLog{}
	part := newPartitionUnit(&partitionConfig{}, fakeConns{
		events: events,
	}, nil, nil, testLogger(logs))
	if err := part.stop(t.Context()); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
	select {
	case <-part.stopped:
	default:
		t.Errorf("stop left the watch running")
	}
	assertDiff(t, "events", []string(nil), events.list())
	assertDiff(t, "logs", "", logs.String())
}

func TestAnAbandonedWriterSeversBeforeTheRenewalIsCanceled(t *testing.T) {
	for _, abandoned := range []int{2, 3} {
		events := &eventLog{}
		part := releasable(events, exitsWith(nil))
		drain := make([]stopStep, 4)
		for index := range drain {
			drain[index] = stopStep{
				name: "step",
				run:  func(context.Context) error { return nil },
			}
		}
		drain[abandoned].run = func(context.Context) error { return errStepAbandoned }

		failures := part.stopWith(t.Context(), drain)

		if !errors.Is(errors.Join(failures...), errStepAbandoned) {
			t.Fatalf("failures = %v, want the abandoned step kept", failures)
		}
		assertDiff(t, "order", []string{"sever", "cancel renew", "wait p0/lease"}, events.list())
	}
}

func TestAPartitionStopTwiceDoesNotPanic(t *testing.T) {
	part := newPartitionUnit(&partitionConfig{}, fakeConns{
		events: &eventLog{},
	}, nil, nil, testLogger(&logBuffer{}))
	for range 2 {
		if err := part.stop(t.Context()); err != nil {
			t.Errorf("stop = %v, want nil", err)
		}
	}
}

func TestTheLeaseWaitNamesTheHolderOrTheLastError(t *testing.T) {
	held := fmt.Errorf("%w: %w", errLeaseWaiting, &lease.HeldError{
		Holder: "other/1",
	})
	unreachable := fmt.Errorf("%w: %w", errLeaseWaiting, context.DeadlineExceeded)
	tests := []struct {
		name string
		err  error
		want map[string]any
	}{
		{"another processor holds it", held, map[string]any{
			"level":     levelInfo,
			"partition": "p0",
			"holder":    "other/1",
		}},
		{"the bucket does not answer", unreachable, map[string]any{
			"level":      levelWarn,
			"partition":  "p0",
			"last_error": unreachable.Error(),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := &logBuffer{}
			began := time.Now().Add(-2 * waitReportPeriod)
			wait := newLeaseWait(began, "p0")
			wait.attempted(tt.err)
			wait.remind(t.Context(), testLogger(logs))
			got := lo.PickByKeys(logs.event(t, eventStillWaiting), []string{"level", "partition", "holder", "last_error"})
			assertDiff(t, "reminder", tt.want, got)
		})
	}
}

func TestARestoreFailureThatRanOutOfItsBudgetSaysSo(t *testing.T) {
	slow := &restoreError{
		err:      errors.Join(processing.ErrCheckpointUnavailable, context.DeadlineExceeded),
		attempts: 1,
	}
	err := errors.Join(&stageError{
		stage: "p0/resume",
		err:   slow,
	}, errRestoreTimeout)
	outcome := outcomeOf(&observed{
		err:   err,
		stage: "p0",
	})
	assertDiff(t, "event", eventRestoreFailed, outcome.event)
	for _, attr := range outcome.attrs {
		if attr.Key == "cause" && strings.Contains(attr.Value.String(), errRestoreTimeout.Error()) {
			return
		}
	}
	t.Errorf("restore_failed %v does not name the restore budget", outcome.attrs)
}
