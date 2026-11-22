package processing_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	gocmp "github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const (
	movedAttempts  = 3
	movedPauses    = 2 * time.Second
	otherPartition = partition + 1
)

var (
	errCorruptPointer = errors.New("pointer does not parse")
	errNotAnImage     = []byte("not a checkpoint image")
	errStopped        = errors.New("partition stopped")
)

func resumeRequest(target *processing.DiscardTarget) processing.ResumeRequest {
	return processing.ResumeRequest{
		Partition: partition,
		Limits:    new(processingtest.Limits()),
		StartedAt: telemetrytest.Epoch(),
		Discard:   target,
	}
}

func emptyStart() processing.Staked {
	return processing.Staked{
		After: processing.Position{
			Partition: partition,
		},
	}
}

func unusableLatest() error {
	return errors.Join(processing.ErrCheckpointUnusable, errCorruptPointer)
}

func TestResumeStakesWhatLatestFound(t *testing.T) {
	store := committedStore(t)
	committed := store.Committed()
	last := committed[len(committed)-1]

	resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	want := processing.Staked{After: last.Position, ReceivedAt: last.ReceivedAt, Created: last.StreamCreated}
	if diff := gocmp.Diff(want, resumed.Staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
	if !resumed.Found || resumed.Attempts != 1 || resumed.DiscardCause != nil {
		t.Errorf("resumed = %+v, want found on the first attempt without a discard", resumed)
	}
	if got := processingtest.TotalCalls(resumed.Builder.Snapshot()); got != tenBatches*callsPerBatch {
		t.Errorf("the staked builder holds %d calls, want the %d of the checkpoint", got, tenBatches*callsPerBatch)
	}
}

func TestResumeStartsEmptyOnlyWithoutAPointer(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)

	resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if diff := gocmp.Diff(emptyStart(), resumed.Staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
	if resumed.Found || resumed.Builder == nil || len(resumed.Builder.Snapshot().Nodes) != 0 {
		t.Errorf("resumed = %+v, want an empty builder and nothing found", resumed)
	}
}

type resumeOutcome struct {
	resumed processing.Resumed
	err     error
}

func resumeInBackground(ctx context.Context, store *processingtest.CheckpointStore) <-chan resumeOutcome {
	done := make(chan resumeOutcome, 1)
	go func() {
		resumed, err := processing.Resume(ctx, store, resumeRequest(nil))
		done <- resumeOutcome{resumed: resumed, err: err}
	}()
	return done
}

func finishedBy(t *testing.T, done <-chan resumeOutcome, after time.Duration) (resumeOutcome, bool) {
	t.Helper()
	time.Sleep(after)
	synctest.Wait()
	select {
	case outcome := <-done:
		return outcome, true
	default:
		return resumeOutcome{}, false
	}
}

func TestResumeRetriesAMovedPointerThreeTimesThenFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := processingtest.NewCheckpointStore(t, nil)
		store.FailLatest(processing.ErrCheckpointMoved)

		done := resumeInBackground(t.Context(), store)

		if _, finished := finishedBy(t, done, movedPauses-time.Nanosecond); finished {
			t.Fatalf("Resume gave up before the %s of pauses", movedPauses)
		}
		outcome, finished := finishedBy(t, done, time.Nanosecond)
		if !finished {
			t.Fatalf("Resume still runs after %s of pauses", movedPauses)
		}
		if !errors.Is(outcome.err, processing.ErrCheckpointMoved) {
			t.Fatalf("Resume = %v, want %v", outcome.err, processing.ErrCheckpointMoved)
		}
		if outcome.resumed.Attempts != movedAttempts {
			t.Errorf("attempts = %d, want %d", outcome.resumed.Attempts, movedAttempts)
		}
		if _, staked := store.StakedAs(); staked {
			t.Error("a pointer that kept moving was staked")
		}
	})
}

func TestResumeTakesAPointerThatSettlesOnALaterAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := processingtest.NewCheckpointStore(t, nil)
		store.FailLatest(processing.ErrCheckpointMoved)
		go func() {
			time.Sleep(time.Second / 2)
			store.FailLatest(nil)
		}()

		resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if resumed.Attempts != 2 {
			t.Errorf("attempts = %d, want 2", resumed.Attempts)
		}
	})
}

func TestResumeNeverDiscardsWithoutATarget(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	store.FailLatest(unusableLatest())

	_, err := processing.Resume(t.Context(), store, resumeRequest(nil))

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("Resume = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
	if got := store.Discards(); got != 0 {
		t.Errorf("%d discards without an operator target", got)
	}
}

func TestResumeDiscardsOnlyWhatTheOperatorMayLose(t *testing.T) {
	type args struct {
		latest error
	}
	type want struct {
		discards int
		staked   bool
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "an unusable checkpoint is discarded",
			args: args{latest: unusableLatest()},
			want: want{discards: 1, staked: true},
		},
		{
			name:    "a refused store keeps its checkpoint",
			args:    args{latest: processing.ErrCheckpointRefused},
			wantErr: processing.ErrCheckpointRefused,
		},
		{
			name:    "an unavailable store keeps its checkpoint",
			args:    args{latest: processing.ErrCheckpointUnavailable},
			wantErr: processing.ErrCheckpointUnavailable,
		},
		{
			name:    "a lost lease keeps the checkpoint",
			args:    args{latest: errors.Join(processing.ErrCheckpointUnusable, processing.ErrLeaseLost)},
			wantErr: processing.ErrLeaseLost,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := processingtest.NewCheckpointStore(t, nil)
			store.FailLatest(tt.args.latest)

			resumed, err := processing.Resume(t.Context(), store, resumeRequest(&processing.DiscardTarget{
				Partition: partition,
			}))

			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Fatalf("Resume = %v, want %v", err, tt.wantErr)
			}
			_, staked := store.StakedAs()
			got := want{discards: store.Discards(), staked: staked}
			if diff := gocmp.Diff(tt.want, got, gocmp.AllowUnexported(want{})); diff != "" {
				t.Errorf("store (-want +got):\n%s", diff)
			}
			if tt.wantErr == nil && !errors.Is(resumed.DiscardCause, processing.ErrCheckpointUnusable) {
				t.Errorf("discard cause = %v, want the unusable checkpoint", resumed.DiscardCause)
			}
		})
	}
}

func TestResumeStartsADiscardedPartitionEmpty(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	store.FailLatest(unusableLatest())

	resumed, err := processing.Resume(t.Context(), store, resumeRequest(&processing.DiscardTarget{Partition: partition}))

	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if diff := gocmp.Diff(emptyStart(), resumed.Staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
	if resumed.Found || resumed.Builder == nil || len(resumed.Builder.Snapshot().Nodes) != 0 {
		t.Errorf("resumed = %+v, want an empty builder", resumed)
	}
}

func TestResumeNeverStakesAnImageItFailedToRestore(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	store.Put(partition, &processing.CheckpointMeta{Position: processing.Position{Partition: partition, Sequence: 9}},
		errNotAnImage)

	_, err := processing.Resume(t.Context(), store, resumeRequest(nil))

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("Resume = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
	if _, staked := store.StakedAs(); staked {
		t.Error("an image the domain refused was staked")
	}
}

func TestResumeRefusesATargetOfAnotherPartition(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	store.FailLatest(unusableLatest())

	_, err := processing.Resume(t.Context(), store, resumeRequest(&processing.DiscardTarget{Partition: otherPartition}))

	if !errors.Is(err, processing.ErrDiscardRefused) {
		t.Fatalf("Resume = %v, want %v", err, processing.ErrDiscardRefused)
	}
	if got := store.DiscardCalls(); got != 0 {
		t.Errorf("%d discards asked for a target of partition %d", got, otherPartition)
	}
}

func TestResumeStopsOnCancelWithoutSleepingTheRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := processingtest.NewCheckpointStore(t, nil)
		store.FailLatest(processing.ErrCheckpointMoved)
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(errStopped)

		outcome, finished := finishedBy(t, resumeInBackground(ctx, store), 0)

		if !finished {
			t.Fatal("Resume sleeps the retry pause after the stop")
		}
		if !errors.Is(outcome.err, errStopped) || !errors.Is(outcome.err, processing.ErrCheckpointMoved) {
			t.Fatalf("Resume = %v, want the stop cause next to the moved checkpoint", outcome.err)
		}
		if outcome.resumed.Attempts != 1 {
			t.Errorf("attempts = %d, want 1", outcome.resumed.Attempts)
		}
	})
}

const (
	streamStart     = 500
	newerCheckpoint = 900
)

func TestResumeKeepsTheClassOfAStakeFailure(t *testing.T) {
	tests := []struct {
		name    string
		args    error
		wantErr error
	}{
		{name: "a lost lease", args: processing.ErrLeaseLost, wantErr: processing.ErrLeaseLost},
		{name: "an unavailable store", args: processing.ErrCheckpointUnavailable, wantErr: processing.ErrCheckpointUnavailable},
		{name: "a refused stake", args: processing.ErrCheckpointRefused, wantErr: processing.ErrCheckpointRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := committedStore(t)
			store.FailStake(tt.args)

			resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Resume = %v, want %v", err, tt.wantErr)
			}
			if resumed.Attempts != 1 {
				t.Errorf("attempts = %d, want 1: only a moved pointer is retried", resumed.Attempts)
			}
			if resumed.Refused != nil {
				t.Errorf("refused target = %+v after a stake failure that cannot be discarded", *resumed.Refused)
			}
		})
	}
}

func TestResumeLatestsAgainWhenTheStakeFindsThePointerMoved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := committedStore(t)
		store.FailStake(processing.ErrCheckpointMoved)
		newer := processing.CheckpointMeta{Position: processing.Position{Partition: partition, Sequence: newerCheckpoint}}
		image := store.LatestBytes(partition)
		go func() {
			time.Sleep(time.Second / 2)
			store.Put(partition, &newer, image)
			store.FailStake(nil)
		}()

		resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if resumed.Attempts != 2 {
			t.Errorf("attempts = %d, want a second Latest after the moved stake", resumed.Attempts)
		}
		if diff := gocmp.Diff(newer.Position, resumed.Staked.After); diff != "" {
			t.Errorf("staked after (-want +got):\n%s, want the checkpoint the second Latest found", diff)
		}
	})
}

func TestResumeDiscardsAMissingCheckpointOnlyAtTheIncidentStart(t *testing.T) {
	tests := []struct {
		name    string
		args    uint64
		wantErr error
	}{
		{name: "the target names where the stream starts", args: streamStart},
		{name: "the target names another start", args: streamStart + 1, wantErr: processing.ErrDiscardRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := processingtest.NewCheckpointStore(t, nil)
			store.StartStreamAt(streamStart)
			store.FailLatest(unusableLatest())

			resumed, err := processing.Resume(t.Context(), store, resumeRequest(&processing.DiscardTarget{
				Partition: partition,
				FirstSeq:  tt.args,
			}))

			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Fatalf("Resume = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil && (resumed.Refused == nil || resumed.Refused.FirstSeq != streamStart) {
				t.Errorf("refused target = %+v, want the current start %d to paste", resumed.Refused, streamStart)
			}
		})
	}
}

func TestResumeKeepsTheCheckpointWhenAFreshBuilderCannotBeMade(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	store.FailLatest(unusableLatest())
	request := resumeRequest(&processing.DiscardTarget{Partition: partition})
	broken := *request.Limits
	broken.MaxNodes = 0
	request.Limits = &broken

	_, err := processing.Resume(t.Context(), store, request)

	if err == nil {
		t.Fatal("Resume with limits no builder accepts passed")
	}
	if got := store.Discards(); got != 0 {
		t.Errorf("%d discards before the start could build its state", got)
	}
}

func TestResumeDiscardsAnImageTheDomainRefusedByItsName(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	refused := processing.CheckpointMeta{Position: processing.Position{Partition: partition, Sequence: 9}}
	store.Put(partition, &refused, errNotAnImage)

	resumed, err := processing.Resume(t.Context(), store, resumeRequest(&processing.DiscardTarget{
		Partition:  partition,
		Checkpoint: processingtest.CheckpointName(&refused),
	}))

	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if diff := gocmp.Diff(emptyStart(), resumed.Staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
	if !errors.Is(resumed.DiscardCause, processing.ErrCheckpointUnusable) || resumed.Found {
		t.Errorf("resumed = %+v, want an empty start caused by the refused image", resumed)
	}
}

func TestResumeNamesTheTargetItCouldNotDiscardAndThatTargetResumes(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	refused := processing.CheckpointMeta{Position: processing.Position{Partition: partition, Sequence: 9}}
	store.Put(partition, &refused, errNotAnImage)

	first, err := processing.Resume(t.Context(), store, resumeRequest(nil))
	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("first Resume = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
	want := processing.DiscardTarget{Partition: partition, Checkpoint: processingtest.CheckpointName(&refused)}
	if diff := gocmp.Diff(&want, first.Refused); diff != "" {
		t.Fatalf("refused target (-want +got):\n%s", diff)
	}

	second, err := processing.Resume(t.Context(), store, resumeRequest(first.Refused))

	if err != nil {
		t.Fatalf("Resume with the target the first one named: %v", err)
	}
	if !errors.Is(second.DiscardCause, processing.ErrCheckpointUnusable) {
		t.Errorf("discard cause = %v, want the refused image", second.DiscardCause)
	}
}

func TestResumeNamesNoTargetForAFailureThatCannotBeDiscarded(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	store.FailLatest(processing.ErrCheckpointUnavailable)

	resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

	if !errors.Is(err, processing.ErrCheckpointUnavailable) {
		t.Fatalf("Resume = %v, want %v", err, processing.ErrCheckpointUnavailable)
	}
	if resumed.Refused != nil {
		t.Errorf("refused target = %+v for an unavailable store", *resumed.Refused)
	}
}

func TestResumeNamesTheTargetOfItsLastAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := processingtest.NewCheckpointStore(t, nil)
		store.StartStreamAt(streamStart)
		store.FailLatest(processing.ErrCheckpointMoved)
		go func() {
			time.Sleep(time.Second / 2)
			store.FailLatest(unusableLatest())
		}()

		resumed, err := processing.Resume(t.Context(), store, resumeRequest(nil))

		if !errors.Is(err, processing.ErrCheckpointUnusable) || resumed.Attempts != 2 {
			t.Fatalf("Resume = %v after %d attempts, want an unusable checkpoint on the second", err, resumed.Attempts)
		}
		want := processing.DiscardTarget{Partition: partition, FirstSeq: streamStart}
		if diff := gocmp.Diff(&want, resumed.Refused); diff != "" {
			t.Errorf("refused target (-want +got):\n%s", diff)
		}
	})
}
