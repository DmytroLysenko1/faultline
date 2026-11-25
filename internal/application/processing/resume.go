package processing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	firstAttempt  = 1
	movedAttempts = 3
	movedPause    = time.Second
)

type checkpointResumer interface {
	Latest(ctx context.Context, partition uint16) (CheckpointMeta, io.ReadCloser, error)
	Stake(ctx context.Context) (Staked, error)
	Discard(ctx context.Context, target DiscardTarget) (Staked, error)
	Refused(ctx context.Context) (DiscardTarget, bool)
}

type ResumeRequest struct {
	Partition uint16
	Limits    *topology.Limits
	StartedAt time.Time
	Discard   *DiscardTarget
	Options   []topology.Option
}

type Resumed struct {
	Builder      *topology.Builder
	Staked       Staked
	Found        bool
	Attempts     int
	DiscardCause error
	Refused      *DiscardTarget
}

func Resume(ctx context.Context, store checkpointResumer, request ResumeRequest) (Resumed, error) {
	for attempt := firstAttempt; ; attempt++ {
		resumed, again, err := request.attempt(ctx, store, attempt)
		if !again {
			return resumed, err
		}
	}
}

func (r *ResumeRequest) attempt(ctx context.Context, store checkpointResumer, attempt int) (Resumed, bool, error) {
	resumed, err := r.once(ctx, store)
	resumed.Attempts = attempt
	resumed.Refused = refusedTarget(ctx, store, err)
	if !errors.Is(err, ErrCheckpointMoved) || attempt == movedAttempts {
		return resumed, false, err
	}
	if slept := retry.Sleep(ctx, movedPause); slept != nil {
		return resumed, false, fmt.Errorf("pause after a moved checkpoint: %w", errors.Join(err, context.Cause(ctx)))
	}
	return resumed, true, nil
}

func refusedTarget(ctx context.Context, store checkpointResumer, err error) *DiscardTarget {
	if !Discardable(err) && !errors.Is(err, ErrDiscardRefused) {
		return nil
	}
	target, refused := store.Refused(ctx)
	if !refused {
		return nil
	}
	return &target
}

func (r *ResumeRequest) once(ctx context.Context, store checkpointResumer) (Resumed, error) {
	restored, err := Restore(ctx, store, r.Partition, r.Limits, r.StartedAt, r.Options...)
	if err != nil {
		return r.afterFailure(ctx, store, err)
	}
	staked, err := store.Stake(ctx)
	if err != nil {
		return Resumed{}, fmt.Errorf("stake partition %d: %w", r.Partition, err)
	}
	return Resumed{
		Builder: restored.Builder,
		Staked:  staked,
		Found:   restored.Found,
	}, nil
}

func (r *ResumeRequest) afterFailure(ctx context.Context, store checkpointResumer, cause error) (Resumed, error) {
	if r.Discard == nil || !Discardable(cause) {
		return Resumed{}, cause
	}
	if r.Discard.Partition != r.Partition {
		return Resumed{}, fmt.Errorf("%w: the target names partition %d, partition %d starts: %w",
			ErrDiscardRefused,
			r.Discard.Partition,
			r.Partition,
			cause,
		)
	}
	return r.discarded(ctx, store, cause)
}

func (r *ResumeRequest) discarded(ctx context.Context, store checkpointResumer, cause error) (Resumed, error) {
	fresh, err := r.restoreRequest().fresh()
	if err != nil {
		return Resumed{}, err
	}
	staked, err := store.Discard(ctx, *r.Discard)
	if err != nil {
		return Resumed{}, fmt.Errorf("discard the checkpoint of partition %d: %w", r.Partition, errors.Join(err, cause))
	}
	return Resumed{
		Builder:      fresh.Builder,
		Staked:       staked,
		DiscardCause: cause,
	}, nil
}

func ResumeBudget(read time.Duration) time.Duration {
	return movedAttempts*read + (movedAttempts-1)*movedPause
}

func (r *ResumeRequest) restoreRequest() *restoreRequest {
	return &restoreRequest{
		partition: r.Partition,
		limits:    r.Limits,
		startedAt: r.StartedAt,
		options:   r.Options,
	}
}
