package feed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/pkg/atomicmax"
)

const (
	commitBudget  = 5 * time.Second
	maxPurgeBytes = 256 << 20
)

func (f *Feed) Commit(ctx context.Context, through processing.Position) error {
	if err := f.retainable(through); err != nil {
		return err
	}
	if through.Sequence <= f.counters.purged.Load() {
		return nil
	}
	committing, cancel := context.WithTimeout(ctx, commitBudget)
	defer cancel()
	target, err := f.purgeTarget(committing, through.Sequence)
	if err != nil {
		return err
	}
	return f.purge(committing, target)
}

func (f *Feed) retainable(through processing.Position) error {
	if f.isHalted() {
		return processing.ErrFeedClosed
	}
	if !f.lease.Valid() {
		return ErrLeaseSuspended
	}
	if through.Partition != f.partition {
		return fmt.Errorf("%w: %d, the feed reads %d", ErrForeignPosition, through.Partition, f.partition)
	}
	return nil
}

func (f *Feed) purgeTarget(ctx context.Context, through uint64) (uint64, error) {
	found, err := f.stream.Describe(ctx)
	if err != nil {
		return 0, describeFailure(err)
	}
	if err := found.retains(f.created, through); err != nil {
		return 0, err
	}
	if err := f.retainable(processing.Position{
		Partition: f.partition,
	}); err != nil {
		return 0, err
	}
	return found.purgeTarget(max(f.counters.purged.Load(), purgedBefore(found.First)), through), nil
}

func describeFailure(err error) error {
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("%w: the stream is gone: %w", ErrStreamIdentity, err)
	}
	return fmt.Errorf("describe the stream for retention: %w", err)
}

func (f *Feed) purge(ctx context.Context, target uint64) error {
	if err := f.stream.Purge(ctx, target+1); err != nil {
		f.counters.purgeFailures.Add(1)
		return fmt.Errorf("purge through %d: %w", target, err)
	}
	atomicmax.Raise(&f.counters.purged, target)
	return nil
}

func (i identity) retains(created time.Time, through uint64) error {
	if !i.Created.Equal(created) {
		return fmt.Errorf("%w: created %s, read from %s", ErrStreamIdentity, i.Created, created)
	}
	if through > i.Last {
		return fmt.Errorf("%w: position %d, the stream ends at %d", ErrPositionRange, through, i.Last)
	}
	return nil
}

func (i identity) purgeTarget(purged, through uint64) uint64 {
	if i.Msgs == 0 {
		return through
	}
	average := max(i.Bytes/i.Msgs, 1)
	return min(through, purged+max(maxPurgeBytes/average, 1))
}

func purgedBefore(first uint64) uint64 {
	return max(first, 1) - 1
}
