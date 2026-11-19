package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const pointerWriteBudget = 2 * time.Second

type nextValue func(found *pointer) (*pointerValue, error)

func (s *Store) Stake(ctx context.Context, restored *processing.CheckpointMeta) (processing.Staked, error) {
	staked, err := s.stake(ctx, restored)
	return staked, translate(err)
}

func (s *Store) StakeEmpty(ctx context.Context) (processing.Staked, error) {
	staked, err := s.stakeEmpty(ctx)
	return staked, translate(err)
}

func (s *Store) stake(ctx context.Context, restored *processing.CheckpointMeta) (processing.Staked, error) {
	known := s.remembered()
	if known.verdict != outcomeRestorable || restored == nil || restored.Position != known.meta.Position {
		return processing.Staked{}, fmt.Errorf("%w: stake needs the checkpoint Latest returned", errStoreState)
	}
	if err := s.write(ctx, s.restakeOf(known.current)); err != nil {
		return processing.Staked{}, err
	}
	return processing.Staked{
		After:      known.meta.Position,
		ReceivedAt: known.meta.ReceivedAt,
		Created:    known.created,
	}, nil
}

func (s *Store) stakeEmpty(ctx context.Context) (processing.Staked, error) {
	known := s.remembered()
	if known.verdict != outcomeEmpty {
		return processing.Staked{}, fmt.Errorf("%w: stake empty needs Latest without a checkpoint", errStoreState)
	}
	if err := s.write(ctx, s.emptyOf(known.revision)); err != nil {
		return processing.Staked{}, err
	}
	return s.emptyStaked(), nil
}

func (s *Store) restakeOf(restored *objectRef) nextValue {
	return func(found *pointer) (*pointerValue, error) {
		if found == nil || !sameObject(found.value.Current, restored) {
			return nil, fmt.Errorf("%w: current is %q, restored %q", errPointerMoved, found.currentNUID(), restored.NUID)
		}
		return s.restaked(found), nil
	}
}

func (s *Store) emptyOf(revision uint64) nextValue {
	return func(found *pointer) (*pointerValue, error) {
		if revisionOf(found) != revision || found.currentNUID() != "" {
			return nil, fmt.Errorf("%w: revision %d after Latest saw %d", errPointerMoved, revisionOf(found), revision)
		}
		return s.emptied(found), nil
	}
}

func (s *Store) emptyStaked() processing.Staked {
	return processing.Staked{
		After: processing.Position{
			Partition: s.options.Partition,
		},
	}
}

func (s *Store) restaked(found *pointer) *pointerValue {
	value := found.value
	value.Epoch = s.options.Epoch
	return &value
}

func (s *Store) emptied(found *pointer) *pointerValue {
	value := pointerValue{
		Version: pointerVersion,
		Epoch:   s.options.Epoch,
	}
	if found != nil {
		value.Previous = found.value.Previous
		value.DiscardedAt = found.value.DiscardedAt
	}
	return &value
}

func sameObject(have, want *objectRef) bool {
	return have != nil && want != nil && have.Name == want.Name && have.NUID == want.NUID && have.Digest == want.Digest
}

func (s *Store) write(ctx context.Context, next nextValue) error {
	if err := s.staked(); err != nil {
		return err
	}
	written, keyed, err := s.writeOnce(ctx, next)
	if err != nil {
		s.unstake()
		return err
	}
	if keyed {
		s.sweep(ctx, written)
	}
	return nil
}

func (s *Store) writeOnce(ctx context.Context, next nextValue) (*pointer, bool, error) {
	if err := s.leaseHeld(); err != nil {
		return nil, false, err
	}
	found, err := s.currentPointer(ctx)
	if err != nil {
		return nil, false, err
	}
	value, err := next(found)
	if err != nil {
		return nil, false, err
	}
	written, err := s.put(ctx, found, value)
	return written, found != nil && !found.corrupt, err
}

func (s *Store) leaseHeld() error {
	if !s.lease.Valid() {
		return fmt.Errorf("%w: partition %d", errLeaseLost, s.options.Partition)
	}
	return nil
}

func (s *Store) currentPointer(ctx context.Context) (*pointer, error) {
	found, err := s.readPointer(ctx)
	switch {
	case errors.Is(err, errNoPointer):
		return nil, nil
	case errors.Is(err, errCorrupt):
		return &found, nil
	case err != nil:
		return nil, err
	case found.value.Epoch > s.options.Epoch:
		return nil, fmt.Errorf("%w: epoch %d over %d", errEpochRegressed, found.value.Epoch, s.options.Epoch)
	}
	return &found, nil
}

func (s *Store) put(ctx context.Context, found *pointer, value *pointerValue) (*pointer, error) {
	data, err := encodePointer(value)
	if err != nil {
		return nil, err
	}
	revision, err := s.send(ctx, found, data)
	if err == nil {
		return &pointer{value: *value, revision: revision}, nil
	}
	if !natsjs.Temporary(err) && !conflict(err) {
		return nil, err
	}
	return s.confirm(ctx, value)
}

func (s *Store) send(ctx context.Context, found *pointer, data []byte) (uint64, error) {
	if err := s.leaseHeld(); err != nil {
		return 0, err
	}
	attempt, cancel := context.WithTimeout(ctx, pointerWriteBudget)
	defer cancel()
	if found == nil {
		return s.buckets.pointers.Create(attempt, s.options.pointerKey(), data)
	}
	return s.buckets.pointers.Update(attempt, s.options.pointerKey(), data, found.revision)
}

func conflict(err error) bool {
	return errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch)
}

func (s *Store) confirm(ctx context.Context, value *pointerValue) (*pointer, error) {
	found, err := s.readPointer(ctx)
	if err != nil && !errors.Is(err, errNoPointer) {
		return nil, err
	}
	if err != nil || !sameValue(&found.value, value) {
		return nil, fmt.Errorf("%w: the pointer write was not applied", errPointerMoved)
	}
	return &found, nil
}

func sameValue(have, want *pointerValue) bool {
	return have.Epoch == want.Epoch && have.DiscardedAt == want.DiscardedAt &&
		sameRef(have.Current, want.Current) && sameRef(have.Previous, want.Previous)
}

func sameRef(have, want *objectRef) bool {
	if have == nil || want == nil {
		return have == want
	}
	return *have == *want
}

func (s *Store) unstake() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateStaked {
		s.state = stateOpened
	}
}
