package checkpoint

import (
	"context"
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

func (s *Store) Discard(ctx context.Context, target processing.DiscardTarget) (processing.Staked, error) {
	staked, err := s.discard(ctx, target)
	return staked, translate(err)
}

func (s *Store) discard(ctx context.Context, target processing.DiscardTarget) (processing.Staked, error) {
	if err := s.acquire(ctx); err != nil {
		return processing.Staked{}, err
	}
	defer s.release()
	known := s.remembered()
	if err := s.targeted(target, &known); err != nil {
		return processing.Staked{}, err
	}
	if err := s.write(ctx, s.discardOf(&known)); err != nil {
		return processing.Staked{}, err
	}
	return s.emptyStaked(), nil
}

func (s *Store) discardOf(known *address) nextValue {
	return func(found *pointer) (*pointerValue, error) {
		if revisionOf(found) != known.revision || found.currentNUID() != nuidOf(known.current) {
			return nil, fmt.Errorf("%w: the pointer moved after Latest", errPointerMoved)
		}
		return s.discarded(found, known.firstSeq), nil
	}
}

func (s *Store) targeted(target processing.DiscardTarget, known *address) error {
	if !known.read || !known.verdict.discardable() {
		return fmt.Errorf("%w: discard needs a Latest that found an unusable or restorable checkpoint", errStoreState)
	}
	return s.sameTarget(target, known)
}

func (s *Store) sameTarget(target processing.DiscardTarget, known *address) error {
	if target.Partition != s.options.Partition || target.Checkpoint != nuidOf(known.current) {
		return fmt.Errorf("%w: target p%d %q, refused p%d %q", errDiscardTarget, target.Partition, target.Checkpoint,
			s.options.Partition, nuidOf(known.current))
	}
	if known.current == nil && target.FirstSeq != known.firstSeq {
		return fmt.Errorf("%w: target first sequence %d, stream %d", errDiscardTarget, target.FirstSeq, known.firstSeq)
	}
	return nil
}

func (s *Store) discarded(found *pointer, firstSeq uint64) *pointerValue {
	value := pointerValue{
		Version:     pointerVersion,
		Epoch:       s.options.Epoch,
		DiscardedAt: firstSeq,
	}
	if found != nil {
		value.Previous = found.value.Current
	}
	return &value
}

func revisionOf(found *pointer) uint64 {
	if found == nil {
		return 0
	}
	return found.revision
}

func nuidOf(ref *objectRef) string {
	if ref == nil {
		return ""
	}
	return ref.NUID
}
