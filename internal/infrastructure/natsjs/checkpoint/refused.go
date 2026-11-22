package checkpoint

import (
	"context"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

func (s *Store) Refused(_ context.Context) (processing.DiscardTarget, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mayDiscard() {
		return processing.DiscardTarget{}, false
	}
	return s.targetOf(&s.latest), true
}

func (s *Store) mayDiscard() bool {
	return s.state == stateOpened && s.staking == nil && s.latest.read && s.latest.verdict.discardable()
}

func (s *Store) targetOf(known *address) processing.DiscardTarget {
	target := processing.DiscardTarget{
		Partition:  s.options.Partition,
		Checkpoint: nuidOf(known.current),
	}
	if known.current == nil {
		target.FirstSeq = known.firstSeq
	}
	return target
}
