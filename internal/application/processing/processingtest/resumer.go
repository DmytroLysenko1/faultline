package processingtest

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

type CheckpointResumer interface {
	Latest(ctx context.Context, partition uint16) (processing.CheckpointMeta, io.ReadCloser, error)
	Stake(ctx context.Context) (processing.Staked, error)
	Discard(ctx context.Context, target processing.DiscardTarget) (processing.Staked, error)
	Refused(ctx context.Context) (processing.DiscardTarget, bool)
}

type resumeVerdict uint8

const (
	verdictRefused resumeVerdict = iota
	verdictEmpty
	verdictUnusable
	verdictFound
	verdictRestorable
)

type resumeState struct {
	read       bool
	partition  uint16
	generation uint64
	movedUpTo  uint64
	verdict    resumeVerdict
	found      storedCheckpoint
	staked     *processing.Staked
	discarded  int
	streamFrom uint64
	failStake  error
	failReads  error
	discards   int
}

func (r *resumeState) begin(partition uint16) {
	r.read = true
	r.partition = partition
	r.generation++
	r.found = storedCheckpoint{}
}

func (r *resumeState) foundCheckpoint(stored *storedCheckpoint) {
	r.verdict = verdictFound
	r.found = *stored
}

func verdictOf(err error) resumeVerdict {
	switch {
	case errors.Is(err, processing.ErrNoCheckpoint):
		return verdictEmpty
	case processing.Discardable(err):
		return verdictUnusable
	}
	return verdictRefused
}

func (v resumeVerdict) discardable() bool {
	return v == verdictUnusable || v == verdictRestorable
}

func CheckpointName(meta *processing.CheckpointMeta) string {
	return fmt.Sprintf("%d-%d", meta.Epoch, meta.Position.Sequence)
}

func (s *CheckpointStore) settleRead(generation uint64, verdict resumeVerdict) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != s.resume.generation {
		return
	}
	s.resume.verdict = verdict
}

func (s *CheckpointStore) readFailure(generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation <= s.resume.movedUpTo {
		return fmt.Errorf("the pointer left the checkpoint: %w", processing.ErrCheckpointMoved)
	}
	return s.resume.failReads
}

func (s *CheckpointStore) FailReads(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resume.failReads = err
}

func (s *CheckpointStore) MovePointer(partition uint16, meta *processing.CheckpointMeta, data []byte) {
	s.mu.Lock()
	s.resume.movedUpTo = s.resume.generation
	s.mu.Unlock()
	s.Put(partition, meta, data)
}

func (s *CheckpointStore) Remove(partition uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.latest, partition)
}

func (s *CheckpointStore) Stake(ctx context.Context) (processing.Staked, error) {
	if err := ctx.Err(); err != nil {
		return processing.Staked{}, fmt.Errorf("stake: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resume.staked != nil {
		return processing.Staked{}, fmt.Errorf("partition already staked: %w", processing.ErrCheckpointRefused)
	}
	if s.resume.failStake != nil {
		return processing.Staked{}, s.resume.failStake
	}
	staked, err := s.stakeOf()
	if err != nil {
		return processing.Staked{}, err
	}
	s.resume.staked = &staked
	return staked, nil
}

func (s *CheckpointStore) stakeOf() (processing.Staked, error) {
	switch s.resume.verdict {
	case verdictRestorable:
		return processing.Staked{
			After:      s.resume.found.meta.Position,
			ReceivedAt: s.resume.found.meta.ReceivedAt,
			Created:    s.resume.found.meta.StreamCreated,
		}, nil
	case verdictEmpty:
		return s.emptyStaked(), nil
	case verdictRefused, verdictUnusable, verdictFound:
	}
	return processing.Staked{}, fmt.Errorf("stake needs a checkpoint read to its end, or none: %w",
		processing.ErrCheckpointRefused)
}

func (s *CheckpointStore) emptyStaked() processing.Staked {
	return processing.Staked{
		After: processing.Position{
			Partition: s.resume.partition,
		},
	}
}

func (s *CheckpointStore) Discard(ctx context.Context, target processing.DiscardTarget) (processing.Staked, error) {
	if err := ctx.Err(); err != nil {
		return processing.Staked{}, fmt.Errorf("discard: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resume.discards++
	if err := s.discardRefusal(target); err != nil {
		return processing.Staked{}, err
	}
	delete(s.latest, s.resume.partition)
	staked := s.emptyStaked()
	s.resume.staked = &staked
	s.resume.discarded++
	return staked, nil
}

func (s *CheckpointStore) discardRefusal(target processing.DiscardTarget) error {
	if !s.mayDiscard() {
		return fmt.Errorf("discard needs a Latest whose checkpoint is unusable or restorable: %w",
			processing.ErrCheckpointRefused)
	}
	return s.sameTarget(target)
}

func (s *CheckpointStore) sameTarget(target processing.DiscardTarget) error {
	if refused := s.refusedTarget(); target != refused {
		return fmt.Errorf("target %+v, refused %+v: %w", target, refused, processing.ErrDiscardRefused)
	}
	return nil
}

func (s *CheckpointStore) refusedTarget() processing.DiscardTarget {
	target := processing.DiscardTarget{
		Partition:  s.resume.partition,
		Checkpoint: s.foundName(),
	}
	if s.resume.found.data == nil {
		target.FirstSeq = s.resume.streamFrom
	}
	return target
}

func (s *CheckpointStore) Refused(_ context.Context) (processing.DiscardTarget, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mayDiscard() {
		return processing.DiscardTarget{}, false
	}
	return s.refusedTarget(), true
}

func (s *CheckpointStore) StartStreamAt(sequence uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resume.streamFrom = sequence
}

func (s *CheckpointStore) FailStake(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resume.failStake = err
}

func (s *CheckpointStore) mayDiscard() bool {
	return s.resume.staked == nil && s.resume.read && s.resume.verdict.discardable()
}

func (s *CheckpointStore) foundName() string {
	if s.resume.found.data == nil {
		return ""
	}
	return CheckpointName(&s.resume.found.meta)
}

func (s *CheckpointStore) StakedAs() (processing.Staked, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resume.staked == nil {
		return processing.Staked{}, false
	}
	return *s.resume.staked, true
}

func (s *CheckpointStore) DiscardCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resume.discards
}

func (s *CheckpointStore) Discards() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resume.discarded
}
