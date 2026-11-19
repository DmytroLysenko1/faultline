package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	pointerRereads  = 3
	firstJournalSeq = 1
	startCallBudget = 10 * time.Second
)

var (
	errObjectMismatch = errors.New("checkpoint object does not match its pointer")
	errNoPointer      = errors.New("checkpoint pointer key not found")
	errNoObject       = errors.New("checkpoint object not found")
	errStreamNotFound = errors.New("partition stream not found")
)

func (s *Store) Latest(ctx context.Context, partition uint16) (processing.CheckpointMeta, io.ReadCloser, error) {
	meta, reader, err := s.latestFor(ctx, partition)
	return meta, reader, translate(err)
}

func (s *Store) latestFor(ctx context.Context, partition uint16) (processing.CheckpointMeta, io.ReadCloser, error) {
	if partition != s.options.Partition {
		return processing.CheckpointMeta{}, nil, fmt.Errorf("%w: store of partition %d asked for %d",
			processing.ErrForeignCheckpoint, s.options.Partition, partition)
	}
	if err := s.open(); err != nil {
		return processing.CheckpointMeta{}, nil, err
	}
	if err := s.acquire(ctx); err != nil {
		return processing.CheckpointMeta{}, nil, err
	}
	defer s.release()
	meta, reader, err := s.latestOf(ctx)
	s.settle(err)
	return meta, reader, err
}

func (s *Store) latestOf(ctx context.Context) (processing.CheckpointMeta, io.ReadCloser, error) {
	revisions := make([]uint64, 0, pointerRereads)
	for range pointerRereads {
		meta, reader, err := s.latestOnce(ctx)
		if !errors.Is(err, errObjectMismatch) {
			return meta, reader, err
		}
		revisions = append(revisions, s.remembered().revision)
	}
	return processing.CheckpointMeta{}, nil, s.mismatched(revisions)
}

func (s *Store) mismatched(revisions []uint64) error {
	last := len(revisions) - 1
	if revisions[last] != revisions[last-1] {
		return fmt.Errorf("%w: partition %d pointer kept moving while its object was looked up", errPointerMoved,
			s.options.Partition)
	}
	return fmt.Errorf("%w: partition %d object does not match its pointer at revision %d", errCorrupt,
		s.options.Partition, revisions[last])
}

func (s *Store) latestOnce(ctx context.Context) (processing.CheckpointMeta, io.ReadCloser, error) {
	found, err := s.readPointer(ctx)
	switch {
	case errors.Is(err, errNoPointer):
		return processing.CheckpointMeta{}, nil, s.withoutPointer(ctx)
	case errors.Is(err, errCorrupt):
		return processing.CheckpointMeta{}, nil, s.unreadable(ctx, &found, err)
	case err != nil:
		return processing.CheckpointMeta{}, nil, err
	}
	state, err := s.checkPointer(ctx, &found)
	if err != nil {
		return processing.CheckpointMeta{}, nil, err
	}
	return s.openCurrent(ctx, &found, state)
}

func (s *Store) withoutPointer(ctx context.Context) error {
	state, err := s.describe(ctx)
	switch {
	case errors.Is(err, errStreamNotFound):
		s.remember(&address{})
		return processing.ErrNoCheckpoint
	case err != nil:
		return err
	}
	s.remember(&address{firstSeq: state.State.FirstSeq})
	if state.State.FirstSeq > firstJournalSeq {
		return fmt.Errorf("%w: partition %d stream starts at %d", errCheckpointMissing, s.options.Partition,
			state.State.FirstSeq)
	}
	return processing.ErrNoCheckpoint
}

func (s *Store) unreadable(ctx context.Context, found *pointer, cause error) error {
	state, err := s.describe(ctx)
	if err != nil && !errors.Is(err, errStreamNotFound) {
		return err
	}
	s.remember(&address{
		revision: found.revision,
		firstSeq: firstSeqOf(state),
	})
	return cause
}

func firstSeqOf(state *jetstream.StreamInfo) uint64 {
	if state == nil {
		return 0
	}
	return state.State.FirstSeq
}

func (s *Store) checkPointer(ctx context.Context, found *pointer) (*jetstream.StreamInfo, error) {
	s.remember(&address{
		revision: found.revision,
		current:  found.value.Current,
	})
	if err := s.notLater(found); err != nil {
		return nil, err
	}
	state, err := s.describe(ctx)
	if errors.Is(err, errStreamNotFound) {
		return nil, s.streamGone(found)
	}
	if err != nil {
		return nil, err
	}
	s.remember(&address{
		revision: found.revision,
		current:  found.value.Current,
		firstSeq: state.State.FirstSeq,
	})
	return state, sameStream(found, state)
}

func sameStream(found *pointer, state *jetstream.StreamInfo) error {
	if !found.created.IsZero() && !found.created.Equal(state.Created) {
		return fmt.Errorf("%w: pointer was taken from the stream created %s, the stream was created %s",
			errStreamRecreated, found.created, state.Created)
	}
	return nil
}

func (s *Store) notLater(found *pointer) error {
	if found.value.Epoch > s.options.Epoch {
		return fmt.Errorf("%w: epoch %d over %d", errEpochRegressed, found.value.Epoch, s.options.Epoch)
	}
	return nil
}

func (s *Store) streamGone(found *pointer) error {
	if found.value.Current == nil && found.created.IsZero() {
		return processing.ErrNoCheckpoint
	}
	return fmt.Errorf("%w: partition %d stream is gone", errStreamRecreated, s.options.Partition)
}

func (s *Store) openCurrent(ctx context.Context, found *pointer, state *jetstream.StreamInfo) (
	processing.CheckpointMeta, io.ReadCloser, error,
) {
	ref := found.value.Current
	if ref == nil {
		return processing.CheckpointMeta{}, nil, s.emptyCurrent(found, state)
	}
	info, err := s.matchingInfo(ctx, ref)
	if err != nil {
		return processing.CheckpointMeta{}, nil, err
	}
	meta, err := s.metaOf(info, ref)
	if err != nil {
		return processing.CheckpointMeta{}, nil, err
	}
	meta.StreamCreated = found.created
	s.remember(&address{
		revision: found.revision,
		current:  ref,
		firstSeq: state.State.FirstSeq,
		meta:     meta,
		created:  found.created,
	})
	return meta, s.reader(ctx, info), nil
}

func (s *Store) metaOf(info *jetstream.ObjectInfo, ref *objectRef) (processing.CheckpointMeta, error) {
	meta, err := decodeMeta(info.Headers, s.options.Partition)
	if err != nil {
		return processing.CheckpointMeta{}, err
	}
	epoch, position, _ := parseName(ref.Name)
	if meta.Position.Sequence != position || meta.Epoch != epoch {
		return processing.CheckpointMeta{}, fmt.Errorf("%w: object %q says position %d and epoch %d",
			errCorrupt, ref.Name, meta.Position.Sequence, meta.Epoch)
	}
	return meta, nil
}

func (s *Store) emptyCurrent(found *pointer, state *jetstream.StreamInfo) error {
	if state.State.FirstSeq > max(firstJournalSeq, found.value.DiscardedAt) {
		return fmt.Errorf("%w: partition %d pointer is empty, stream starts at %d, discarded at %d",
			errCheckpointMissing, s.options.Partition, state.State.FirstSeq, found.value.DiscardedAt)
	}
	return processing.ErrNoCheckpoint
}

func (s *Store) matchingInfo(ctx context.Context, ref *objectRef) (*jetstream.ObjectInfo, error) {
	info, err := s.infoOf(ctx, ref.Name)
	if errors.Is(err, errNoObject) {
		return nil, errObjectMismatch
	}
	if err != nil {
		return nil, err
	}
	if !matches(info, ref) {
		return nil, errObjectMismatch
	}
	return info, nil
}

func matches(info *jetstream.ObjectInfo, ref *objectRef) bool {
	return info.Name == ref.Name && info.NUID == ref.NUID && info.Digest == ref.Digest && info.Size == ref.Size
}

func (s *Store) stillCurrent(ctx context.Context, nuid string) error {
	found, err := s.readPointer(ctx)
	if err != nil && !errors.Is(err, errNoPointer) {
		return err
	}
	if found.currentNUID() != nuid {
		return fmt.Errorf("%w: pointer now names %q", errObjectChanged, found.currentNUID())
	}
	return nil
}

func (s *Store) readPointer(ctx context.Context) (pointer, error) {
	var entry jetstream.KeyValueEntry
	err := startCall(ctx, func(attempt context.Context) error {
		var err error
		entry, err = s.buckets.pointers.Get(attempt, s.options.pointerKey())
		return err
	})
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return pointer{}, errNoPointer
	}
	if err != nil {
		return pointer{}, err
	}
	found, err := decodePointer(entry.Value(), entry.Revision(), s.options.MaxBytes)
	if err != nil {
		return pointer{revision: entry.Revision(), corrupt: true}, err
	}
	return found, nil
}

func (s *Store) describe(ctx context.Context) (*jetstream.StreamInfo, error) {
	var state *jetstream.StreamInfo
	err := startCall(ctx, func(attempt context.Context) error {
		var err error
		state, err = s.buckets.describe(attempt)
		return err
	})
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, errStreamNotFound
	}
	return state, err
}

func (s *Store) infoOf(ctx context.Context, name string) (*jetstream.ObjectInfo, error) {
	var info *jetstream.ObjectInfo
	err := startCall(ctx, func(attempt context.Context) error {
		var err error
		info, err = s.buckets.objects.GetInfo(attempt, name)
		return err
	})
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, errNoObject
	}
	return info, bucketFailure(err)
}

func bucketFailure(err error) error {
	if errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrBucketNotFound) {
		return fmt.Errorf("%w: object bucket is gone: %w", errCheckpointMissing, err)
	}
	return err
}

func startCall(ctx context.Context, call func(attempt context.Context) error) error {
	budget, cancel := context.WithTimeout(ctx, startCallBudget)
	defer cancel()
	return retrying(budget, call)
}
