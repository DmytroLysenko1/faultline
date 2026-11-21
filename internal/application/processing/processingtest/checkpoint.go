package processingtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const finalSlots = 2

var (
	errWriterFinished = errors.New("checkpoint writer already closed or aborted")
	errReaderClosed   = errors.New("checkpoint reader already closed")
)

type Retention interface {
	Commit(ctx context.Context, through processing.Position) error
}

type storedCheckpoint struct {
	meta processing.CheckpointMeta
	data []byte
}

type CheckpointStore struct {
	tb          testing.TB
	retention   Retention
	mu          sync.Mutex
	latest      map[uint16]storedCheckpoint
	committed   []processing.CheckpointMeta
	begun       int
	finished    int
	aborted     int
	openReaders int
	busy        bool
	failBegin   error
	failClose   error
	failLatest  error
	failRead    error
	failAbort   error
	loseUploads bool
	inFlight    int
	finalTaken  bool
	holding     bool
	held        []*spoolWriter
	closed      bool
	onWrite     func() error
	onBegin     func(*processing.CheckpointMeta)
}

func NewCheckpointStore(tb testing.TB, retention Retention) *CheckpointStore {
	tb.Helper()
	return &CheckpointStore{tb: tb, retention: retention, latest: map[uint16]storedCheckpoint{}}
}

func (s *CheckpointStore) SetBusy(busy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.busy = busy
}

func (s *CheckpointStore) FailBegin(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failBegin = err
}

func (s *CheckpointStore) FailClose(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failClose = err
}

func (s *CheckpointStore) FailLatest(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLatest = err
}

func (s *CheckpointStore) FailReaderClose(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRead = err
}

func (s *CheckpointStore) FailAbort(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAbort = err
}

func (s *CheckpointStore) LoseUploads(lose bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loseUploads = lose
}

func (s *CheckpointStore) OnWrite(hook func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onWrite = hook
}

func (s *CheckpointStore) OnBegin(hook func(*processing.CheckpointMeta)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onBegin = hook
}

func (s *CheckpointStore) Put(partition uint16, meta *processing.CheckpointMeta, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[partition] = storedCheckpoint{meta: *meta, data: bytes.Clone(data)}
}

func (s *CheckpointStore) Begin(ctx context.Context, meta *processing.CheckpointMeta,
) (processing.CheckpointWriter, error) {
	return s.begin(ctx, meta, false)
}

func (s *CheckpointStore) BeginOnStop(ctx context.Context, meta *processing.CheckpointMeta,
) (processing.CheckpointWriter, error) {
	return s.begin(ctx, meta, true)
}

func (s *CheckpointStore) begin(
	ctx context.Context,
	meta *processing.CheckpointMeta,
	final bool,
) (processing.CheckpointWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("begin through %d: %w", meta.Position.Sequence, err)
	}
	s.beforeBegin(meta)
	s.mu.Lock()
	defer s.mu.Unlock()
	consumed := final && !s.finalTaken
	if err := s.refusal(meta, consumed); err != nil {
		return nil, err
	}
	s.begun++
	s.inFlight++
	s.finalTaken = s.finalTaken || consumed
	return &spoolWriter{store: s, meta: *meta, final: consumed}, nil
}

func (s *CheckpointStore) beforeBegin(meta *processing.CheckpointMeta) {
	s.mu.Lock()
	hook := s.onBegin
	s.mu.Unlock()
	if hook != nil {
		hook(meta)
	}
}

func (s *CheckpointStore) refusal(meta *processing.CheckpointMeta, consumed bool) error {
	switch {
	case s.closed:
		return fmt.Errorf("begin through %d: %w", meta.Position.Sequence, processing.ErrCheckpointRefused)
	case s.busy || s.inFlight >= slotLimit(consumed):
		return fmt.Errorf("begin through %d: %w", meta.Position.Sequence, processing.ErrCheckpointBusy)
	default:
		return s.failBegin
	}
}

func slotLimit(final bool) int {
	if final {
		return finalSlots
	}
	return 1
}

func (s *CheckpointStore) Hold() (release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holding = true
	return func() {
		s.mu.Lock()
		s.holding = false
		held := s.held
		s.held = nil
		s.mu.Unlock()
		for _, w := range held {
			s.land(w)
		}
	}
}

func (s *CheckpointStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *CheckpointStore) Latest(_ context.Context, partition uint16,
) (processing.CheckpointMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failLatest != nil {
		return processing.CheckpointMeta{}, nil, s.failLatest
	}
	stored, found := s.latest[partition]
	if !found {
		return processing.CheckpointMeta{}, nil, fmt.Errorf("partition %d: %w", partition, processing.ErrNoCheckpoint)
	}
	s.openReaders++
	return stored.meta, &checkpointReader{store: s, Reader: bytes.NewReader(stored.data)}, nil
}

func (s *CheckpointStore) Committed() []processing.CheckpointMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]processing.CheckpointMeta(nil), s.committed...)
}

func (s *CheckpointStore) LatestBytes(partition uint16) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.latest[partition].data)
}

func (s *CheckpointStore) Aborted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborted
}

func (s *CheckpointStore) Unfinished() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.begun - s.finished
}

func (s *CheckpointStore) OpenReaders() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openReaders
}

func (s *CheckpointStore) write(w *spoolWriter, data []byte) (int, error) {
	s.mu.Lock()
	hook := s.onWrite
	s.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return 0, err
		}
	}
	return w.data.Write(data)
}

func (s *CheckpointStore) close(w *spoolWriter) error {
	s.mu.Lock()
	s.finished++
	if s.failClose != nil || s.loseUploads {
		defer s.mu.Unlock()
		s.inFlight--
		return s.failClose
	}
	if s.holding {
		defer s.mu.Unlock()
		s.held = append(s.held, w)
		return nil
	}
	s.mu.Unlock()
	s.land(w)
	return nil
}

func (s *CheckpointStore) land(w *spoolWriter) {
	s.mu.Lock()
	meta := w.meta
	meta.CheckpointVersion = topology.CheckpointVersion
	s.latest[meta.Position.Partition] = storedCheckpoint{meta: meta, data: bytes.Clone(w.data.Bytes())}
	s.committed = append(s.committed, meta)
	s.inFlight--
	s.mu.Unlock()
	if !w.final {
		s.retain(meta.Position)
	}
}

func (s *CheckpointStore) retain(through processing.Position) {
	if s.retention == nil {
		return
	}
	if err := s.retention.Commit(s.tb.Context(), through); err != nil {
		s.tb.Errorf("retention could not follow the checkpoint through %+v: %v", through, err)
	}
}

func (s *CheckpointStore) abort() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished++
	s.aborted++
	s.inFlight--
	return s.failAbort
}

func (s *CheckpointStore) readerClosed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openReaders--
	return s.failRead
}

type spoolWriter struct {
	store    *CheckpointStore
	meta     processing.CheckpointMeta
	data     bytes.Buffer
	finished bool
	final    bool
}

func (w *spoolWriter) Write(data []byte) (int, error) {
	if w.finished {
		return 0, errWriterFinished
	}
	return w.store.write(w, data)
}

func (w *spoolWriter) Close() error {
	if w.finished {
		return errWriterFinished
	}
	w.finished = true
	return w.store.close(w)
}

func (w *spoolWriter) Abort() error {
	if w.finished {
		return nil
	}
	w.finished = true
	return w.store.abort()
}

type checkpointReader struct {
	*bytes.Reader
	store  *CheckpointStore
	closed bool
}

func (r *checkpointReader) Close() error {
	if r.closed {
		return errReaderClosed
	}
	r.closed = true
	return r.store.readerClosed()
}
