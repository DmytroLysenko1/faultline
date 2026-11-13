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

// CheckpointStore keeps the latest checkpoint per partition the way the real store promises: a
// checkpoint becomes latest only after its writer closes, and only then does retention move up to
// it. The real store uploads after Close, so an upload it loses — or retention it cannot move — is
// never an error of Close.
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
	retainErrs  []error
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

// LoseUploads makes closed checkpoints vanish, the way an upload or a pointer update that fails after
// Close does: the previous checkpoint stays the latest and retention stays where it was.
func (s *CheckpointStore) LoseUploads(lose bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loseUploads = lose
}

func (s *CheckpointStore) RetentionFailures() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.retainErrs...)
}

// OnWrite runs before every Write of the checkpoint; an error it returns is what Write returns.
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

// Put makes a checkpoint the latest of partition as is, the way a damaged or misfiled one would be.
func (s *CheckpointStore) Put(partition uint16, meta *processing.CheckpointMeta, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[partition] = storedCheckpoint{meta: *meta, data: bytes.Clone(data)}
}

func (s *CheckpointStore) Begin(_ context.Context, meta *processing.CheckpointMeta,
) (processing.CheckpointWriter, error) {
	s.mu.Lock()
	hook := s.onBegin
	s.mu.Unlock()
	if hook != nil {
		hook(meta)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy {
		return nil, fmt.Errorf("begin through %d: %w", meta.Position.Sequence, processing.ErrCheckpointBusy)
	}
	if s.failBegin != nil {
		return nil, s.failBegin
	}
	s.begun++
	return &spoolWriter{store: s, meta: *meta}, nil
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

// Unfinished counts writers the pipeline neither closed nor aborted.
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
		return s.failClose
	}
	meta := w.meta
	meta.CheckpointVersion = topology.CheckpointVersion
	s.latest[meta.Position.Partition] = storedCheckpoint{meta: meta, data: bytes.Clone(w.data.Bytes())}
	s.committed = append(s.committed, meta)
	s.mu.Unlock()
	s.retain(meta.Position)
	return nil
}

func (s *CheckpointStore) retain(through processing.Position) {
	if s.retention == nil {
		return
	}
	if err := s.retention.Commit(s.tb.Context(), through); err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.retainErrs = append(s.retainErrs, err)
	}
}

func (s *CheckpointStore) abort() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished++
	s.aborted++
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
		return errWriterFinished
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
