package checkpoint

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	firstSequence   = 1
	defaultReadRate = 16 << 20
	readSlack       = 30 * time.Second
)

var (
	errReadDone       = errors.New("checkpoint object read and verified")
	errReadIncomplete = errors.New("checkpoint object closed before it was read to its end")
	errNoMessage      = errors.New("no message on the subject")
)

type objectReader struct {
	fetch   func(sequence uint64) (*jetstream.RawStreamMsg, error)
	before  func(read uint64) error
	still   func() error
	settled func(err error)
	once    sync.Once
	cancel  context.CancelFunc
	info    *jetstream.ObjectInfo
	subject string
	next    uint64
	read    uint32
	total   uint64
	digest  hash.Hash
	pending []byte
	err     error
}

func (s *Store) reader(ctx context.Context, info *jetstream.ObjectInfo) *objectReader {
	readCtx, cancel := readContext(ctx, info.Size)
	reader := s.readerFor(readCtx, cancel, info, func() error {
		return s.stillCurrent(readCtx, info.NUID)
	})
	generation := s.currentGeneration()
	reader.settled = func(err error) {
		s.settleRead(generation, err)
	}
	return reader
}

func (s *Store) readerWith(ctx context.Context, info *jetstream.ObjectInfo, still func() error) *objectReader {
	readCtx, cancel := readContext(ctx, info.Size)
	return s.readerFor(readCtx, cancel, info, still)
}

func (s *Store) readerFor(
	readCtx context.Context,
	cancel context.CancelFunc,
	info *jetstream.ObjectInfo,
	still func() error,
) *objectReader {
	subject := s.chunkSubject(info.NUID)
	return &objectReader{
		fetch: func(sequence uint64) (*jetstream.RawStreamMsg, error) {
			return s.chunk(readCtx, subject, sequence)
		},
		still:   still,
		cancel:  cancel,
		info:    info,
		subject: subject,
		next:    firstSequence,
		digest:  sha256.New(),
	}
}

func readContext(ctx context.Context, size uint64) (context.Context, context.CancelFunc) {
	if _, bounded := ctx.Deadline(); bounded {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, readBudget(size))
}

func readBudget(size uint64) time.Duration {
	return time.Duration(float64(size)/defaultReadRate*float64(time.Second)) + readSlack
}

func (r *objectReader) Read(out []byte) (int, error) {
	r.fill()
	if errors.Is(r.err, errReadDone) {
		return 0, io.EOF
	}
	if len(r.pending) == 0 {
		return 0, translate(r.err)
	}
	copied := copy(out, r.pending)
	r.pending = r.pending[copied:]
	return copied, nil
}

func (r *objectReader) fill() {
	if r.err != nil {
		return
	}
	for len(r.pending) == 0 && r.err == nil {
		r.err = r.advance()
	}
	if r.err != nil {
		r.cancel()
		r.settleLast()
	}
}

func (r *objectReader) settleLast() {
	if !r.settle(r.err) && errors.Is(r.err, errReadDone) {
		r.err = fmt.Errorf("%w: %q was closed while its end was read", errReadIncomplete, r.info.Name)
	}
}

func (r *objectReader) Close() error {
	r.cancel()
	r.settle(errReadIncomplete)
	return nil
}

func (r *objectReader) settle(err error) bool {
	if r.settled == nil {
		return true
	}
	won := false
	r.once.Do(func() {
		won = true
		r.settled(err)
	})
	return won
}

func (r *objectReader) advance() error {
	if r.read == r.info.Chunks {
		return r.finish()
	}
	found, err := r.fetched(r.next)
	if errors.Is(err, errNoMessage) {
		return r.missing()
	}
	if err != nil {
		return err
	}
	if found.Subject != r.subject {
		return fmt.Errorf("%w: chunk %d of %q came from %q", errCorrupt, r.read, r.info.Name, found.Subject)
	}
	r.accept(found)
	return nil
}

func (r *objectReader) accept(found *jetstream.RawStreamMsg) {
	r.next = found.Sequence + 1
	r.read++
	r.total += uint64(len(found.Data))
	r.digest.Write(found.Data)
	r.pending = found.Data
}

func (r *objectReader) fetched(sequence uint64) (*jetstream.RawStreamMsg, error) {
	if r.before != nil {
		if err := r.before(r.total); err != nil {
			return nil, err
		}
	}
	return r.fetch(sequence)
}

func (r *objectReader) finish() error {
	_, err := r.fetched(r.next)
	if err == nil {
		return fmt.Errorf("%w: %q has chunks past its %d", errCorrupt, r.info.Name, r.info.Chunks)
	}
	if !errors.Is(err, errNoMessage) {
		return err
	}
	return r.verify()
}

func (r *objectReader) verify() error {
	digest := jetstream.GetObjectDigestValue(r.digest)
	if r.total != r.info.Size || digest != r.info.Digest {
		return fmt.Errorf("%w: %q read %d bytes with digest %s, meta says %d and %s",
			errCorrupt, r.info.Name, r.total, digest, r.info.Size, r.info.Digest)
	}
	return errReadDone
}

func (r *objectReader) missing() error {
	if err := r.still(); err != nil {
		return err
	}
	return fmt.Errorf("%w: chunk %d of %q is missing", errCorrupt, r.read, r.info.Name)
}

func (s *Store) chunk(ctx context.Context, subject string, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var found *jetstream.RawStreamMsg
	err := retrying(ctx, func(attempt context.Context) error {
		var err error
		found, err = s.buckets.chunkAt(attempt, subject, sequence)
		return err
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, errNoMessage
	}
	return found, bucketFailure(err)
}
