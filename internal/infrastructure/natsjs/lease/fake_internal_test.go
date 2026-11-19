package lease

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fault uint8

const (
	answer fault = iota
	hang
	applyAndHang
	noStream
	slowAnswer
)

const slowAnswerDelay = 900 * time.Millisecond

// errUnmodelledOption: the fake ignores what a create option would change, so an option the lease starts to pass
// must be modelled here first.
var errUnmodelledOption = errors.New("the fake does not model this create option")

type stored struct {
	value    []byte
	revision uint64
	written  time.Time
}

// fakeStore is one key of a KV bucket: revisions come from one sequence, a value expires maxAge after
// it was written, and writes fail the way NATS fails — never answered, applied with the answer lost,
// or with the bucket gone. queued faults are used one per write before steady applies.
type fakeStore struct {
	lock     sync.Mutex
	maxAge   time.Duration
	sequence uint64
	current  *stored
	queued   []fault
	steady   fault
	stale    *entry
	once     bool
}

func newFakeStore(maxAge time.Duration) *fakeStore {
	return &fakeStore{maxAge: maxAge}
}

func (f *fakeStore) Create(ctx context.Context, _ string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	if len(opts) != 0 {
		return 0, errUnmodelledOption
	}
	return f.write(ctx, func() (uint64, error) {
		if f.live() != nil {
			return 0, jetstream.ErrKeyExists
		}
		return f.put(value), nil
	})
}

func (f *fakeStore) Update(ctx context.Context, _ string, value []byte, revision uint64) (uint64, error) {
	return f.write(ctx, func() (uint64, error) {
		if !f.at(revision) {
			return 0, jetstream.ErrKeyRevisionMismatch
		}
		return f.put(value), nil
	})
}

func (f *fakeStore) Delete(ctx context.Context, _ string, revision uint64) error {
	_, err := f.write(ctx, func() (uint64, error) {
		if !f.at(revision) {
			return 0, jetstream.ErrKeyRevisionMismatch
		}
		f.sequence++
		f.current = nil
		return f.sequence, nil
	})
	return err
}

func (f *fakeStore) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.steady == noStream {
		return nil, jetstream.ErrBucketNotFound
	}
	if f.stale != nil {
		shown := *f.stale
		shown.key = key
		if f.once {
			f.stale = nil
		}
		return &shown, nil
	}
	current := f.live()
	if current == nil {
		return nil, jetstream.ErrKeyNotFound
	}
	return &entry{key: key, value: current.value, revision: current.revision}, nil
}

func (f *fakeStore) write(ctx context.Context, apply func() (uint64, error)) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.lock.Lock()
	next := f.nextFault()
	if next == noStream {
		f.lock.Unlock()
		return 0, jetstream.ErrNoStreamResponse
	}
	if next == hang {
		f.lock.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}
	revision, err := apply()
	f.lock.Unlock()
	if next == applyAndHang {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if next == slowAnswer {
		time.Sleep(slowAnswerDelay)
	}
	return revision, err
}

func (f *fakeStore) nextFault() fault {
	if len(f.queued) == 0 {
		return f.steady
	}
	next := f.queued[0]
	f.queued = f.queued[1:]
	return next
}

func (f *fakeStore) put(value []byte) uint64 {
	f.sequence++
	f.current = &stored{value: value, revision: f.sequence, written: time.Now()}
	return f.sequence
}

func (f *fakeStore) live() *stored {
	if f.current != nil && time.Since(f.current.written) >= f.maxAge {
		f.current = nil
	}
	return f.current
}

func (f *fakeStore) at(revision uint64) bool {
	current := f.live()
	return current != nil && current.revision == revision
}

func (f *fakeStore) failNext(faults ...fault) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.queued = append(f.queued, faults...)
}

func (f *fakeStore) failAlways(next fault) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.steady = next
}

func (f *fakeStore) readStale(shown entry) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.stale = &shown
}

func (f *fakeStore) readStaleOnce(shown entry) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.stale = &shown
	f.once = true
}

func (f *fakeStore) takeOver(value []byte) uint64 {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.put(value)
}

func (f *fakeStore) held() (entry, bool) {
	f.lock.Lock()
	defer f.lock.Unlock()
	current := f.live()
	if current == nil {
		return entry{}, false
	}
	return entry{value: current.value, revision: current.revision}, true
}

// entry is what Get answers: a key's value at a revision, the rest of KeyValueEntry is not read by the lease.
type entry struct {
	key      string
	value    []byte
	revision uint64
}

func (e *entry) Bucket() string                  { return Bucket }
func (e *entry) Key() string                     { return e.key }
func (e *entry) Value() []byte                   { return e.value }
func (e *entry) Revision() uint64                { return e.revision }
func (e *entry) Created() time.Time              { return time.Time{} }
func (e *entry) Delta() uint64                   { return 0 }
func (e *entry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }
