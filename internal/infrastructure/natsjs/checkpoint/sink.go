package checkpoint

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	defaultStop = 6 * time.Second
	stopMargin  = 500 * time.Millisecond
	spoolSlots  = 1
)

type retention interface {
	Created() time.Time
	Commit(ctx context.Context, through processing.Position) error
}

type Sink struct {
	up *uploader
}

type uploader struct {
	store        *Store
	feed         retention
	spools       chan spool
	stopping     chan struct{}
	exited       chan struct{}
	cancel       context.CancelFunc
	cancelUpload context.CancelFunc
	busy         bool
	accepting    bool
	stopAt       time.Time
	stopTimer    *time.Timer
	landed       []*commitRecord
	pending      []pendingObject
	feedClosed   bool
}

func (s *Store) Start(ctx context.Context, feed retention) (*Sink, error) {
	up, err := s.start(ctx, feed)
	if err != nil {
		return nil, translate(err)
	}
	return &Sink{
		up: up,
	}, nil
}

func (s *Store) start(ctx context.Context, feed retention) (*uploader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.startable(); err != nil {
		return nil, err
	}
	s.state = stateStarted
	root, cancel := context.WithCancel(context.WithoutCancel(ctx))
	upload, cancelUpload := context.WithCancel(root)
	s.up = newUploader(s, feed, cancel, cancelUpload)
	go s.up.run(root, upload)
	return s.up, nil
}

func (s *Store) startable() error {
	switch s.state {
	case stateStaked:
		return nil
	case stateClosed:
		return errStoreClosed
	default:
		return fmt.Errorf("%w: start needs a staked partition", errStoreState)
	}
}

func newUploader(store *Store, feed retention, cancel, cancelUpload context.CancelFunc) *uploader {
	return &uploader{
		store:        store,
		feed:         feed,
		spools:       make(chan spool, spoolSlots),
		stopping:     make(chan struct{}),
		exited:       make(chan struct{}),
		cancel:       cancel,
		cancelUpload: cancelUpload,
		accepting:    true,
	}
}

func (s *Sink) Begin(_ context.Context, meta *processing.CheckpointMeta) (processing.CheckpointWriter, error) {
	writer, err := s.up.begin(meta)
	if err != nil {
		return nil, translate(err)
	}
	return writer, nil
}

func (u *uploader) begin(meta *processing.CheckpointMeta) (*spoolWriter, error) {
	if meta.Position.Partition != u.store.options.Partition {
		return nil, fmt.Errorf("%w: store of partition %d got a checkpoint of %d", processing.ErrForeignCheckpoint,
			u.store.options.Partition, meta.Position.Partition)
	}
	if err := u.reserve(); err != nil {
		return nil, err
	}
	file, written, err := u.store.createSpool(u.completed(meta))
	if err != nil {
		u.free()
		return nil, err
	}
	return &spoolWriter{
		up:     u,
		file:   file,
		buffer: bufio.NewWriterSize(file, spoolBuffer),
		spool:  written,
	}, nil
}

func (u *uploader) reserve() error {
	valid := u.store.lease.Valid()
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if !u.taking() {
		return u.refusal()
	}
	if u.busy {
		u.store.stats.skip(skipBusy)
		return fmt.Errorf("%w: the previous checkpoint is uploading", processing.ErrCheckpointBusy)
	}
	if !valid {
		u.store.stats.skip(skipLeaseSuspended)
		return fmt.Errorf("%w: the partition lease is suspended", processing.ErrCheckpointBusy)
	}
	u.busy = true
	return nil
}

func (u *uploader) taking() bool {
	return u.accepting && u.store.state != stateClosed && u.store.failed == nil
}

func (u *uploader) refusal() error {
	if u.store.failed != nil {
		return u.store.failed
	}
	return errStoreClosed
}

func (u *uploader) completed(meta *processing.CheckpointMeta) *processing.CheckpointMeta {
	options := &u.store.options
	completed := *meta
	completed.StreamCreated = u.feed.Created()
	completed.Epoch = options.Epoch
	completed.CheckpointVersion = topology.CheckpointVersion
	completed.RecordVersion = record.Version
	completed.LimitsHash = options.LimitsHash
	completed.RulesHash = options.RulesHash
	return &completed
}

func (u *uploader) handOff(written *spool) error {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if !u.taking() {
		u.store.removeSpool(written.name)
		u.busy = false
		return u.refusal()
	}
	u.spools <- *written
	return nil
}

func (u *uploader) release(written *spool) {
	u.store.removeSpool(written.name)
	u.free()
}

func (u *uploader) free() {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	u.busy = false
}

func (u *uploader) stop(deadline time.Time) {
	u.stopAt = deadline
	u.stopTimer = time.AfterFunc(time.Until(deadline), u.cancel)
	close(u.stopping)
	u.cancelUpload()
}

func (u *uploader) stopDeadline() (time.Time, bool) {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	return u.stopAt, !u.stopAt.IsZero()
}

func (u *uploader) run(root, upload context.Context) {
	defer u.exit()
	for {
		if !u.next(root, upload) {
			u.leave(root)
			return
		}
	}
}

func (u *uploader) leave(root context.Context) {
	if !u.stopRequested() || u.ended() {
		return
	}
	err := u.resolve(root)
	u.publishCommits()
	if outcome, known := errors.AsType[*cycleError](err); known && outcome.kind == kindTerminal {
		outcome.apply(u)
		return
	}
	u.store.stats.leftAtStop.Add(uint64(len(u.pending)))
}

func (u *uploader) publishCommits() {
	for _, record := range u.landed {
		u.store.stats.commit(record)
	}
	u.landed = nil
}

func (u *uploader) ended() bool {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	return u.store.failed != nil
}

func (u *uploader) next(root, upload context.Context) bool {
	if u.stopRequested() {
		return false
	}
	select {
	case <-u.stopping:
		return false
	case next := <-u.spools:
		return !u.handle(root, upload, new(next))
	}
}

func (u *uploader) handle(root, upload context.Context, next *spool) bool {
	err := u.cycle(root, upload, next)
	u.release(next)
	u.publishCommits()
	ended := u.record(err)
	u.store.stats.cycles.Add(1)
	return ended
}

func (u *uploader) stopRequested() bool {
	select {
	case <-u.stopping:
		return true
	default:
		return false
	}
}

func (u *uploader) exit() {
	u.store.mu.Lock()
	u.accepting = false
	queued := u.drain()
	terminal := u.store.failed != nil
	if u.stopTimer != nil {
		u.stopTimer.Stop()
	}
	u.store.mu.Unlock()
	for _, left := range queued {
		u.store.removeSpool(left.name)
	}
	u.cancel()
	if terminal {
		close(u.store.done)
	}
	close(u.exited)
}

func (u *uploader) drain() []spool {
	var queued []spool
	for {
		select {
		case left := <-u.spools:
			queued = append(queued, left)
		default:
			return queued
		}
	}
}

func (u *uploader) terminate(cause error) {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	u.store.failed = cause
}
