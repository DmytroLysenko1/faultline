package checkpoint

import (
	"bufio"
	"context"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	defaultStop = 6 * time.Second
	stopMargin  = 500 * time.Millisecond
	spoolSlots  = 2
)

type retention interface {
	Created() time.Time
	Commit(ctx context.Context, through processing.Position) error
}

type spoolKind uint8

const (
	periodicSpool spoolKind = iota
	stopSpool
)

type Sink struct {
	up *uploader
}

type uploader struct {
	store          *Store
	feed           retention
	spools         chan spool
	stopping       chan struct{}
	exited         chan struct{}
	cancels        [edges]func()
	timers         [edges]*time.Timer
	slots          int
	finalTaken     bool
	finalHandedOff bool
	accepting      bool
	stopAt         time.Time
	landed         []*commitRecord
	pending        []pendingObject
	feedClosed     bool
	finalCommitted bool
	queued         *spool
	phase          uploadPhase
	left           int64
	size           int64
	measured       rates
	progress       progress
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
	upload, control, cancels := stopEdges(ctx)
	s.up = newUploader(s, feed, cancels)
	go s.up.run(upload, control)
	return s.up, nil
}

func stopEdges(ctx context.Context) (context.Context, context.Context, [edges]func()) {
	root, cancelRoot := context.WithCancel(context.WithoutCancel(ctx))
	upload, cancelUpload := context.WithCancelCause(root)
	control, cancelControl := context.WithCancelCause(root)
	return upload, control, [edges]func(){
		edgeUpload: func() {
			cancelUpload(errStopUpload)
		},
		edgeControl: func() {
			cancelControl(errStopControl)
		},
		edgeRoot: cancelRoot,
	}
}

func (s *Store) startable() error {
	switch s.state {
	case stateStaked:
		return s.notStaking()
	case stateClosed:
		return errStoreClosed
	default:
		return fmt.Errorf("%w: start needs a staked partition", errStoreState)
	}
}

func newUploader(store *Store, feed retention, cancels [edges]func()) *uploader {
	return &uploader{
		store:     store,
		feed:      feed,
		spools:    make(chan spool, spoolSlots),
		stopping:  make(chan struct{}),
		exited:    make(chan struct{}),
		cancels:   cancels,
		accepting: true,
	}
}

func (s *Sink) Begin(ctx context.Context, meta *processing.CheckpointMeta) (processing.CheckpointWriter, error) {
	return s.begin(ctx, meta, periodicSpool)
}

func (s *Sink) BeginOnStop(ctx context.Context, meta *processing.CheckpointMeta) (processing.CheckpointWriter, error) {
	return s.begin(ctx, meta, stopSpool)
}

func (s *Sink) begin(
	ctx context.Context,
	meta *processing.CheckpointMeta,
	kind spoolKind,
) (processing.CheckpointWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	writer, err := s.up.begin(meta, kind)
	if err != nil {
		return nil, translate(err)
	}
	return writer, nil
}

func (u *uploader) begin(meta *processing.CheckpointMeta, kind spoolKind) (*spoolWriter, error) {
	if err := u.ownPartition(meta); err != nil {
		return nil, err
	}
	final, err := u.reserve(kind)
	if err != nil {
		return nil, err
	}
	return u.openSpool(meta, final)
}

func (u *uploader) ownPartition(meta *processing.CheckpointMeta) error {
	if meta.Position.Partition == u.store.options.Partition {
		return nil
	}
	return fmt.Errorf("%w: store of partition %d got a checkpoint of %d", processing.ErrForeignCheckpoint,
		u.store.options.Partition, meta.Position.Partition)
}

func (u *uploader) openSpool(meta *processing.CheckpointMeta, final bool) (*spoolWriter, error) {
	file, written, err := u.store.createSpool(u.completed(meta))
	if err != nil {
		u.free()
		return nil, err
	}
	written.final = final
	return &spoolWriter{
		up:     u,
		file:   file,
		buffer: bufio.NewWriterSize(file, spoolBuffer),
		spool:  written,
	}, nil
}

func (u *uploader) reserve(kind spoolKind) (bool, error) {
	valid := u.store.lease.Valid()
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if !u.taking() {
		return false, u.refusal()
	}
	final := u.takesStopSlot(kind)
	if err := u.slotRefusal(final, valid); err != nil {
		return false, err
	}
	u.slots++
	u.finalTaken = u.finalTaken || final
	return final, nil
}

func (u *uploader) takesStopSlot(kind spoolKind) bool {
	return kind == stopSpool && !u.finalTaken
}

func (u *uploader) slotRefusal(final, valid bool) error {
	if u.slots >= slotLimit(final) {
		u.store.stats.skip(skipBusy)
		return fmt.Errorf("%w: the previous checkpoint is uploading", processing.ErrCheckpointBusy)
	}
	if !valid {
		u.store.stats.skip(skipLeaseSuspended)
		return fmt.Errorf("%w: the partition lease is suspended", processing.ErrCheckpointBusy)
	}
	return nil
}

func slotLimit(final bool) int {
	if final {
		return spoolSlots
	}
	return 1
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
	if err := u.passOn(written); err != nil {
		u.store.removeSpool(written.name)
		return err
	}
	return nil
}

func (u *uploader) passOn(written *spool) error {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if !u.taking() {
		u.slots--
		return u.refusal()
	}
	u.finalHandedOff = u.finalHandedOff || written.final
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
	u.slots--
}

func (u *uploader) stopDeadline() (time.Time, bool) {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	return u.stopAt, !u.stopAt.IsZero()
}

func (u *uploader) run(upload, control context.Context) {
	defer u.exit()
	for u.next(upload, control) {
	}
	u.leave(upload, control)
}

func (u *uploader) publishCommits() {
	for _, each := range u.landed {
		u.store.stats.commit(each)
		u.finalCommitted = u.finalCommitted || each.final
	}
	u.landed = nil
}

func (u *uploader) ended() bool {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	return u.store.failed != nil
}

func (u *uploader) next(upload, control context.Context) bool {
	if u.stopRequested() {
		return false
	}
	select {
	case <-u.stopping:
		return false
	case next := <-u.spools:
		return u.taken(upload, control, new(next))
	}
}

func (u *uploader) taken(upload, control context.Context, next *spool) bool {
	if u.stopRequested() {
		u.queueAtStop(next)
		return false
	}
	return !u.handle(upload, control, next)
}

func (u *uploader) handle(upload, control context.Context, next *spool) bool {
	err := u.cycle(upload, control, next)
	u.phase = phaseIdle
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
	terminal, left := u.closeIntake()
	for i := range left {
		u.notStarted(&left[i])
	}
	if u.finalUnsettled() {
		u.store.stats.skip(skipFinalSkipped)
	}
	u.cancelThrough(edgeRoot)
	u.signalExit(terminal)
}

func (u *uploader) closeIntake() (bool, []spool) {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	u.accepting = false
	u.stopTimers()
	left := u.queuedSpools()
	if u.queued != nil {
		left = append(left, *u.queued)
	}
	return u.store.failed != nil, left
}

func (u *uploader) signalExit(terminal bool) {
	if terminal {
		close(u.store.done)
	}
	close(u.exited)
}

func (u *uploader) notStarted(left *spool) {
	u.release(left)
	if !left.final {
		u.store.stats.skip(skipNotStarted)
	}
}

func (u *uploader) queuedSpools() []spool {
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
