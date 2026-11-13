package processing_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	partition       = uint16(3)
	receiveStep     = time.Second
	everyThird      = 3 * receiveStep
	neverFire       = 1000 * time.Hour
	publishEach     = 1
	callsPerBatch   = 2
	tenBatches      = 10
	stepBetweenEnds = 1500 * time.Millisecond
)

var (
	errSpoolFull    = errors.New("spool disk full")
	errUploadLost   = errors.New("upload lost")
	errStoreOffline = errors.New("checkpoint store offline")
	errAbortStuck   = errors.New("spool cannot be removed")
)

type lastPublisher struct {
	last   *topology.Snapshot
	count  int
	fail   error
	cancel context.CancelFunc
}

func (p *lastPublisher) Publish(ctx context.Context, snapshot *topology.Snapshot) error {
	if p.cancel != nil {
		p.cancel()
		return fmt.Errorf("publish: %w", ctx.Err())
	}
	if p.fail != nil {
		return p.fail
	}
	p.last, p.count = snapshot, p.count+1
	return nil
}

// steadyJournal is size batches of calls a second and a half apart, then a heartbeat far ahead
// that closes every window.
func steadyJournal(t *testing.T, size int) []batch.Batch {
	t.Helper()
	batches := make([]batch.Batch, 0, size+1)
	end := telemetrytest.Epoch()
	for index := range size {
		end = end.Add(stepBetweenEnds)
		batches = append(batches, processingtest.TimedPairs(t, uint64(index*callsPerBatch), callsPerBatch, end))
	}
	return append(batches, batch.Heartbeat(end.Add(heartbeatAhead)))
}

// processor is one pipeline over a retained log: every Receive hands out PerReceive batches and
// moves the clock a second; it publishes after every Receive and never fires early panes, so what
// it publishes is a function of the log alone.
type processor struct {
	reader    *processingtest.LogReader
	publisher *lastPublisher
	pipeline  *processing.Pipeline
	clock     *processingtest.SteppingClock
}

type processorArgs struct {
	log        *processingtest.RetainedLog
	from       uint64
	builder    *topology.Builder
	store      *processingtest.CheckpointStore
	interval   time.Duration
	perReceive int
}

func newProcessor(t *testing.T, args *processorArgs) *processor {
	t.Helper()
	reader, err := args.log.Open(processing.Position{Partition: partition, Sequence: args.from})
	if err != nil {
		t.Fatalf("open the log at %d: %v", args.from, err)
	}
	clock := &processingtest.SteppingClock{At: telemetrytest.Epoch(), Step: receiveStep}
	reader.Clock, reader.PerReceive = clock, args.perReceive
	built := &processor{reader: reader, publisher: &lastPublisher{}, clock: clock}
	built.pipeline, err = processing.NewPipeline(reader, args.builder, built.publisher, args.store, clock,
		processing.Cadence{FirePeriod: neverFire, MaxHeldBytes: publishEach, CheckpointInterval: args.interval})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	return built
}

func stepping(t *testing.T, size int, interval time.Duration) (*processor, *processingtest.CheckpointStore,
	*processingtest.RetainedLog,
) {
	t.Helper()
	log := processingtest.NewRetainedLog(partition, steadyJournal(t, size)[:size])
	store := processingtest.NewCheckpointStore(t, log)
	return newProcessor(t, &processorArgs{
		log: log, from: 1, builder: processingtest.NewBuilder(t), store: store, interval: interval, perReceive: 1,
	}), store, log
}

func (p *processor) run(t *testing.T) {
	t.Helper()
	if err := p.pipeline.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func assertCommitted(t *testing.T, store *processingtest.CheckpointStore, want []uint64) {
	t.Helper()
	if diff := gocmp.Diff(want, committedThrough(store)); diff != "" {
		t.Errorf("committed through (-want +got):\n%s", diff)
	}
}

func committedThrough(store *processingtest.CheckpointStore) []uint64 {
	var through []uint64
	for _, meta := range store.Committed() {
		through = append(through, meta.Position.Sequence)
	}
	return through
}

func totalCalls(p *processor) uint64 {
	if p.publisher.last == nil {
		return 0
	}
	return processingtest.TotalCalls(p.publisher.last)
}

// TestACheckpointCoversExactlyWhatWasPublished: a checkpoint starts only after the snapshot with
// every applied batch is published and released, and names the last of them, so a restart neither
// skips nor repeats a batch.
func TestACheckpointCoversExactlyWhatWasPublished(t *testing.T) {
	log := processingtest.NewRetainedLog(partition, steadyJournal(t, tenBatches))
	store := processingtest.NewCheckpointStore(t, log)
	p := newProcessor(t, &processorArgs{
		log: log, from: 1, builder: processingtest.NewBuilder(t), store: store, interval: 2 * receiveStep, perReceive: 2,
	})
	var problems []string
	store.OnBegin(func(meta *processing.CheckpointMeta) {
		problems = append(problems, coverageProblems(p, log, meta)...)
	})

	p.run(t)

	if len(problems) > 0 {
		t.Errorf("checkpoints that do not cover what was published:\n%v", problems)
	}
	assertCommitted(t, store, []uint64{4, 8, tenBatches + 1})
}

func coverageProblems(p *processor, log *processingtest.RetainedLog, meta *processing.CheckpointMeta) []string {
	handedOut := p.reader.HandedOut()
	var problems []string
	if want := uint64(len(handedOut)); meta.Position != (processing.Position{Partition: partition, Sequence: want}) {
		problems = append(problems, fmt.Sprintf("position %+v, but %d batches were applied", meta.Position, want))
	}
	if last := handedOut[len(handedOut)-1].Batch().ReceivedAt; !meta.ReceivedAt.Equal(last) {
		problems = append(problems, fmt.Sprintf("received at %s, the last applied batch at %s", meta.ReceivedAt, last))
	}
	for _, delivery := range handedOut {
		if delivery.Released() != 1 {
			problems = append(problems, fmt.Sprintf("batch %d released %d times before its checkpoint",
				delivery.Position().Sequence, delivery.Released()))
		}
	}
	if first := log.FirstRetained(); first != 1 && first > meta.Position.Sequence {
		problems = append(problems, fmt.Sprintf("retention at %d before the checkpoint through %d was committed",
			first, meta.Position.Sequence))
	}
	return problems
}

// TestCheckpointsFollowTheInterval: one batch a second and a three-second interval checkpoint after
// the third, sixth and ninth batch, and the stop writes the last one.
func TestCheckpointsFollowTheInterval(t *testing.T) {
	p, store, log := stepping(t, tenBatches, everyThird)

	p.run(t)

	assertCommitted(t, store, []uint64{3, 6, 9, tenBatches})
	if got := log.FirstRetained(); got != tenBatches+1 {
		t.Errorf("first retained = %d, want %d: the journal keeps what the last checkpoint covers", got, tenBatches+1)
	}
	want := processing.CheckpointStats{
		Spooled: 4, SpooledThrough: processing.Position{Partition: partition, Sequence: tenBatches},
	}
	if diff := gocmp.Diff(want, p.pipeline.CheckpointStats(), cmpopts.EquateErrors()); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

// TestCheckpointIsSkippedWhileThePreviousUploads: a busy store skips the checkpoint instead of
// queueing it; processing goes on and the next interval writes everything applied by then.
func TestCheckpointIsSkippedWhileThePreviousUploads(t *testing.T) {
	p, store, _ := stepping(t, tenBatches, everyThird)
	attempts := 0
	store.OnBegin(func(*processing.CheckpointMeta) {
		attempts++
		store.SetBusy(attempts == 1)
	})

	p.run(t)

	assertCommitted(t, store, []uint64{6, 9, tenBatches})
	want := processing.CheckpointStats{
		Spooled: 3, Skipped: 1, SpooledThrough: processing.Position{Partition: partition, Sequence: tenBatches},
	}
	if diff := gocmp.Diff(want, p.pipeline.CheckpointStats(), cmpopts.EquateErrors()); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
	if got := totalCalls(p); got != tenBatches*callsPerBatch {
		t.Errorf("published %d calls, want %d: a skipped checkpoint must not stop processing", got,
			tenBatches*callsPerBatch)
	}
}

type failOnce struct {
	attempts int
}

func (f *failOnce) first() bool {
	return f.attempts == 1
}

// TestAFailedCheckpointIsCountedAndProcessingContinues: whatever step fails, the previous checkpoint
// stays the latest, retention does not move, the half-written spool is aborted, and the next
// interval checkpoints everything applied by then.
func TestAFailedCheckpointIsCountedAndProcessingContinues(t *testing.T) {
	type want struct {
		aborted int
		err     error
	}
	tests := []struct {
		name string
		args func(store *processingtest.CheckpointStore, once *failOnce)
		want want
	}{
		{
			name: "begin fails",
			args: func(store *processingtest.CheckpointStore, once *failOnce) {
				store.OnBegin(func(*processing.CheckpointMeta) {
					once.attempts++
					store.FailBegin(failIf(once, errStoreOffline))
				})
			},
			want: want{err: errStoreOffline},
		},
		{
			name: "write fails",
			args: func(store *processingtest.CheckpointStore, once *failOnce) {
				store.OnBegin(func(*processing.CheckpointMeta) { once.attempts++ })
				store.OnWrite(func() error { return failIf(once, errSpoolFull) })
			},
			want: want{aborted: 1, err: errSpoolFull},
		},
		{
			name: "close fails",
			args: func(store *processingtest.CheckpointStore, once *failOnce) {
				store.OnBegin(func(*processing.CheckpointMeta) {
					once.attempts++
					store.FailClose(failIf(once, errUploadLost))
				})
			},
			want: want{err: errUploadLost},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, store, log := stepping(t, tenBatches, everyThird)
			once := &failOnce{}
			tt.args(store, once)

			p.run(t)

			assertFailedOnce(t, p, store, []uint64{6, 9, tenBatches}, tt.want.aborted, tt.want.err)
			if got := log.FirstRetained(); got != tenBatches+1 {
				t.Errorf("first retained = %d, want %d", got, tenBatches+1)
			}
		})
	}
}

func failIf(once *failOnce, err error) error {
	if once.first() {
		return err
	}
	return nil
}

func assertFailedOnce(t *testing.T, p *processor, store *processingtest.CheckpointStore, committed []uint64,
	aborted int, wantErr error,
) {
	t.Helper()
	assertCommitted(t, store, committed)
	stats := p.pipeline.CheckpointStats()
	if stats.Spooled != uint64(len(committed)) || stats.Failed != 1 || stats.Skipped != 0 {
		t.Errorf("stats = %+v, want %d written and one failure", stats, len(committed))
	}
	if !errors.Is(stats.LastFailure, wantErr) {
		t.Errorf("last failure = %v, want %v", stats.LastFailure, wantErr)
	}
	if got := store.Aborted(); got != aborted {
		t.Errorf("aborted %d spools, want %d", got, aborted)
	}
	if got := store.Unfinished(); got != 0 {
		t.Errorf("%d checkpoint writers left neither closed nor aborted", got)
	}
	if got := totalCalls(p); got != tenBatches*callsPerBatch {
		t.Errorf("published %d calls, want %d: a failed checkpoint must not stop processing", got,
			tenBatches*callsPerBatch)
	}
}

// TestProcessorStopWritesAFinalCheckpoint: a processor that drains its feed checkpoints what it
// applied even before the interval, so the next one starts from there and retention drops it.
func TestProcessorStopWritesAFinalCheckpoint(t *testing.T) {
	p, store, log := stepping(t, 5, time.Hour)

	p.run(t)

	assertCommitted(t, store, []uint64{5})
	if got := log.FirstRetained(); got != 6 {
		t.Errorf("first retained = %d, want 6", got)
	}
}

// TestACheckpointIsNotRepeatedWithoutNewBatches: the stop after a checkpoint of the last batch has
// nothing new to cover, so it writes nothing — a second upload of the same state would only cost.
func TestACheckpointIsNotRepeatedWithoutNewBatches(t *testing.T) {
	p, store, _ := stepping(t, 3, receiveStep)

	p.run(t)

	assertCommitted(t, store, []uint64{1, 2, 3})
}

func TestAStopWithNothingNewWritesNoCheckpoint(t *testing.T) {
	p, store, _ := stepping(t, 0, everyThird)

	p.run(t)

	if got := store.Committed(); len(got) != 0 {
		t.Errorf("committed %+v with nothing applied", got)
	}
	if got := p.pipeline.CheckpointStats(); got != (processing.CheckpointStats{}) {
		t.Errorf("stats = %+v, want none", got)
	}
}

// TestAPublishFailureWritesNoCheckpoint: the state was never published, so it must not become the
// state a restart continues from.
func TestAPublishFailureWritesNoCheckpoint(t *testing.T) {
	p, store, log := stepping(t, tenBatches, receiveStep)
	p.publisher.fail = errPublishBroken

	err := p.pipeline.Run(t.Context())

	if !errors.Is(err, errPublishBroken) {
		t.Fatalf("Run = %v, want the publish error", err)
	}
	if got := store.Committed(); len(got) != 0 {
		t.Errorf("committed %+v after a failed publish", got)
	}
	if got := log.FirstRetained(); got != 1 {
		t.Errorf("first retained = %d, want 1: nothing may be dropped", got)
	}
}

func TestACheckpointWithoutAnIntervalIsNeverWritten(t *testing.T) {
	p, store, log := stepping(t, tenBatches, 0)

	p.run(t)

	if got := store.Committed(); len(got) != 0 {
		t.Errorf("committed %+v with checkpoints off", got)
	}
	if got := log.FirstRetained(); got != 1 {
		t.Errorf("first retained = %d, want 1", got)
	}
}

func TestNewPipelineRefusesACheckpointCadenceItCannotKeep(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	tests := []struct {
		name  string
		store *processingtest.CheckpointStore
		args  time.Duration
	}{
		{name: "an interval without a store", args: time.Second},
		{name: "a negative interval", store: store, args: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sink interface {
				Begin(context.Context, *processing.CheckpointMeta) (processing.CheckpointWriter, error)
			}
			if tt.store != nil {
				sink = tt.store
			}
			cadence := processing.Cadence{FirePeriod: time.Second, MaxHeldBytes: 1, CheckpointInterval: tt.args}

			_, err := processing.NewPipeline(&scriptedFeed{}, processingtest.NewBuilder(t), &lastPublisher{}, sink,
				&fakeClock{}, cadence)

			if !errors.Is(err, processing.ErrInvalidCadence) {
				t.Errorf("NewPipeline = %v, want ErrInvalidCadence", err)
			}
		})
	}
}

// TestACheckpointIsTimed: the pause a checkpoint costs the pipeline is reported with its timings.
func TestACheckpointIsTimed(t *testing.T) {
	p, store, _ := stepping(t, 3, receiveStep)
	store.OnWrite(func() error {
		p.clock.Advance()
		return nil
	})

	p.run(t)
	widest := p.pipeline.TakeTimings()

	if widest.Checkpoint < receiveStep {
		t.Errorf("checkpoint timing = %s, want at least %s", widest.Checkpoint, receiveStep)
	}
}

// TestCheckpointStatsCanBeReadWhileThePipelineRuns: metrics read stats from another goroutine.
func TestCheckpointStatsCanBeReadWhileThePipelineRuns(t *testing.T) {
	p, _, _ := stepping(t, tenBatches, receiveStep)
	done := make(chan struct{})
	var reader sync.WaitGroup
	reader.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				p.pipeline.CheckpointStats()
			}
		}
	})

	p.run(t)
	close(done)
	reader.Wait()

	if got := p.pipeline.CheckpointStats().Spooled; got == 0 {
		t.Error("no checkpoint written")
	}
}

// TestAFailedCheckpointIsRetriedAtTheStop: the last batch was applied, its checkpoint failed and
// nothing new came; the stop still owes that checkpoint and writes it.
func TestAFailedCheckpointIsRetriedAtTheStop(t *testing.T) {
	p, store, _ := stepping(t, 3, everyThird)
	once := &failOnce{}
	store.OnBegin(func(*processing.CheckpointMeta) {
		once.attempts++
		store.FailBegin(failIf(once, errStoreOffline))
	})

	p.run(t)

	assertCommitted(t, store, []uint64{3})
	if got := p.pipeline.CheckpointStats(); got.Failed != 1 || got.Spooled != 1 {
		t.Errorf("stats = %+v, want one failure, then one spooled", got)
	}
}

func TestAnAbortThatFailsIsReportedWithTheWriteError(t *testing.T) {
	p, store, _ := stepping(t, 3, everyThird)
	store.OnWrite(func() error { return errSpoolFull })
	store.FailAbort(errAbortStuck)

	err := p.pipeline.Run(t.Context())

	if !errors.Is(err, errSpoolFull) || !errors.Is(err, errAbortStuck) {
		t.Fatalf("Run = %v, want the final checkpoint's write and abort errors", err)
	}
	last := p.pipeline.CheckpointStats().LastFailure
	if !errors.Is(last, errSpoolFull) || !errors.Is(last, errAbortStuck) {
		t.Errorf("last failure = %v, want both the write and the abort error", last)
	}
	if got := store.Unfinished(); got != 0 {
		t.Errorf("%d checkpoint writers left open", got)
	}
}

// TestALostFinalCheckpointFailsTheStop: the processor stops without the checkpoint it owed, so the
// caller learns of it; the journal still holds everything after the previous one.
func TestALostFinalCheckpointFailsTheStop(t *testing.T) {
	p, store, log := stepping(t, 5, everyThird)
	attempts := 0
	store.OnBegin(func(*processing.CheckpointMeta) {
		attempts++
		if attempts == 2 {
			store.FailBegin(errStoreOffline)
		}
	})

	err := p.pipeline.Run(t.Context())

	if !errors.Is(err, errStoreOffline) {
		t.Fatalf("Run = %v, want the final checkpoint's error", err)
	}
	assertCommitted(t, store, []uint64{3})
	if got := log.FirstRetained(); got != 4 {
		t.Errorf("first retained = %d, want 4: what the lost checkpoint covered stays in the journal", got)
	}
}

func TestABusyStoreAtTheStopOnlySkipsTheFinalCheckpoint(t *testing.T) {
	p, store, log := stepping(t, 2, everyThird)
	store.SetBusy(true)

	p.run(t)

	if got := p.pipeline.CheckpointStats(); got.Skipped != 1 || got.Failed != 0 {
		t.Errorf("stats = %+v, want one skipped", got)
	}
	if got := log.FirstRetained(); got != 1 {
		t.Errorf("first retained = %d, want 1", got)
	}
}

// TestACheckpointCutByTheCallerStopsWithoutAFailure: a shutdown that cancels the context mid-write
// aborts the spool and ends Run; it is not a checkpoint failure to alert on.
func TestACheckpointCutByTheCallerStopsWithoutAFailure(t *testing.T) {
	p, store, _ := stepping(t, tenBatches, everyThird)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store.OnWrite(func() error {
		cancel()
		return context.Canceled
	})

	err := p.pipeline.Run(ctx)

	if err != nil {
		t.Fatalf("Run = %v, want a quiet stop", err)
	}
	if got := p.pipeline.CheckpointStats(); got != (processing.CheckpointStats{}) {
		t.Errorf("stats = %+v, want nothing counted", got)
	}
	if got := len(p.reader.HandedOut()); got != 3 {
		t.Errorf("handed out %d batches, want 3: Run must stop at the cut checkpoint", got)
	}
	if got := store.Unfinished(); got != 0 {
		t.Errorf("%d checkpoint writers left open", got)
	}
}

// TestAnUploadLostAfterCloseLeavesThePreviousCheckpoint: the pipeline spooled it, the store never
// committed it — a restart continues from the one before, and the journal kept everything after it.
func TestAnUploadLostAfterCloseLeavesThePreviousCheckpoint(t *testing.T) {
	p, store, log := stepping(t, tenBatches, everyThird)
	attempts := 0
	store.OnBegin(func(*processing.CheckpointMeta) {
		attempts++
		store.LoseUploads(attempts > 1)
	})

	p.run(t)

	assertCommitted(t, store, []uint64{3})
	if got := p.pipeline.CheckpointStats().Spooled; got != 4 {
		t.Errorf("spooled %d, want 4", got)
	}
	if got := log.FirstRetained(); got != 4 {
		t.Errorf("first retained = %d, want 4", got)
	}
}

// straySink answers Begin the way the contract does not promise: a writer with an error, or
// nothing at all.
type straySink struct {
	writer  *abortCounter
	err     error
	nothing bool
}

type abortCounter struct {
	aborted int
}

func (*abortCounter) Write(data []byte) (int, error) { return len(data), nil }
func (*abortCounter) Close() error                   { return nil }
func (a *abortCounter) Abort() error {
	a.aborted++
	return nil
}

func (s *straySink) Begin(context.Context, *processing.CheckpointMeta) (processing.CheckpointWriter, error) {
	if s.nothing {
		return nil, nil
	}
	return s.writer, s.err
}

func TestAStoreThatBreaksTheBeginContractLeavesNoSpoolAndFails(t *testing.T) {
	tests := []struct {
		name        string
		args        *straySink
		wantAborted int
		wantErr     error
	}{
		{name: "a writer with an error", args: &straySink{writer: &abortCounter{}, err: errStoreOffline},
			wantAborted: 1, wantErr: errStoreOffline},
		{name: "no writer and no error", args: &straySink{writer: &abortCounter{}, nothing: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := processingtest.NewRetainedLog(partition, steadyJournal(t, 3)[:3])
			reader, err := log.Open(processing.Position{Partition: partition, Sequence: 1})
			if err != nil {
				t.Fatal(err)
			}
			pipeline, err := processing.NewPipeline(reader, processingtest.NewBuilder(t), &lastPublisher{}, tt.args,
				&processingtest.SteppingClock{At: telemetrytest.Epoch(), Step: receiveStep},
				processing.Cadence{FirePeriod: neverFire, MaxHeldBytes: publishEach, CheckpointInterval: time.Hour})
			if err != nil {
				t.Fatalf("NewPipeline: %v", err)
			}

			err = pipeline.Run(t.Context())

			if err == nil || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Errorf("Run = %v, want the lost final checkpoint (%v)", err, tt.wantErr)
			}
			if got := tt.args.writer.aborted; got != tt.wantAborted {
				t.Errorf("aborted %d writers, want %d", got, tt.wantAborted)
			}
			if got := pipeline.CheckpointStats().Failed; got != 1 {
				t.Errorf("failed = %d, want 1", got)
			}
		})
	}
}

// TestACancelDuringTheDrainIsAQuietStop: the caller gave up on the drain; that is a stop it asked
// for, not a failure to report.
func TestACancelDuringTheDrainIsAQuietStop(t *testing.T) {
	p, store, _ := stepping(t, 0, everyThird)
	ctx, cancel := context.WithCancel(t.Context())
	p.publisher.cancel = cancel

	err := p.pipeline.Run(ctx)

	if err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
	if got := store.Committed(); len(got) != 0 {
		t.Errorf("committed %+v", got)
	}
}
