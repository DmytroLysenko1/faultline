package restart_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

const (
	killSeed       = 20261113
	killScenarios  = 100
	partition      = uint16(1)
	minBatches     = 20
	extraBatches   = 60
	maxPerReceive  = 4
	maxInterval    = 4
	maxCalls       = 4
	maxEndStepMs   = 3000
	services       = 5
	killInWriteOdd = 3
	receiveStep    = time.Second
	neverFire      = 1000 * time.Hour
	publishEach    = 1
	heartbeatAhead = 10 * time.Minute
	earlyFire      = 2 * time.Second
	killSeries     = 64
	killSketch     = 1 << 11
)

// firePeriods: early panes fire on the clock, which starts over with the restarted processor; the
// finals must not depend on when they fired.
var firePeriods = []time.Duration{neverFire, earlyFire}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// killLimits keep the state small, so a hundred processors checkpoint every few seconds quickly.
func killLimits() topology.Limits {
	limits := topologytest.Limits()
	limits.MaxSeries, limits.SketchEntries = killSeries, killSketch
	return limits
}

func newBuilder(t *testing.T, sink *finalsSink) *topology.Builder {
	t.Helper()
	builder, err := topology.NewBuilder(new(killLimits()), telemetrytest.Epoch(), topology.WithFinalSink(sink))
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	return builder
}

type finalsSink struct {
	got []traffic.Final
}

func (s *finalsSink) Final(final *traffic.Final) {
	s.got = append(s.got, *final)
}

type lastPublisher struct {
	last *topology.Snapshot
}

func (p *lastPublisher) Publish(_ context.Context, snapshot *topology.Snapshot) error {
	p.last = snapshot
	return nil
}

// randomJournal is calls between a few services with random counts and gaps, so windows close,
// pairs meet across batches and services and edges appear at different positions; a heartbeat far
// ahead closes every window at the end.
func randomJournal(t *testing.T, rng *rand.Rand) []batch.Batch {
	t.Helper()
	size := minBatches + rng.IntN(extraBatches)
	batches := make([]batch.Batch, 0, size+1)
	end, trace := telemetrytest.Epoch(), uint64(0)
	for range size {
		end = end.Add(time.Duration(rng.IntN(maxEndStepMs)) * time.Millisecond)
		calls := 1 + rng.IntN(maxCalls)
		caller, callee := fmt.Sprintf("svc-%d", rng.IntN(services)), fmt.Sprintf("svc-%d", rng.IntN(services))
		batches = append(batches, processingtest.TimedCalls(t, trace, calls, caller, callee, end))
		trace += uint64(calls)
	}
	return append(batches, batch.Heartbeat(end.Add(heartbeatAhead)))
}

type scenario struct {
	batches    []batch.Batch
	perReceive int
	interval   time.Duration
	firePeriod time.Duration
}

type outcome struct {
	Finals []traffic.Final
	Calls  uint64
	Nodes  int
	Edges  int
}

type run struct {
	reader  *processingtest.LogReader
	store   *processingtest.CheckpointStore
	builder *topology.Builder
}

func openAt(t *testing.T, log *processingtest.RetainedLog, from uint64) *processingtest.LogReader {
	t.Helper()
	reader, err := log.Open(processing.Position{Partition: partition, Sequence: from})
	if err != nil {
		t.Fatalf("open the log at %d (first retained %d): %v", from, log.FirstRetained(), err)
	}
	return reader
}

func (s *scenario) process(ctx context.Context, t *testing.T, r *run) *lastPublisher {
	t.Helper()
	clock := &processingtest.SteppingClock{At: telemetrytest.Epoch(), Step: receiveStep}
	r.reader.Clock, r.reader.PerReceive = clock, s.perReceive
	publisher := &lastPublisher{}
	pipeline, err := processing.NewPipeline(r.reader, r.builder, publisher, r.store, clock,
		processing.Cadence{FirePeriod: s.firePeriod, MaxHeldBytes: publishEach, CheckpointInterval: s.interval})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	if err := pipeline.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return publisher
}

func (s *scenario) uninterrupted(t *testing.T) outcome {
	t.Helper()
	log := processingtest.NewRetainedLog(partition, s.batches)
	sink := &finalsSink{}
	publisher := s.process(t.Context(), t, &run{
		reader: openAt(t, log, 1), store: processingtest.NewCheckpointStore(t, log), builder: newBuilder(t, sink),
	})
	return observed(sink.got, publisher)
}

func observed(finals []traffic.Final, publisher *lastPublisher) outcome {
	return outcome{
		Finals: finals,
		Calls:  processingtest.TotalCalls(publisher.last),
		Nodes:  len(publisher.last.Nodes),
		Edges:  len(publisher.last.Edges),
	}
}

// killed runs a processor that dies at a random Receive — or, one time in three, while it writes a
// checkpoint — and returns, for every checkpoint it began, the finals published by then. They are
// keyed by how many batches the processor had been handed, not by what the checkpoint claims, so a
// checkpoint that names the wrong position restarts from finals that do not match.
func (s *scenario) killed(t *testing.T, rng *rand.Rand, log *processingtest.RetainedLog,
) (*processingtest.CheckpointStore, map[uint64][]traffic.Final) {
	t.Helper()
	store := processingtest.NewCheckpointStore(t, log)
	sink := &finalsSink{}
	reader := openAt(t, log, 1)
	finalsAt := map[uint64][]traffic.Final{0: nil}
	store.OnBegin(func(*processing.CheckpointMeta) {
		finalsAt[uint64(len(reader.HandedOut()))] = slices.Clone(sink.got)
	})
	ctx, kill := context.WithCancel(t.Context())
	defer kill()
	if rng.IntN(killInWriteOdd) == 0 {
		store.OnWrite(killOnWrite(rng, kill))
	}
	reader.KillAfter, reader.Kill = 1+rng.IntN(len(s.batches)/s.perReceive+2), kill
	s.process(ctx, t, &run{reader: reader, store: store, builder: newBuilder(t, sink)})
	if store.Unfinished() != 0 {
		t.Fatalf("%d checkpoint writers left open by the killed processor's pipeline", store.Unfinished())
	}
	return store, finalsAt
}

func killOnWrite(rng *rand.Rand, kill context.CancelFunc) func() error {
	writes, killAt := 0, 1+rng.IntN(maxInterval*maxPerReceive)
	return func() error {
		writes++
		if writes < killAt {
			return nil
		}
		kill()
		return context.Canceled
	}
}

// TestARestartedPipelinePublishesTheSameFinalsAsAnUninterruptedOne (I14 through a restart, KT-1 at
// the pipeline): a processor checkpoints on its cadence and is killed at a random moment, sometimes
// in the middle of a checkpoint; the next one restores the last committed checkpoint and reads the
// journal from right after it. The finals the first published up to that checkpoint, followed by
// everything the second publishes, are the finals of a processor that never stopped — nothing is
// lost to retention, nothing applied twice.
func TestARestartedPipelinePublishesTheSameFinalsAsAnUninterruptedOne(t *testing.T) {
	rng := rand.New(rand.NewPCG(killSeed, killSeed))
	for index := range killScenarios {
		s := &scenario{
			batches:    randomJournal(t, rng),
			perReceive: 1 + rng.IntN(maxPerReceive),
			interval:   time.Duration(1+rng.IntN(maxInterval)) * receiveStep,
			firePeriod: firePeriods[rng.IntN(len(firePeriods))],
		}
		want := s.uninterrupted(t)
		log := processingtest.NewRetainedLog(partition, s.batches)
		store, finalsAt := s.killed(t, rng, log)

		got, through := s.restarted(t, log, store, finalsAt)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("scenario %d: restarted after a checkpoint through %d of %d (-uninterrupted +restarted):\n%s",
				index, through, len(s.batches), diff)
		}
	}
}

func (s *scenario) restarted(t *testing.T, log *processingtest.RetainedLog, store *processingtest.CheckpointStore,
	finalsAt map[uint64][]traffic.Final,
) (outcome, uint64) {
	t.Helper()
	sink := &finalsSink{}
	restored, err := processing.Restore(t.Context(), store, partition, new(killLimits()), telemetrytest.Epoch(),
		topology.WithFinalSink(sink))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	before, found := finalsAt[restored.Through.Sequence]
	if !found {
		t.Fatalf("restored through %d, which no checkpoint began at", restored.Through.Sequence)
	}
	publisher := s.process(t.Context(), t, &run{
		reader: openAt(t, log, restored.Through.Next().Sequence), store: processingtest.NewCheckpointStore(t, log),
		builder: restored.Builder,
	})
	if publisher.last == nil {
		publisher.last = restored.Builder.Snapshot()
	}
	return observed(slices.Concat(before, sink.got), publisher), restored.Through.Sequence
}

func TestAKilledProcessorWithoutACheckpointRestartsFromTheFirstRecord(t *testing.T) {
	log := processingtest.NewRetainedLog(partition, randomJournal(t, rand.New(rand.NewPCG(1, 1))))
	store := processingtest.NewCheckpointStore(t, log)
	restored, err := processing.Restore(t.Context(), store, partition, new(processingtest.Limits()), telemetrytest.Epoch())

	if err != nil || restored.Found {
		t.Fatalf("Restore = %+v, %v; want a fresh start", restored, err)
	}
	if _, err := log.Open(restored.Through.Next()); err != nil {
		t.Errorf("the first record is not readable: %v", err)
	}
	if _, err := log.Open(restored.Through); !errors.Is(err, processingtest.ErrRetentionPassed) {
		t.Errorf("opening before the first record = %v, want ErrRetentionPassed", err)
	}
}
