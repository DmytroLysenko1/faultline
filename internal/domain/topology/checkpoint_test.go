package topology_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

const (
	killSeed      = 20261113
	killScenarios = 300
	killSpanMax   = 30 * time.Second
)

// run is what an observer of the processor sees: every final in order and every snapshot it publishes.
type run struct {
	Finals     []traffic.Final
	Snapshots  []*topology.Snapshot
	Structures []bool
}

type journal struct {
	limits  topology.Limits
	batches []batch.Batch
	fires   []bool
	looks   []bool
}

// randomJournal mixes the step-2 generators with what a restart must survive: RED traffic with
// varied latencies and many operations (sketch arena pressure, collapse, series and per-service
// overflow), late calls inside the allowed lateness (late panes and their revisions), retried and
// duplicated halves whose ReceivedAt jumps back (a new journal leader), and heartbeats.
func randomJournal(t *testing.T, rng *rand.Rand) journal {
	t.Helper()
	limits := smallLimits(rng)
	timed := slices.Concat(redBatches(t, rng, redScenario(rng)), variedBatches(t, rng))
	slices.SortStableFunc(timed, func(a, b batch.Batch) int {
		return a.ReceivedAt.Compare(b.ReceivedAt)
	})
	var stepped []batch.Batch
	for _, step := range topologytest.RandomSteps(t, rng) {
		stepped = append(stepped, batchAt(t, step.At+time.Duration(rng.Int64N(int64(killSpanMax))), step.Spans...))
	}
	batches := withHeartbeats(rng, interleave(rng, timed, stepped))
	built := journal{
		limits:  limits,
		batches: batches,
		fires:   make([]bool, len(batches)),
		looks:   make([]bool, len(batches)),
	}
	for i := range batches {
		built.fires[i], built.looks[i] = rng.IntN(3) == 0, rng.IntN(4) == 0
	}
	return built
}

// interleave keeps the order of each list, so the stepped one brings its backward clock jumps.
func interleave(rng *rand.Rand, first, second []batch.Batch) []batch.Batch {
	out := make([]batch.Batch, 0, len(first)+len(second))
	for len(first) > 0 || len(second) > 0 {
		if len(second) == 0 || len(first) > 0 && rng.IntN(len(first)+len(second)) < len(first) {
			out, first = append(out, first[0]), first[1:]
			continue
		}
		out, second = append(out, second[0]), second[1:]
	}
	return out
}

const (
	variedCalls      = 60
	variedOperations = 9
	variedLatencyMax = int64(10 * time.Second)
	lateOneIn        = 4
	lateArrivalMin   = 35 * time.Second
	lateArrivalSpan  = 20 * time.Second
)

// variedBatches are calls with random latencies over many operations; every fourth arrives after
// the watermark passed its window but inside the allowed lateness.
func variedBatches(t *testing.T, rng *rand.Rand) []batch.Batch {
	t.Helper()
	batches := make([]batch.Batch, 0, variedCalls)
	for i := range variedCalls {
		end := telemetrytest.Epoch().Add(time.Duration(rng.Int64N(int64(redWindowsSpan * redWindow))))
		arrival := end.Add(time.Duration(rng.Int64N(int64(redMaxDisplaced))))
		if rng.IntN(lateOneIn) == 0 {
			arrival = end.Add(lateArrivalMin + time.Duration(rng.Int64N(int64(lateArrivalSpan))))
		}
		pair := timedPair(t, uint64(1000+i), topologytest.RandomService(rng), topologytest.RandomService(rng), end,
			rng.IntN(redFailOneIn) == 0)
		latency := time.Duration(1 + rng.Int64N(variedLatencyMax))
		operation := fmt.Sprintf("GET /op/%d", rng.IntN(variedOperations))
		for k := range pair {
			pair[k].Interval = eventtime.FromUnixNanos(uint64(end.Add(-latency).UnixNano()), uint64(end.UnixNano()))
			pair[k].Operation.Name = operation
		}
		batches = append(batches, receivedAt(t, arrival, pair...))
	}
	return batches
}

func smallLimits(rng *rand.Rand) topology.Limits {
	limits := topologytest.Limits()
	pairing := topologytest.RandomLimits(rng)
	limits.PairingTTL, limits.MaxPending = pairing.TTL, pairing.MaxPending
	limits.TombstoneTTL, limits.MaxTombstones = pairing.TombstoneTTL, pairing.MaxTombstones
	limits.MaxNodes = 1 + rng.IntN(topologytest.ServiceCount)
	limits.MaxEdges = 1 + rng.IntN(6)
	limits.MaxSeries = 1 + rng.IntN(12)
	limits.MaxOperationsPerService = 1 + rng.IntN(3)
	limits.SketchEntries = 1 << (11 + rng.IntN(2))
	return limits
}

func withHeartbeats(rng *rand.Rand, batches []batch.Batch) []batch.Batch {
	out := make([]batch.Batch, 0, len(batches)+len(batches)/4)
	for _, received := range batches {
		out = append(out, received)
		if rng.IntN(4) == 0 {
			out = append(out, batch.Heartbeat(received.ReceivedAt))
		}
	}
	return append(out, batch.Heartbeat(telemetrytest.Epoch().Add(time.Hour)))
}

func (j *journal) replay(t *testing.T, builder *topology.Builder, sink *finalsSink, from, to int) run {
	t.Helper()
	var observed run
	for i := from; i < to; i++ {
		builder.Apply(j.batches[i])
		if j.fires[i] {
			builder.Fire()
		}
		if j.looks[i] {
			observed.Structures = append(observed.Structures, builder.StructureChanged())
			observed.Snapshots = append(observed.Snapshots, builder.Snapshot())
		}
	}
	observed.Finals = sink.got
	return observed
}

func checkpointOf(t *testing.T, builder *topology.Builder) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := builder.WriteCheckpoint(&out); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	return out.Bytes()
}

func restored(t *testing.T, limits *topology.Limits, data []byte, sink *finalsSink) *topology.Builder {
	t.Helper()
	builder, err := topology.RestoreBuilder(limits, bytes.NewReader(data), topology.WithFinalSink(sink))
	if err != nil {
		t.Fatalf("RestoreBuilder: %v", err)
	}
	return builder
}

// TestKilledProcessorProducesTheSameFinalsAsAnUninterruptedRun (I14 through a restart): a processor
// checkpoints after batch k, keeps working, is killed, restarts from the checkpoint and replays the
// journal from k. Everything it then publishes — finals and snapshots — is what the processor that
// never stopped published after k, so step 4's idempotent write sees the same rows.
func TestKilledProcessorProducesTheSameFinalsAsAnUninterruptedRun(t *testing.T) {
	rng := rand.New(rand.NewPCG(killSeed, killSeed))
	for scenario := range killScenarios {
		j := randomJournal(t, rng)
		uninterruptedSink := &finalsSink{}
		uninterrupted := j.replay(t, newTestBuilder(t, &j.limits, topology.WithFinalSink(uninterruptedSink)),
			uninterruptedSink, 0, len(j.batches))

		killAt := rng.IntN(len(j.batches) + 1)
		firstSink := &finalsSink{}
		first := newTestBuilder(t, &j.limits, topology.WithFinalSink(firstSink))
		before := j.replay(t, first, firstSink, 0, killAt)
		data := checkpointOf(t, first)
		j.replay(t, first, firstSink, killAt, min(len(j.batches), killAt+1+rng.IntN(5)))

		secondSink := &finalsSink{}
		after := j.replay(t, restored(t, &j.limits, data, secondSink), secondSink, killAt, len(j.batches))

		got := run{
			Finals:     slices.Concat(before.Finals, after.Finals),
			Snapshots:  slices.Concat(before.Snapshots, after.Snapshots),
			Structures: slices.Concat(before.Structures, after.Structures),
		}
		if diff := cmp.Diff(uninterrupted, got); diff != "" {
			t.Fatalf("scenario %d, killed after batch %d of %d: restart changed what was published "+
				"(-uninterrupted +restarted):\n%s", scenario, killAt, len(j.batches), diff)
		}
	}
}

// TestACheckpointIsAFunctionOfTheState: writing a restored builder gives the same bytes, so equal
// states give equal checkpoints and a restore loses nothing the writer saw.
func TestACheckpointIsAFunctionOfTheState(t *testing.T) {
	rng := rand.New(rand.NewPCG(killSeed+1, killSeed))
	for scenario := range killScenarios / 3 {
		j := randomJournal(t, rng)
		sink := &finalsSink{}
		builder := newTestBuilder(t, &j.limits, topology.WithFinalSink(sink))
		j.replay(t, builder, sink, 0, rng.IntN(len(j.batches)+1))
		data := checkpointOf(t, builder)

		again := checkpointOf(t, restored(t, &j.limits, data, &finalsSink{}))

		if !bytes.Equal(data, again) {
			t.Fatalf("scenario %d: a restored builder writes a different checkpoint (%d vs %d bytes)",
				scenario, len(data), len(again))
		}
	}
}

func TestAnEmptyBuilderRestoresToAnEmptyBuilder(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())

	back := restored(t, builderLimits(), checkpointOf(t, builder), &finalsSink{})

	if diff := cmp.Diff(builder.Snapshot(), back.Snapshot()); diff != "" {
		t.Errorf("empty snapshot changed (-written +restored):\n%s", diff)
	}
}

func steadyCheckpoint(t *testing.T) []byte {
	t.Helper()
	builder, _ := steadyBuilder(t)
	return checkpointOf(t, builder)
}

// TestEveryShrunkLimitIsIncompatible: a smaller capacity cannot hold what the checkpoint holds, and a
// different window geometry would place open windows elsewhere — both refuse before any state is read.
func TestEveryShrunkLimitIsIncompatible(t *testing.T) {
	data := steadyCheckpoint(t)
	tests := []struct {
		name   string
		change func(*topology.Limits)
	}{
		{name: "max pending", change: func(l *topology.Limits) { l.MaxPending-- }},
		{name: "max tombstones", change: func(l *topology.Limits) { l.MaxTombstones-- }},
		{name: "max nodes", change: func(l *topology.Limits) { l.MaxNodes-- }},
		{name: "max edges", change: func(l *topology.Limits) { l.MaxEdges-- }},
		{name: "max series", change: func(l *topology.Limits) { l.MaxSeries-- }},
		{name: "max operations per service", change: func(l *topology.Limits) { l.MaxOperationsPerService-- }},
		{name: "sketch entries", change: func(l *topology.Limits) { l.SketchEntries /= 2 }},
		{name: "window size", change: func(l *topology.Limits) { l.Windows.Size *= 2 }},
		{name: "watermark delay", change: func(l *topology.Limits) { l.Windows.Delay += time.Second }},
		{name: "allowed lateness", change: func(l *topology.Limits) { l.Windows.AllowedLateness += time.Second }},
		{name: "slack windows", change: func(l *topology.Limits) { l.Windows.SlackWindows++ }},
		{name: "pairing ttl", change: func(l *topology.Limits) { l.PairingTTL-- }},
		{name: "tombstone ttl", change: func(l *topology.Limits) { l.TombstoneTTL-- }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := builderLimits()
			tt.change(limits)

			_, err := topology.RestoreBuilder(limits, bytes.NewReader(data))

			if !errors.Is(err, topology.ErrCheckpointIncompatible) {
				t.Errorf("err = %v, want ErrCheckpointIncompatible", err)
			}
		})
	}
}

// TestRestoreIntoGrownLimitsContinuesLikeTheOriginal: raising a capacity (an upgrade with bigger
// limits) keeps the state, and while traffic stays below the old limits everything published —
// finals, early panes, the snapshot and its stats — is the same; only the capacities read larger.
func TestRestoreIntoGrownLimitsContinuesLikeTheOriginal(t *testing.T) {
	tests := []struct {
		name   string
		change func(*topology.Limits)
	}{
		{name: "max pending", change: func(l *topology.Limits) { l.MaxPending *= 2 }},
		{name: "max tombstones", change: func(l *topology.Limits) { l.MaxTombstones *= 2 }},
		{name: "max nodes", change: func(l *topology.Limits) { l.MaxNodes *= 2 }},
		{name: "max edges", change: func(l *topology.Limits) { l.MaxEdges *= 2 }},
		{name: "max series", change: func(l *topology.Limits) { l.MaxSeries *= 2 }},
		{name: "max operations per service", change: func(l *topology.Limits) { l.MaxOperationsPerService *= 2 }},
		{name: "sketch entries", change: func(l *topology.Limits) { l.SketchEntries *= 4 }},
		{name: "every capacity at once", change: func(l *topology.Limits) {
			for _, capacity := range []*int{&l.MaxPending, &l.MaxTombstones, &l.MaxNodes, &l.MaxEdges, &l.MaxSeries,
				&l.MaxOperationsPerService, &l.SketchEntries} {
				*capacity *= 2
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalSink := &finalsSink{}
			base := belowEveryLimit()
			original, rest := steadyBuilderWith(t, &base, originalSink)
			grown := belowEveryLimit()
			tt.change(&grown)
			grownSink := &finalsSink{}
			upgraded := restored(t, &grown, checkpointOf(t, original), grownSink)
			originalSink.got = nil

			want, got := runSteady(original, originalSink, rest), runSteady(upgraded, grownSink, rest)

			if diff := cmp.Diff(want, got, ignoreCapacities()); diff != "" {
				t.Fatalf("grown limits changed what was published (-original +grown):\n%s", diff)
			}
		})
	}
}

// TestLongerTTLsAndAnotherEarlyFirePeriodAreAccepted: a longer pairing or tombstone TTL only makes
// new halves and tombstones wait longer, and the early fire period only shapes early panes, so a
// restore accepts them; the behaviour then follows the new limits.
func TestLongerTTLsAndAnotherEarlyFirePeriodAreAccepted(t *testing.T) {
	data := steadyCheckpoint(t)
	limits := builderLimits()
	limits.PairingTTL, limits.TombstoneTTL = 2*limits.PairingTTL, 2*limits.TombstoneTTL
	limits.EarlyFirePeriod /= 2

	if _, err := topology.RestoreBuilder(limits, bytes.NewReader(data)); err != nil {
		t.Errorf("RestoreBuilder: %v", err)
	}
}

// belowEveryLimit leaves room for the steady traffic: a minute of its tombstones would fill the
// default hundred, and an eviction there would differ from a grown limit for a good reason.
func belowEveryLimit() topology.Limits {
	limits := topologytest.Limits()
	limits.MaxTombstones = steadyTombstoneRoom
	return limits
}

const steadyTombstoneRoom = 1 << 16

func steadyBuilderWith(t *testing.T, limits *topology.Limits, sink *finalsSink) (*topology.Builder, []batch.Batch) {
	t.Helper()
	batches := steadyBatches(t, steadyWarmBatches+steadyRuns)
	builder := newTestBuilder(t, limits, topology.WithFinalSink(sink))
	for _, received := range batches[:steadyWarmBatches] {
		builder.Apply(received)
		builder.Fire()
	}
	return builder, batches[steadyWarmBatches:]
}

func runSteady(builder *topology.Builder, sink *finalsSink, batches []batch.Batch) run {
	var observed run
	for i, received := range batches {
		builder.Apply(received)
		builder.Fire()
		if i%10 == 0 {
			observed.Structures = append(observed.Structures, builder.StructureChanged())
			observed.Snapshots = append(observed.Snapshots, builder.Snapshot())
		}
	}
	observed.Finals = sink.got
	return observed
}

func ignoreCapacities() cmp.Option {
	return cmp.FilterPath(func(path cmp.Path) bool {
		name := path.Last().String()
		return name == ".Capacity" || name == ".Budget"
	}, cmp.Ignore())
}

func TestLimitsHashSeesEveryLimit(t *testing.T) {
	base := builderLimits()
	changes := []func(*topology.Limits){
		func(l *topology.Limits) { l.PairingTTL++ },
		func(l *topology.Limits) { l.TombstoneTTL++ },
		func(l *topology.Limits) { l.EarlyFirePeriod++ },
		func(l *topology.Limits) { l.Windows.Size++ },
		func(l *topology.Limits) { l.Windows.Delay++ },
		func(l *topology.Limits) { l.Windows.AllowedLateness++ },
		func(l *topology.Limits) { l.Windows.SlackWindows++ },
		func(l *topology.Limits) { l.MaxPending++ },
		func(l *topology.Limits) { l.MaxTombstones++ },
		func(l *topology.Limits) { l.MaxNodes++ },
		func(l *topology.Limits) { l.MaxEdges++ },
		func(l *topology.Limits) { l.MaxSeries++ },
		func(l *topology.Limits) { l.MaxOperationsPerService++ },
		func(l *topology.Limits) { l.SketchEntries++ },
	}
	if base.Hash() != builderLimits().Hash() {
		t.Fatalf("equal limits hash differently")
	}
	for i, change := range changes {
		changed := builderLimits()
		change(changed)
		if changed.Hash() == base.Hash() {
			t.Errorf("change %d does not move the hash", i)
		}
	}
}

func TestAnotherVersionIsIncompatible(t *testing.T) {
	data := steadyCheckpoint(t)
	binary.LittleEndian.PutUint32(data[4:], topology.CheckpointVersion+1)

	_, err := topology.RestoreBuilder(builderLimits(), bytes.NewReader(data))

	if !errors.Is(err, topology.ErrCheckpointIncompatible) {
		t.Errorf("err = %v, want ErrCheckpointIncompatible", err)
	}
}

// TestADamagedCheckpointIsRejected: a flipped byte anywhere, a cut or an extra byte is an error
// (CRC32C per chunk, the section order and the end of the stream), never a quietly different state.
func TestADamagedCheckpointIsRejected(t *testing.T) {
	data := steadyCheckpoint(t)
	rng := rand.New(rand.NewPCG(killSeed, 3))
	damaged := [][]byte{
		data[:len(data)-1],
		data[:len(data)/2],
		append(slices.Clone(data), 0),
		nil,
	}
	for range 200 {
		flipped := slices.Clone(data)
		flipped[rng.IntN(len(flipped))] ^= byte(1 + rng.IntN(255))
		damaged = append(damaged, flipped)
	}
	for i, input := range damaged {
		_, err := topology.RestoreBuilder(builderLimits(), bytes.NewReader(input))
		if !errors.Is(err, topology.ErrCheckpointCorrupt) && !errors.Is(err, topology.ErrCheckpointIncompatible) {
			t.Errorf("damage %d: err = %v, want a corrupt or incompatible checkpoint", i, err)
		}
	}
}

// reseal recomputes every chunk checksum, so a test reaches the decoders behind the CRC — as a bug
// in the writer would.
func reseal(data []byte) []byte {
	const header, frame, crc = 8, 5, 4
	out := slices.Clone(data)
	for at := header; at+frame <= len(out); {
		length := int(binary.LittleEndian.Uint32(out[at+1:]))
		end := at + frame + length
		if end+crc > len(out) {
			return out
		}
		sum := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), out[at:at+frame])
		sum = crc32.Update(sum, crc32.MakeTable(crc32.Castagnoli), out[at+frame:end])
		binary.LittleEndian.PutUint32(out[end:], sum)
		at = end + crc
	}
	return out
}

// mirroredBuilder sees the same calls as steadyBuilder but with every batch's spans reversed, so its
// nodes and edges get other handles.
func mirroredBuilder(tb testing.TB, count int) *topology.Builder {
	tb.Helper()
	builder, err := topology.NewBuilder(builderLimits(), telemetrytest.Epoch())
	if err != nil {
		tb.Fatalf("NewBuilder: %v", err)
	}
	for _, received := range steadyBatches(tb, count) {
		spans := slices.Clone(received.Spans)
		slices.Reverse(spans)
		mirrored, err := batch.New(spans, received.ReceivedAt)
		if err != nil {
			tb.Fatalf("batch.New: %v", err)
		}
		builder.Apply(mirrored)
	}
	return builder
}

const (
	checkpointHeaderBytes = 8
	chunkFrameBytes       = 5
	chunkCRCBytes         = 4
)

// sections splits a checkpoint into its header and whole sections, each with every chunk and its
// end chunk, so a test can swap a section between two checkpoints and keep every CRC valid.
func sections(data []byte) ([]byte, [][]byte) {
	var split [][]byte
	start := checkpointHeaderBytes
	for at := start; at+chunkFrameBytes <= len(data); {
		length := int(binary.LittleEndian.Uint32(data[at+1:]))
		at += chunkFrameBytes + length + chunkCRCBytes
		if length == 0 {
			split = append(split, data[start:at])
			start = at
		}
	}
	return data[:checkpointHeaderBytes], split
}

func spliced(first, second []byte, section int) []byte {
	header, own := sections(first)
	_, other := sections(second)
	out := slices.Clone(header)
	for i := range own {
		part := own[i]
		if i == section {
			part = other[i]
		}
		out = append(out, part...)
	}
	return out
}

func writeTB(tb testing.TB, builder *topology.Builder) []byte {
	tb.Helper()
	var out bytes.Buffer
	if err := builder.WriteCheckpoint(&out); err != nil {
		tb.Fatalf("WriteCheckpoint: %v", err)
	}
	return out.Bytes()
}

// TestSectionsFromAnotherBuilderAreRefused: each section is valid on its own, but taken from a
// builder whose nodes have other handles it disagrees with the rest (review: the traffic section
// of another graph passed restore and later released an operation under the wrong owner).
func TestSectionsFromAnotherBuilderAreRefused(t *testing.T) {
	steady, _ := steadyBuilder(t)
	own, other := writeTB(t, steady), writeTB(t, mirroredBuilder(t, steadyWarmBatches))
	_, parts := sections(own)
	if len(parts) != 5 {
		t.Fatalf("%d sections, want 5", len(parts))
	}
	for _, section := range []int{3, 4} {
		_, err := topology.RestoreBuilder(builderLimits(), bytes.NewReader(spliced(own, other, section)))
		if !errors.Is(err, topology.ErrCheckpointCorrupt) {
			t.Errorf("section %d from another builder: err = %v, want ErrCheckpointCorrupt", section+1, err)
		}
	}
}

// FuzzCheckpointDecode: whatever bytes arrive — CRC-valid or not, whole sections swapped between two
// builders — RestoreBuilder returns an error or a builder that keeps applying, firing and showing
// snapshots without a panic.
func FuzzCheckpointDecode(f *testing.F) {
	steady, _ := steadyBuilder(f)
	own, other := writeTB(f, steady), writeTB(f, mirroredBuilder(f, steadyWarmBatches))
	f.Add(own, uint16(0), byte(0))
	f.Add(other, uint16(0), byte(0))
	_, parts := sections(own)
	for section := range parts {
		f.Add(spliced(own, other, section), uint16(0), byte(0))
	}
	batches := steadyBatches(f, 8)
	f.Fuzz(func(t *testing.T, data []byte, at uint16, flip byte) {
		mutated := slices.Clone(data)
		if len(mutated) > 0 {
			mutated[int(at)%len(mutated)] ^= flip
		}
		for _, input := range [][]byte{mutated, reseal(mutated)} {
			builder, err := topology.RestoreBuilder(builderLimits(), bytes.NewReader(input))
			if err != nil {
				continue
			}
			for _, received := range batches {
				builder.Apply(received)
				builder.Fire()
			}
			builder.Snapshot()
		}
	})
}

func BenchmarkWriteCheckpoint(b *testing.B) {
	builder, _ := steadyBuilder(b)
	var out bytes.Buffer
	for b.Loop() {
		out.Reset()
		if err := builder.WriteCheckpoint(&out); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(out.Len()), "bytes")
}

func BenchmarkRestoreBuilder(b *testing.B) {
	builder, _ := steadyBuilder(b)
	var out bytes.Buffer
	if err := builder.WriteCheckpoint(&out); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := topology.RestoreBuilder(builderLimits(), bytes.NewReader(out.Bytes())); err != nil {
			b.Fatal(err)
		}
	}
}
