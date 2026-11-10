package sketch

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

const (
	invariantBudget   = 4 * MaxEntriesPerSketch
	invariantSketches = 8
	opBytes           = 9
	releaseEvery      = 17
)

func FuzzArenaInvariants(f *testing.F) {
	f.Add(slices.Repeat([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2}, 64))
	f.Add(seedOps(0x5eed, 3000))
	f.Add(seedOps(0xfeed, 6000))
	f.Fuzz(func(t *testing.T, data []byte) {
		arena, err := NewArena(invariantBudget)
		if err != nil {
			t.Fatal(err)
		}
		sketches := make([]Sketch, invariantSketches)
		for op := range slices.Chunk(data, opBytes) {
			if len(op) < opBytes {
				break
			}
			apply(arena, &sketches[int(op[0])%invariantSketches], op)
			assertArenaInvariants(t, arena, sketches)
		}
	})
}

func apply(arena *Arena, s *Sketch, op []byte) {
	if op[0]%releaseEvery == 0 {
		arena.Release(s)
		return
	}
	arena.Add(s, int64(binary.LittleEndian.Uint64(op[1:])))
}

func assertArenaInvariants(t *testing.T, arena *Arena, sketches []Sketch) {
	t.Helper()
	capacities := 0
	for i := range sketches {
		capacities += sketches[i].capacity()
		assertSketchInvariants(t, arena, &sketches[i])
	}
	if capacities != arena.Stats.InUse {
		t.Fatalf("InUse = %d, sum of capacities = %d", arena.Stats.InUse, capacities)
	}
	if free := freeEntries(&arena.blocks); free+capacities != arena.Stats.Budget {
		t.Fatalf("free %d + in use %d = %d, want the budget %d: a block leaked from the free lists", free,
			capacities, free+capacities, arena.Stats.Budget)
	}
}

func assertSketchInvariants(t *testing.T, arena *Arena, s *Sketch) {
	t.Helper()
	entries := arena.entries(s)
	total := uint64(s.zero)
	for i, e := range entries {
		if e.count() == 0 || (i > 0 && entries[i-1].index() >= e.index()) {
			t.Fatalf("entries not strictly sorted with positive counts: %v", entries)
		}
		total += e.count()
	}
	if total != s.Count || int(s.length) > s.capacity() {
		t.Fatalf("zero + Σ entries = %d, count = %d, length %d, capacity %d", total, s.Count, s.length, s.capacity())
	}
}

func TestZeroCounterSaturatesInsteadOfWrapping(t *testing.T) {
	arena, err := NewArena(MaxEntriesPerSketch)
	if err != nil {
		t.Fatal(err)
	}
	s := Sketch{zero: maxZero, Count: uint64(maxZero)}

	arena.Add(&s, 0)

	if s.zero != maxZero || s.Count != uint64(maxZero) {
		t.Fatalf("zero %d, count %d after saturation; want %d and %d", s.zero, s.Count, maxZero, maxZero)
	}
	if dropped := arena.Stats.DroppedSamples; dropped != 1 {
		t.Errorf("DroppedSamples = %d, want 1", dropped)
	}
	if got, ok := arena.Percentiles(&s); !ok || got != (Percentiles{}) {
		t.Errorf("Percentiles of an all-zero saturated sketch = %+v, %v; want zeros, true", got, ok)
	}
}

func TestEntryCountSaturatesInsteadOfCorruptingTheIndex(t *testing.T) {
	arena, err := NewArena(MaxEntriesPerSketch)
	if err != nil {
		t.Fatal(err)
	}
	s := Sketch{}
	arena.Add(&s, math.MaxInt32)
	entries := arena.entries(&s)
	index := entries[0].index()
	entries[0] = entryKey(index) | countMask

	arena.Add(&s, math.MaxInt32)

	if got := arena.entries(&s)[0]; got.index() != index || got.count() != countMask {
		t.Fatalf("entry after overflow attempt: index %d count %d; want index %d count %d", got.index(), got.count(),
			index, uint64(countMask))
	}
	if dropped := arena.Stats.DroppedSamples; dropped != 1 {
		t.Errorf("DroppedSamples = %d, want 1", dropped)
	}
}

func TestEveryBinFitsTheLargestSketchClass(t *testing.T) {
	if bins := len(newMapping().values); bins > MaxEntriesPerSketch {
		t.Fatalf("%d bins exceed MaxEntriesPerSketch %d: a sketch could need to grow past the largest class", bins,
			MaxEntriesPerSketch)
	}
}

func TestFoldThatWouldOverflowDropsTheNewSampleAndKeepsCountedOnes(t *testing.T) {
	arena, err := NewArena(MaxEntriesPerSketch)
	if err != nil {
		t.Fatal(err)
	}
	s := Sketch{}
	arena.Add(&s, math.MaxInt32/2)
	arena.Add(&s, math.MaxInt32)
	entries := arena.entries(&s)
	entries[0], entries[1] = entryKey(entries[0].index())|countMask, entryKey(entries[1].index())|countMask
	s.Count = 2 * countMask
	before := append([]entry(nil), entries...)
	fillers := fillAllBlocks(t, arena)
	droppedBefore := arena.Stats.DroppedSamples

	arena.Add(&s, math.MaxInt64)

	if !slices.Equal(arena.entries(&s), before) || s.Count != 2*countMask {
		t.Fatalf("entries %v count %d after a refused fold; want them unchanged (%v, %d)", arena.entries(&s), s.Count,
			before, uint64(2*countMask))
	}
	assertSketchInvariants(t, arena, &s)
	if dropped := arena.Stats.DroppedSamples - droppedBefore; dropped != 1 {
		t.Errorf("DroppedSamples grew by %d, want 1", dropped)
	}
	for i := range fillers {
		arena.Release(&fillers[i])
	}
}

func fillAllBlocks(t *testing.T, arena *Arena) []Sketch {
	t.Helper()
	fillers := make([]Sketch, 0, MaxEntriesPerSketch/MinEntriesPerSketch)
	for range cap(fillers) {
		fillers = append(fillers, Sketch{})
		arena.Add(&fillers[len(fillers)-1], 1)
		if arena.Stats.DroppedSamples != 0 {
			return fillers
		}
	}
	t.Fatal("arena never filled")
	return nil
}

func freeEntries(b *buddy) int {
	total := 0
	for order, head := range b.heads {
		for offset := head; offset != noBlock; offset = b.slab[offset+linkNext].link() {
			total += 1 << order
		}
	}
	return total
}

func TestReleasingEverythingMergesTheSlabBackIntoOneTopBlock(t *testing.T) {
	arena, err := NewArena(invariantBudget)
	if err != nil {
		t.Fatal(err)
	}
	sketches := make([]Sketch, invariantBudget/MinEntriesPerSketch)
	for i := range sketches {
		arena.Add(&sketches[i], int64(i+1))
	}
	for i := range sketches {
		arena.Release(&sketches[i])
	}

	if head := arena.blocks.heads[arena.blocks.topOrder]; head != firstFree {
		t.Fatalf("top-order free head = %d, want %d: released blocks did not merge up to the top order", head, firstFree)
	}
	if free := freeEntries(&arena.blocks); free != invariantBudget {
		t.Errorf("free entries = %d, want %d", free, invariantBudget)
	}
}

func seedOps(seed uint64, count int) []byte {
	random := rand.New(rand.NewPCG(seed, seed))
	ops := make([]byte, 0, count*opBytes)
	for range count {
		op := make([]byte, opBytes)
		op[0] = byte(random.IntN(math.MaxUint8 + 1))
		binary.LittleEndian.PutUint64(op[1:], random.Uint64()>>uint(random.IntN(wordBits)))
		ops = append(ops, op...)
	}
	return ops
}
