package sketch_test

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

func TestPercentilesOfLognormalLatencyStayWithinTwoPercent(t *testing.T) {
	tests := []struct {
		name  string
		sigma float64
	}{
		{name: "narrow tail sigma 0.5", sigma: 0.5},
		{name: "typical tail sigma 1", sigma: 1},
		{name: "heavy tail sigma 2 that broke a 128-bin sketch", sigma: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arena := newArena(t, defaultBudget)
			values := lognormalSample(1, 100_000, tt.sigma)
			got, ok := arena.Percentiles(record(arena, values))
			if !ok {
				t.Fatal("Percentiles reported no values after 100 000 adds")
			}
			assertPercentilesAccurate(t, got, exactPercentiles(values))
			if collapsed := arena.Stats.CollapsedAdds; collapsed != 0 {
				t.Errorf("CollapsedAdds = %d, want 0 with an ample budget", collapsed)
			}
		})
	}
}

func TestPercentilesOfSingleBoundaryValue(t *testing.T) {
	tests := []struct {
		name  string
		value int64
	}{
		{name: "zero latency goes to the zero bin", value: 0},
		{name: "negative latency is treated as zero", value: -5},
		{name: "one nanosecond", value: 1},
		{name: "just below a bin boundary", value: int64(math.Pow(gamma, 300)) - 1},
		{name: "exactly on a bin boundary", value: int64(math.Pow(gamma, 300))},
		{name: "ten-minute stream", value: int64(10 * time.Minute)},
		{name: "largest duration", value: math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arena := newArena(t, minimalBudget)
			got, _ := arena.Percentiles(record(arena, []int64{tt.value}))
			want := max(tt.value, 0)
			if want == 0 {
				if diff := cmp.Diff(sketch.Percentiles{}, got); diff != "" {
					t.Fatalf("non-positive latency must report exactly 0 (-want +got):\n%s", diff)
				}
				return
			}
			assertPercentilesAccurate(t, got, sketch.Percentiles{P50: want, P95: want, P99: want})
		})
	}
}

func TestEmptySketchReportsNoPercentiles(t *testing.T) {
	arena := newArena(t, minimalBudget)
	if got, ok := arena.Percentiles(new(sketch.Sketch)); ok {
		t.Fatalf("Percentiles of an empty sketch = %+v, true; want false", got)
	}
}

func TestNormalLoadFitsTheBudgetWithoutCollapsing(t *testing.T) {
	tests := []struct {
		name          string
		budget        int
		eventsPerCell int
	}{
		{name: "default budget, 10 events per cell", budget: defaultBudget, eventsPerCell: 10},
		{name: "first memory concession, 5 events per cell as in NFR-PERF-002", budget: concedeBudget, eventsPerCell: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arena := newArena(t, tt.budget)
			values := lognormalSample(2, tt.eventsPerCell*subjectCells, 1)
			for cell := range subjectCells {
				record(arena, values[cell*tt.eventsPerCell:(cell+1)*tt.eventsPerCell])
			}
			stats := arena.Stats
			if stats.CollapsedAdds != 0 || stats.DroppedSamples != 0 {
				t.Fatalf("%d cells of %d events: collapsed %d, dropped %d; want 0 and 0 (in use %d of %d)", subjectCells,
					tt.eventsPerCell, stats.CollapsedAdds, stats.DroppedSamples, stats.InUse, stats.Budget)
			}
		})
	}
}

func TestReleasedWideSketchesLeaveRoomForManyNarrowOnes(t *testing.T) {
	arena := newArena(t, smallBudget)
	wide := fillWithWideSketches(arena)
	for _, s := range wide {
		arena.Release(s)
	}
	afterBurst := arena.Stats
	if afterBurst.InUse != 0 {
		t.Fatalf("InUse after releasing every wide sketch = %d, want 0", afterBurst.InUse)
	}

	narrow := distinctBinValues(400, 16)
	for range 200_000 {
		record(arena, narrow)
	}

	stats := arena.Stats
	dropped := stats.DroppedSamples - afterBurst.DroppedSamples
	collapsed := stats.CollapsedAdds - afterBurst.CollapsedAdds
	if dropped != 0 || collapsed != 0 {
		t.Fatalf("narrow sketches after a burst of wide ones: dropped %d, collapsed %d; want 0 and 0", dropped, collapsed)
	}
}

func fillWithWideSketches(arena *sketch.Arena) []*sketch.Sketch {
	wideValues := distinctBinValues(100, sketch.MaxEntriesPerSketch/2+1)
	var wide []*sketch.Sketch
	for range maxFillers(arena) {
		wide = append(wide, record(arena, wideValues))
		if arena.Stats.CollapsedAdds != 0 || arena.Stats.DroppedSamples != 0 {
			return wide
		}
	}
	panic("arena never filled with wide sketches: collapse and drop are not reported")
}

func TestFullArenaMergesLowestEntriesAndKeepsHighPercentiles(t *testing.T) {
	arena := newArena(t, minimalBudget)
	values := distinctBinValues(500, 40)
	target := record(arena, values[:16])
	fillers := fillArena(arena)

	for _, value := range values[16:] {
		arena.Add(target, value)
	}

	got, _ := arena.Percentiles(target)
	if !withinAccuracy(got.P99, exactPercentiles(values).P99) {
		t.Errorf("p99 after collapsing = %d, exact %d", got.P99, exactPercentiles(values).P99)
	}
	if target.Count != uint64(len(values)) {
		t.Errorf("Count = %d, want %d: collapsing must keep every sample", target.Count, len(values))
	}
	stats := arena.Stats
	if stats.CollapsedSketches != 1 || stats.CollapsedAdds != uint64(len(values)-16) {
		t.Errorf("collapsed sketches %d, adds %d; want 1 and %d", stats.CollapsedSketches, stats.CollapsedAdds,
			len(values)-16)
	}
	releaseAll(arena, fillers)
}

func fillArena(arena *sketch.Arena) []*sketch.Sketch {
	var fillers []*sketch.Sketch
	for range maxFillers(arena) {
		fillers = append(fillers, record(arena, []int64{1}))
		if arena.Stats.DroppedSamples != 0 {
			return fillers
		}
	}
	panic("arena never filled: a sketch without a block does not report dropped samples")
}

func maxFillers(arena *sketch.Arena) int {
	return arena.Stats.Budget/sketch.MinEntriesPerSketch + 1
}

func releaseAll(arena *sketch.Arena, sketches []*sketch.Sketch) {
	for _, s := range sketches {
		arena.Release(s)
	}
}

func TestSketchWithoutSpaceDropsSamplesUntilSpaceReturns(t *testing.T) {
	arena := newArena(t, minimalBudget)
	fillers := fillArena(arena)
	starved := new(sketch.Sketch)

	arena.Add(starved, valueInBin(300))
	if starved.Count != 0 {
		t.Fatalf("Count of a sketch that got no block = %d, want 0", starved.Count)
	}
	droppedWhileFull := arena.Stats.DroppedSamples

	releaseAll(arena, fillers)
	arena.Add(starved, valueInBin(300))

	if starved.Count != 1 {
		t.Errorf("Count after space returned = %d, want 1", starved.Count)
	}
	if arena.Stats.DroppedSamples != droppedWhileFull {
		t.Errorf("DroppedSamples grew after space returned: %d → %d", droppedWhileFull, arena.Stats.DroppedSamples)
	}
}

func TestReleaseReturnsEveryEntryAndEmptiesTheSketch(t *testing.T) {
	arena := newArena(t, minimalBudget)
	s := record(arena, distinctBinValues(200, 100))

	arena.Release(s)

	if s.Count != 0 {
		t.Errorf("Count after Release = %d, want 0", s.Count)
	}
	want := sketch.ArenaStats{Budget: minimalBudget}
	if diff := cmp.Diff(want, arena.Stats); diff != "" {
		t.Errorf("stats after Release mismatch (-want +got):\n%s", diff)
	}
}

func TestEqualDependsOnValuesNotOnInsertionOrder(t *testing.T) {
	arena := newArena(t, defaultBudget)
	values := lognormalSample(3, 1_000, 1)
	forward := record(arena, values)
	backward := record(arena, reversed(values))
	differs := record(arena, append(slices.Clone(values), 1))

	if !arena.Equal(forward, backward) {
		t.Error("Equal(forward, backward) = false, want true: a sketch must not depend on insertion order")
	}
	if arena.Equal(forward, differs) {
		t.Error("Equal(forward, one more value) = true, want false")
	}
}

func TestNewArenaRejectsInvalidBudgets(t *testing.T) {
	tests := []struct {
		name    string
		budget  int
		wantErr error
	}{
		{name: "zero", budget: 0, wantErr: sketch.ErrInvalidBudget},
		{name: "below one full sketch", budget: sketch.MaxEntriesPerSketch / 2, wantErr: sketch.ErrInvalidBudget},
		{name: "not a power of two", budget: 3 << 20, wantErr: sketch.ErrInvalidBudget},
		{name: "above 256 Mi", budget: 512 << 20, wantErr: sketch.ErrInvalidBudget},
		{name: "exactly one full sketch", budget: sketch.MaxEntriesPerSketch, wantErr: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sketch.NewArena(tt.budget)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewArena(%d) error = %v, want %v", tt.budget, err, tt.wantErr)
			}
		})
	}
}

func TestHotPathDoesNotAllocate(t *testing.T) {
	arena := newArena(t, defaultBudget)
	values := lognormalSample(4, 64, 1)
	warm := record(arena, values)
	tests := []struct {
		name string
		run  func()
	}{
		{name: "add to an existing bin", run: func() { arena.Add(warm, values[0]) }},
		{name: "percentiles", run: func() { _, _ = arena.Percentiles(warm) }},
		{name: "cell lifecycle", run: func() { arena.Release(record(arena, values[:10])) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if allocs := testing.AllocsPerRun(100, tt.run); allocs != 0 {
				t.Errorf("allocations per run = %v, want 0", allocs)
			}
		})
	}
}

func TestSketchIsPointerFreeAndFitsTwentyFourBytes(t *testing.T) {
	if size := unsafe.Sizeof(sketch.Sketch{}); size != 24 {
		t.Errorf("unsafe.Sizeof(Sketch) = %d, want 24", size)
	}
	if kind, ok := pointerField(reflect.TypeFor[sketch.Sketch]()); ok {
		t.Errorf("Sketch holds a %s field; window slots embedding it would be scanned by the GC", kind)
	}
}

func pointerField(typ reflect.Type) (reflect.Kind, bool) {
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.String, reflect.Interface, reflect.Chan, reflect.Func,
		reflect.UnsafePointer:
		return typ.Kind(), true
	case reflect.Array:
		return pointerField(typ.Elem())
	case reflect.Struct:
		for field := range typ.Fields() {
			if kind, ok := pointerField(field.Type); ok {
				return kind, true
			}
		}
	}
	return reflect.Invalid, false
}

func TestReleasedNarrowSketchesMergeBackIntoFullWidthBlocks(t *testing.T) {
	for _, budget := range []int{minimalBudget, 64 << 10} {
		t.Run(fmt.Sprintf("budget %d", budget), func(t *testing.T) {
			assertNarrowBurstMergesBack(t, budget)
		})
	}
}

func assertNarrowBurstMergesBack(t *testing.T, budget int) {
	t.Helper()
	arena := newArena(t, budget)
	releaseAll(arena, fillArena(arena))
	afterRelease := arena.Stats

	wideValues := distinctBinValues(0, allBins)
	for range budget / sketch.MaxEntriesPerSketch {
		record(arena, wideValues)
	}

	stats := arena.Stats
	if collapsed := stats.CollapsedAdds - afterRelease.CollapsedAdds; collapsed != 0 {
		t.Errorf("collapsed adds = %d, want 0: released neighbours were not merged back", collapsed)
	}
	if stats.InUse != budget {
		t.Errorf("InUse = %d, want the whole budget %d in full-width blocks", stats.InUse, budget)
	}
}

func TestSingleSketchFillsTheMinimalBudgetWithoutCollapsing(t *testing.T) {
	arena := newArena(t, minimalBudget)

	s := record(arena, distinctBinValues(0, allBins))

	stats := arena.Stats
	if stats.CollapsedAdds != 0 || stats.InUse != minimalBudget {
		t.Fatalf("one sketch over every bin: collapsed %d, in use %d; want 0 and %d (it must grow into its free buddy)",
			stats.CollapsedAdds, stats.InUse, minimalBudget)
	}
	if s.Count != uint64(allBins) {
		t.Errorf("Count = %d, want %d", s.Count, allBins)
	}
}

func TestSketchGrowsThroughPowerOfTwoClasses(t *testing.T) {
	arena := newArena(t, minimalBudget)
	values := distinctBinValues(300, 9)
	s := new(sketch.Sketch)
	want := []int{2, 2, 4, 4, 8, 8, 8, 8, 16}
	got := make([]int, 0, len(values))

	for _, value := range values {
		arena.Add(s, value)
		got = append(got, arena.Stats.InUse)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("capacity after each new bin mismatch (-want +got):\n%s", diff)
	}
}

func TestValueBelowEveryKeptBinFoldsIntoTheLowestEntry(t *testing.T) {
	arena := newArena(t, minimalBudget)
	low, high := valueInBin(100), valueInBin(200)
	target := record(arena, []int64{low, high})
	fillers := fillArena(arena)

	arena.Add(target, valueInBin(50))

	got, _ := arena.Percentiles(target)
	want := sketch.Percentiles{P50: low, P95: low, P99: low}
	if diff := cmp.Diff(want, got, approxNanos()); diff != "" {
		t.Errorf("percentiles after folding a lower value mismatch (-want +got):\n%s", diff)
	}
	if target.Count != 3 {
		t.Errorf("Count = %d, want 3", target.Count)
	}
	releaseAll(arena, fillers)
}

func TestStarvedSketchCountsZerosButDropsPositiveSamples(t *testing.T) {
	arena := newArena(t, minimalBudget)
	fillers := fillArena(arena)
	starved := new(sketch.Sketch)

	arena.Add(starved, 0)
	arena.Add(starved, valueInBin(300))

	got, ok := arena.Percentiles(starved)
	if !ok || starved.Count != 1 {
		t.Fatalf("Count = %d, ok = %v; want 1 and true: zeros need no entries", starved.Count, ok)
	}
	if diff := cmp.Diff(sketch.Percentiles{}, got); diff != "" {
		t.Errorf("percentiles of a starved sketch with one zero mismatch (-want +got):\n%s", diff)
	}
	releaseAll(arena, fillers)
}

func TestUpperHalfSketchGrowsIntoItsFreedLowerNeighbour(t *testing.T) {
	arena := newArena(t, minimalBudget)
	lower := record(arena, []int64{valueInBin(100)})
	upper := record(arena, []int64{valueInBin(300), valueInBin(301)})
	arena.Release(lower)

	arena.Add(upper, valueInBin(302))
	neighbour := record(arena, distinctBinValues(500, 4))

	if want := record(arena, distinctBinValues(300, 3)); !arena.Equal(upper, want) {
		t.Error("upper-half sketch lost or mixed entries after growing into its lower neighbour")
	}
	if want := record(arena, distinctBinValues(500, 4)); !arena.Equal(neighbour, want) {
		t.Error("a sketch allocated after the in-place growth overlaps the grown block")
	}
}

func TestZeroDurationSpansTakeTheLowRanksBeforePositiveLatencies(t *testing.T) {
	arena := newArena(t, minimalBudget)
	millisecond := int64(time.Millisecond)
	values := append(slices.Repeat([]int64{0}, 60), slices.Repeat([]int64{millisecond}, 40)...)

	got, _ := arena.Percentiles(record(arena, values))

	want := sketch.Percentiles{P50: 0, P95: millisecond, P99: millisecond}
	if diff := cmp.Diff(want, got, approxNanos()); diff != "" {
		t.Errorf("percentiles of 60 zero-duration and 40 one-millisecond spans mismatch (-want +got):\n%s", diff)
	}
}
