package red_test

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	windowSize       = 10 * time.Second
	arenaEntries     = 2 * sketch.MaxEntriesPerSketch
	allocRuns        = 100
	wiringPrefix     = "red: "
	subjectSlotLimit = 48
	opSlotLimit      = 32
	ratioTolerance   = 1e-9
	sketchAlpha      = 0.02
	binStep          = 1.02
)

func newArena(t testing.TB) *sketch.Arena {
	t.Helper()
	arena, err := sketch.NewArena(arenaEntries)
	if err != nil {
		t.Fatalf("NewArena: %v", err)
	}
	return arena
}

func paneAt(kind stream.PaneKind, revision uint32) stream.Pane {
	start := time.Unix(1_700_000_000, 0)
	return stream.Pane{
		Window:   stream.Window{ID: 170_000_000, Start: start, End: start.Add(windowSize)},
		Kind:     kind,
		Revision: revision,
	}
}

func TestCellRecordsEveryRequestAndOnlyKnownLatencies(t *testing.T) {
	arena := newArena(t)
	var cell red.Cell
	cell.Record(arena, false, 5*time.Millisecond, true)
	cell.Record(arena, true, 0, false)
	cell.Record(arena, true, 7*time.Millisecond, true)

	type observed struct {
		Counts  red.Counts
		Samples uint64
	}
	want := observed{Counts: red.Counts{Requests: 3, Failed: 2}, Samples: 2}
	if diff := cmp.Diff(want, observed{cell.Counts, cell.Latency.Count}); diff != "" {
		t.Errorf("cell (-want +got):\n%s", diff)
	}
}

func TestSummarizeCarriesThePaneTheCountsAndThePercentiles(t *testing.T) {
	arena := newArena(t)
	var cell red.Cell
	for millis := 1; millis <= 100; millis++ {
		cell.Record(arena, millis%10 == 0, time.Duration(millis)*time.Millisecond, true)
	}
	cell.Record(arena, false, 0, false)
	pane := paneAt(stream.PaneOnTime, 1)

	got := red.Summarize(arena, pane, &cell, windowSize)

	percentiles := [3]time.Duration{got.P50, got.P95, got.P99}
	got.P50, got.P95, got.P99 = 0, 0, 0
	want := red.WindowStats{
		StartUnixNano: pane.Window.Start.UnixNano(), Kind: red.PaneOnTime, Revision: 1,
		Requests: 101, Failed: 10, LatencySamples: 100, Observed: windowSize,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("summary (-want +got):\n%s", diff)
	}
	wantPercentiles := [3]time.Duration{50 * time.Millisecond, 95 * time.Millisecond, 99 * time.Millisecond}
	if diff := cmp.Diff(wantPercentiles, percentiles, cmp.Comparer(withinRelativeError)); diff != "" {
		t.Errorf("[p50, p95, p99] within α (-want +got):\n%s", diff)
	}
}

func TestSummariesWithoutLatencyHaveNoPercentiles(t *testing.T) {
	arena := newArena(t)
	counts := red.Counts{Requests: 4, Failed: 1}
	cell := red.Cell{Counts: counts}
	pane := paneAt(stream.PaneEarly, 0)
	tests := []struct {
		Name string
		Args red.WindowStats
	}{
		{Name: "counts of an operation", Args: red.SummarizeCounts(pane, &counts, time.Second)},
		{Name: "cell with unknown latencies", Args: red.Summarize(arena, pane, &cell, time.Second)},
	}
	want := red.WindowStats{
		StartUnixNano: pane.Window.Start.UnixNano(), Kind: red.PaneEarly, Requests: 4, Failed: 1, Observed: time.Second,
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(want, tt.Args); diff != "" {
				t.Errorf("summary (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEveryStreamPaneKindHasADomainKind(t *testing.T) {
	counts := red.Counts{Requests: 1}
	want := map[stream.PaneKind]red.PaneKind{
		stream.PaneEarly:  red.PaneEarly,
		stream.PaneOnTime: red.PaneOnTime,
		stream.PaneLate:   red.PaneLate,
	}
	got := make(map[stream.PaneKind]red.PaneKind, len(want))
	for kind := range want {
		got[kind] = red.SummarizeCounts(paneAt(kind, 1), &counts, windowSize).Kind
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("pane kinds (-want +got):\n%s", diff)
	}
}

func TestRateAndErrorRatio(t *testing.T) {
	tests := []struct {
		Name string
		Args red.WindowStats
		Want [2]float64
	}{
		{Name: "ten requests over two seconds, one failed",
			Args: red.SummarizeCounts(paneAt(stream.PaneEarly, 0), &red.Counts{Requests: 10, Failed: 1}, 2*time.Second),
			Want: [2]float64{5, 0.1}},
		{Name: "no requests", Args: red.SummarizeCounts(paneAt(stream.PaneOnTime, 1), &red.Counts{}, windowSize)},
		{Name: "zero value"},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := [2]float64{tt.Args.Rate(), tt.Args.ErrorRatio()}
			if diff := cmp.Diff(tt.Want, got, cmpopts.EquateApprox(0, ratioTolerance)); diff != "" {
				t.Errorf("[Rate, ErrorRatio] (-want +got):\n%s", diff)
			}
		})
	}
}

func TestIsZeroOnlyForTheUnsetSummary(t *testing.T) {
	built := red.SummarizeCounts(paneAt(stream.PaneLate, 2), &red.Counts{}, windowSize)
	got := [2]bool{red.WindowStats{}.IsZero(), built.IsZero()}
	if diff := cmp.Diff([2]bool{true, false}, got); diff != "" {
		t.Errorf("[zero value, built] IsZero (-want +got):\n%s", diff)
	}
}

func TestEqualComparesEveryFieldAndCmpUsesIt(t *testing.T) {
	pane := paneAt(stream.PaneOnTime, 1)
	base := red.SummarizeCounts(pane, &red.Counts{Requests: 3}, windowSize)
	tests := []struct {
		Name string
		Args red.WindowStats
		Want bool
	}{
		{Name: "same", Args: red.SummarizeCounts(pane, &red.Counts{Requests: 3}, windowSize), Want: true},
		{Name: "another revision", Args: red.SummarizeCounts(paneAt(stream.PaneLate, 2), &red.Counts{Requests: 3}, windowSize)},
		{Name: "another count", Args: red.SummarizeCounts(pane, &red.Counts{Requests: 4}, windowSize)},
		{Name: "another coverage", Args: red.SummarizeCounts(pane, &red.Counts{Requests: 3}, time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := [2]bool{base.Equal(tt.Args), cmp.Diff(base, tt.Args) == ""}
			if diff := cmp.Diff([2]bool{tt.Want, tt.Want}, got); diff != "" {
				t.Errorf("[Equal, cmp.Diff empty] (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSummariesOutsideTheWindowAreWiringPanics(t *testing.T) {
	tests := []struct {
		Name string
		Args func()
	}{
		{Name: "zero coverage", Args: func() { red.SummarizeCounts(paneAt(stream.PaneEarly, 0), &red.Counts{}, 0) }},
		{Name: "coverage past the window", Args: func() {
			red.SummarizeCounts(paneAt(stream.PaneEarly, 0), &red.Counts{}, windowSize+time.Nanosecond)
		}},
		{Name: "unknown pane kind", Args: func() { red.SummarizeCounts(paneAt(0, 0), &red.Counts{}, windowSize) }},
		{Name: "more failures than requests", Args: func() {
			red.SummarizeCounts(paneAt(stream.PaneEarly, 0), &red.Counts{Requests: 1, Failed: 2}, windowSize)
		}},
		{Name: "latency without a request", Args: func() {
			arena := newArena(t)
			var cell red.Cell
			arena.Add(&cell.Latency, int64(time.Millisecond))
			red.Summarize(arena, paneAt(stream.PaneEarly, 0), &cell, windowSize)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			defer func() {
				message, wiring := recover().(string)
				if !wiring || !strings.HasPrefix(message, wiringPrefix) {
					t.Errorf("panic = %q, want a wiring panic starting with %q", message, wiringPrefix)
				}
			}()
			tt.Args()
		})
	}
}

func TestCellsFitTheirSlotBudgetsAndHoldNoPointers(t *testing.T) {
	type subjectSlot struct {
		Cell  red.Cell
		Flags uint32
	}
	type operationSlot struct {
		Cell  red.Counts
		Flags uint32
	}
	type layout struct {
		SubjectSlot, OperationSlot, CellPointerFree, CountsPointerFree, StatsPointerFree bool
	}
	want := layout{true, true, true, true, true}
	got := layout{
		SubjectSlot:       unsafe.Sizeof(subjectSlot{}) <= subjectSlotLimit,
		OperationSlot:     unsafe.Sizeof(operationSlot{}) <= opSlotLimit,
		CellPointerFree:   pointerFree(reflect.TypeFor[red.Cell]()),
		CountsPointerFree: pointerFree(reflect.TypeFor[red.Counts]()),
		StatsPointerFree:  pointerFree(reflect.TypeFor[red.WindowStats]()),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("layout (-want +got):\n%s", diff)
	}
}

func pointerFree(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Struct:
		for field := range typ.Fields() {
			if !pointerFree(field.Type) {
				return false
			}
		}
		return true
	case reflect.Array:
		return pointerFree(typ.Elem())
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.String, reflect.Interface, reflect.Chan, reflect.Func,
		reflect.UnsafePointer:
		return false
	}
	return true
}

func TestRecordAndSummarizeDoNotAllocate(t *testing.T) {
	arena := newArena(t)
	var cell red.Cell
	cell.Record(arena, false, time.Millisecond, true)
	pane := paneAt(stream.PaneOnTime, 1)
	tests := []struct {
		Name string
		Args func()
	}{
		{Name: "Record", Args: func() { cell.Record(arena, false, time.Millisecond, true) }},
		{Name: "Summarize", Args: func() { red.Summarize(arena, pane, &cell, windowSize) }},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if allocations := testing.AllocsPerRun(allocRuns, tt.Args); allocations != 0 {
				t.Errorf("allocates %.0f times, want 0", allocations)
			}
		})
	}
}

func BenchmarkSummarize(b *testing.B) {
	arena := newArena(b)
	var cell red.Cell
	for millis := 1; millis <= 15; millis++ {
		cell.Record(arena, false, time.Duration(millis)*time.Millisecond, true)
	}
	pane := paneAt(stream.PaneOnTime, 1)
	b.ReportAllocs()
	for b.Loop() {
		red.Summarize(arena, pane, &cell, windowSize)
	}
}

// withinRelativeError accepts a sketch estimate within the DDSketch bound α·v + ½ ns.
func withinRelativeError(want, got time.Duration) bool {
	bound := time.Duration(sketchAlpha*float64(want)) + time.Nanosecond
	return got >= want-bound && got <= want+bound
}

func TestReleaseReturnsTheSketchBlockToTheArena(t *testing.T) {
	arena := newArena(t)
	var cell red.Cell
	cell.Record(arena, false, time.Millisecond, true)
	held := arena.Stats.InUse

	cell.Release(arena)

	if diff := cmp.Diff([2]int{sketch.MinEntriesPerSketch, 0}, [2]int{held, arena.Stats.InUse}); diff != "" {
		t.Errorf("[in use while held, after Release] (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(red.Counts{}, cell.Counts); diff != "" {
		t.Errorf("counts after Release (-want +got):\n%s", diff)
	}
}

func TestAnExhaustedSketchBudgetShowsFewerSamplesThanRequests(t *testing.T) {
	arena := newArena(t)
	wide := make([]sketch.Sketch, 0, arenaEntries)
	for arena.Stats.InUse < arena.Stats.Budget {
		wide = append(wide, sketch.Sketch{})
		for nanos := 1.0; nanos < math.MaxInt64/2; nanos *= binStep {
			arena.Add(&wide[len(wide)-1], int64(nanos))
		}
	}
	var cell red.Cell
	cell.Record(arena, false, time.Millisecond, true)
	cell.Record(arena, false, 0, true)

	stats := red.Summarize(arena, paneAt(stream.PaneOnTime, 1), &cell, windowSize)

	type observed struct {
		Requests, LatencySamples uint64
		HasLatency               bool
	}
	want := observed{Requests: 2, LatencySamples: 1, HasLatency: true}
	if diff := cmp.Diff(want, observed{stats.Requests, stats.LatencySamples, stats.HasLatency()}); diff != "" {
		t.Errorf("summary without arena room (-want +got):\n%s", diff)
	}
}
