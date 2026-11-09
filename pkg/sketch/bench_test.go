package sketch_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

func BenchmarkSketchAddExistingBin(b *testing.B) {
	arena := newArena(b, defaultBudget)
	values := lognormalSample(5, 4096, 1)
	s := record(arena, values)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		arena.Add(s, values[i%len(values)])
		i++
	}
}

func BenchmarkSketchCellLifecycle(b *testing.B) {
	arena := newArena(b, defaultBudget)
	values := lognormalSample(6, 10, 1)
	b.ReportAllocs()
	for b.Loop() {
		s := sketch.Sketch{}
		for _, value := range values {
			arena.Add(&s, value)
		}
		arena.Release(&s)
	}
}

func BenchmarkSketchPercentiles(b *testing.B) {
	arena := newArena(b, defaultBudget)
	s := record(arena, lognormalSample(7, 10_000, 1))
	b.ReportAllocs()
	for b.Loop() {
		_, _ = arena.Percentiles(s)
	}
}

func BenchmarkSketchAddTypicalCell(b *testing.B) {
	arena := newArena(b, defaultBudget)
	values := distinctBinValues(400, 16)
	s := record(arena, values)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		arena.Add(s, values[i%len(values)])
		i++
	}
}
