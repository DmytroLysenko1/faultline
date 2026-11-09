//go:build faultline_debug

package sketch_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

func TestUsingAStaleCopyOfASketchPanics(t *testing.T) {
	tests := []struct {
		name  string
		stale func(arena *sketch.Arena) *sketch.Sketch
		use   func(arena *sketch.Arena, s *sketch.Sketch)
	}{
		{name: "add through a copy kept after release", stale: copyAfterRelease, use: addThrough},
		{name: "release through a copy kept after release", stale: copyAfterRelease, use: releaseThrough},
		{name: "percentiles through a copy kept after release", stale: copyAfterRelease, use: percentilesThrough},
		{name: "add through a copy of a live sketch that changed since", stale: copyOfChangedLive, use: addThrough},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arena := newArena(t, sketch.MaxEntriesPerSketch)
			stale := tt.stale(arena)
			defer func() {
				if recover() == nil {
					t.Fatal("stale copy was used without a panic; it would corrupt another sketch's block")
				}
			}()
			tt.use(arena, stale)
		})
	}
}

func copyAfterRelease(arena *sketch.Arena) *sketch.Sketch {
	live := record(arena, []int64{1_000})
	stale := *live
	arena.Release(live)
	record(arena, []int64{2_000})
	return &stale
}

func copyOfChangedLive(arena *sketch.Arena) *sketch.Sketch {
	live := record(arena, []int64{1_000})
	stale := *live
	arena.Add(live, 4_000_000)
	return &stale
}

func addThrough(arena *sketch.Arena, s *sketch.Sketch) {
	arena.Add(s, 3_000)
}

func releaseThrough(arena *sketch.Arena, s *sketch.Sketch) {
	arena.Release(s)
}

func percentilesThrough(arena *sketch.Arena, s *sketch.Sketch) {
	_, _ = arena.Percentiles(s)
}
