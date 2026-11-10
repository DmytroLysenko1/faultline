package sketch_test

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"
)

func FuzzSketch(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	f.Add(binary.LittleEndian.AppendUint64(nil, math.MaxUint64))
	f.Fuzz(func(t *testing.T, data []byte) {
		values := latenciesFrom(data)
		if len(values) == 0 {
			return
		}
		arena := newArena(t, fuzzBudget)
		forward := record(arena, values)

		if forward.Count != uint64(len(values)) {
			t.Fatalf("Count = %d, want %d", forward.Count, len(values))
		}
		got, _ := arena.Percentiles(forward)
		if got.P50 > got.P95 || got.P95 > got.P99 {
			t.Fatalf("percentiles not monotonic: %+v", got)
		}
		assertPercentilesAccurate(t, got, exactPercentiles(clampedToZero(values)))
		if !arena.Equal(forward, record(arena, reversed(values))) {
			t.Fatal("sketch depends on insertion order")
		}
	})
}

func latenciesFrom(data []byte) []int64 {
	values := make([]int64, 0, len(data)/8)
	for chunk := range slices.Chunk(data, 8) {
		if len(chunk) < 8 {
			break
		}
		values = append(values, int64(binary.LittleEndian.Uint64(chunk)))
	}
	return values
}

func clampedToZero(values []int64) []int64 {
	clamped := slices.Clone(values)
	for i, value := range clamped {
		clamped[i] = max(value, 0)
	}
	return clamped
}
