package sketch

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	worstBudget      = 1 << 24
	worstBlockOrder  = 10
	worstBufferBytes = 1 << 20
)

// fullArena fills a production-size arena with sketches that each use every bin they can hold:
// the arena is the bulk of a worst-case checkpoint (C14: the pipeline pause must stay under 1 s).
func fullArena(b *testing.B) (*Arena, []Sketch) {
	b.Helper()
	arena, err := NewArena(worstBudget)
	if err != nil {
		b.Fatal(err)
	}
	bins := min(len(arena.mapping.values)-1, 1<<worstBlockOrder)
	sketches := make([]Sketch, worstBudget>>worstBlockOrder)
	for i := range sketches {
		for bin := 1; bin <= bins; bin++ {
			arena.Add(&sketches[i], arena.mapping.values[bin])
		}
	}
	return arena, sketches
}

// BenchmarkWriteCheckpointWorstCase writes a full 2^24-entry arena through bufio to a file, as
// the processor writes its spool; MB/s and bytes give the pause for the budgeted 450 MiB state.
func BenchmarkWriteCheckpointWorstCase(b *testing.B) {
	arena, sketches := fullArena(b)
	path := filepath.Join(b.TempDir(), "spool")
	var written int64
	for b.Loop() {
		written = writeSpool(b, path, arena, sketches)
	}
	b.SetBytes(int64(worstBudget) * wordBytes)
	b.ReportMetric(float64(written), "checkpoint-bytes")
}

func writeSpool(b *testing.B, path string, arena *Arena, sketches []Sketch) int64 {
	b.Helper()
	file, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	buffered := bufio.NewWriterSize(file, worstBufferBytes)
	w := checkpoint.NewWriter(buffered, 1)
	w.Begin(1)
	arena.WriteCheckpoint(w)
	for i := range sketches {
		arena.WriteSketch(w, &sketches[i])
	}
	w.End()
	if err := w.Err(); err != nil {
		b.Fatal(err)
	}
	if err := buffered.Flush(); err != nil {
		b.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	return info.Size()
}
