package red_test

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

const redArena = 1 << 12

func roundTrip(write func(w *checkpoint.Writer), read func(r *checkpoint.Reader)) error {
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(1)
	write(w)
	w.End()
	r := checkpoint.NewReader(&out)
	r.Begin(1)
	read(r)
	r.End()
	return r.Finish()
}

func restoredArena(r *checkpoint.Reader) *sketch.ArenaRestore {
	restore, err := sketch.RestoreArena(redArena, r)
	if err != nil {
		panic(err)
	}
	return restore
}

// TestACellComesBackWithItsCountsAndLatencies: a restored cell summarizes to the same RED as the
// original, percentiles included.
func TestACellComesBackWithItsCountsAndLatencies(t *testing.T) {
	arena, err := sketch.NewArena(redArena)
	if err != nil {
		t.Fatal(err)
	}
	var original red.Cell
	for i := range 50 {
		original.Record(arena, i%7 == 0, time.Duration(i+1)*time.Millisecond, true)
	}
	var restored red.Cell
	var restore *sketch.ArenaRestore

	err = roundTrip(func(w *checkpoint.Writer) {
		arena.WriteCheckpoint(w)
		original.WriteCheckpoint(w, arena)
	}, func(r *checkpoint.Reader) {
		restore = restoredArena(r)
		restored.ReadCheckpoint(r, restore)
		restore.Finish(r)
	})

	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	want, _ := arena.Percentiles(&original.Latency)
	got, _ := restore.Arena.Percentiles(&restored.Latency)
	if diff := cmp.Diff(want, got); diff != "" || original.Counts != restored.Counts {
		t.Errorf("cell differs: counts %+v vs %+v, percentiles (-original +restored):\n%s",
			original.Counts, restored.Counts, diff)
	}
}

func TestWindowStatsComeBackField(t *testing.T) {
	original := red.WindowStats{
		StartUnixNano: -1, Kind: red.PaneLate, Revision: math.MaxUint32, Requests: 9, Failed: 2, LatencySamples: 7,
		P50: 1, P95: 2, P99: 3, Observed: 10 * time.Second,
	}
	var restored red.WindowStats

	err := roundTrip(original.WriteCheckpoint, func(r *checkpoint.Reader) {
		restored = red.ReadWindowStats(r)
	})

	if err != nil || restored != original {
		t.Errorf("window stats %+v (err %v), want %+v", restored, err, original)
	}
}

func TestCountsThatCannotBeAreRefused(t *testing.T) {
	tests := []struct {
		name  string
		write func(w *checkpoint.Writer)
		read  func(r *checkpoint.Reader)
	}{
		{name: "more failed than requests", write: (&red.Counts{Requests: 1, Failed: 2}).WriteCheckpoint,
			read: func(r *checkpoint.Reader) { new(red.Counts).ReadCheckpoint(r) }},
		{name: "requests about to wrap", write: (&red.Counts{Requests: math.MaxUint64}).WriteCheckpoint,
			read: func(r *checkpoint.Reader) { new(red.Counts).ReadCheckpoint(r) }},
		{name: "more latency samples than requests", write: func(w *checkpoint.Writer) {
			arena, err := sketch.NewArena(redArena)
			if err != nil {
				panic(err)
			}
			var cell red.Cell
			arena.Add(&cell.Latency, int64(time.Millisecond))
			arena.WriteCheckpoint(w)
			cell.WriteCheckpoint(w, arena)
		}, read: func(r *checkpoint.Reader) {
			restore := restoredArena(r)
			new(red.Cell).ReadCheckpoint(r, restore)
			restore.Finish(r)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := roundTrip(tt.write, tt.read)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}
