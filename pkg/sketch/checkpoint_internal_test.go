package sketch

import (
	"bytes"
	"errors"
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const regressionBudget = 1 << 12

func checkpointOne(t *testing.T, damage func(a *Arena, s *Sketch)) []byte {
	t.Helper()
	arena, err := NewArena(regressionBudget)
	if err != nil {
		t.Fatal(err)
	}
	var s Sketch
	for nanos := range int64(40) {
		arena.Add(&s, nanos*nanos*1_000+1)
	}
	damage(arena, &s)
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(1)
	arena.WriteCheckpoint(w)
	arena.WriteSketch(w, &s)
	w.End()
	return out.Bytes()
}

func restoreOne(data []byte) error {
	r := checkpoint.NewReader(bytes.NewReader(data))
	r.Begin(1)
	restore, err := RestoreArena(regressionBudget, r)
	if err != nil {
		return err
	}
	var s Sketch
	restore.ReadSketch(r, &s)
	restore.Finish(r)
	r.End()
	return r.Finish()
}

// TestASketchCheckpointCannotPointPastTheArena: every value a sketch carries is checked against
// what the arena can hold (review: a bin past the mapping table passed restore and panicked in
// Percentiles on the next emit).
func TestASketchCheckpointCannotPointPastTheArena(t *testing.T) {
	tests := []struct {
		name   string
		damage func(a *Arena, s *Sketch)
	}{
		{name: "a bin past the mapping table", damage: func(a *Arena, s *Sketch) {
			entries := a.entries(s)
			entries[len(entries)-1] = entryKey(uint16(len(a.mapping.values))) | 1
		}},
		{name: "bins out of order", damage: func(a *Arena, s *Sketch) {
			entries := a.entries(s)
			entries[0], entries[1] = entries[1], entries[0]
		}},
		{name: "a count that does not add up", damage: func(_ *Arena, s *Sketch) {
			s.Count++
		}},
		{name: "a block that overlaps a free one", damage: func(a *Arena, s *Sketch) {
			s.offset = a.blocks.heads[s.order]
		}},
	}
	if err := restoreOne(checkpointOne(t, func(*Arena, *Sketch) {})); err != nil {
		t.Fatalf("an undamaged arena: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := restoreOne(checkpointOne(t, tt.damage))

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}
