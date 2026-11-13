package stream

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

type countSink struct{}

func (countSink) Emit(int, Pane, *uint64)     {}
func (countSink) Retire(int, Window, *uint64) {}
func (countSink) Closed(int)                  {}

type countCodec struct{}

func (countCodec) WriteKey(w *checkpoint.Writer, key int)    { w.Count(key) }
func (countCodec) ReadKey(r *checkpoint.Reader) int          { return r.Index(internalCapacity) }
func (countCodec) WriteCell(w *checkpoint.Writer, c *uint64) { w.Uint(*c) }
func (countCodec) ReadCell(r *checkpoint.Reader, c *uint64)  { *c = r.Uint() }

const internalCapacity = 4

var internalConfig = Config{Size: 10 * time.Second, Delay: 30 * time.Second, AllowedLateness: 30 * time.Second,
	SlackWindows: 2}

func liveWindows(t *testing.T) *Windows[int, uint64] {
	t.Helper()
	w, err := New[int, uint64](internalConfig, Options{Capacity: internalCapacity}, countSink{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	w.Begin(Mark{Time: now, Known: true}, now)
	for key := range internalCapacity {
		cell, _, _ := w.Locate(key, now.Add(-time.Duration(key)*internalConfig.Size))
		*cell++
	}
	w.End()
	return w
}

func restoreFrom(t *testing.T, w *Windows[int, uint64]) error {
	t.Helper()
	var out bytes.Buffer
	writer := checkpoint.NewWriter(&out, 1)
	writer.Begin(1)
	w.WriteCheckpoint(writer, countCodec{})
	writer.End()
	reader := checkpoint.NewReader(&out)
	reader.Begin(1)
	if _, err := Restore[int, uint64](internalConfig, Options{Capacity: internalCapacity}, countSink{}, reader,
		countCodec{}); err != nil {
		return err
	}
	reader.End()
	return reader.Finish()
}

func firstLiveBucket(t *testing.T, w *Windows[int, uint64]) *bucket {
	t.Helper()
	for position := range w.buckets {
		if w.buckets[position].live {
			return &w.buckets[position]
		}
	}
	t.Fatal("no live window")
	return nil
}

// TestAWindowOffTheHorizonIsRefused: a window that is in its slot but a whole ring away from the
// horizon (review: Locate then panics with a ring collision), a wrong final mark or live windows
// without a known mark are all refused at restore, never left for Locate to find.
func TestAWindowOffTheHorizonIsRefused(t *testing.T) {
	tests := []struct {
		name   string
		damage func(w *Windows[int, uint64])
	}{
		{name: "a window one ring ahead", damage: func(w *Windows[int, uint64]) {
			firstLiveBucket(t, w).windowID += w.slotsPer
		}},
		{name: "a window one ring behind", damage: func(w *Windows[int, uint64]) {
			firstLiveBucket(t, w).windowID -= w.slotsPer
		}},
		{name: "a closed window not marked final", damage: func(w *Windows[int, uint64]) {
			b := firstLiveBucket(t, w)
			b.final = !b.final
		}},
		{name: "live windows without a known mark", damage: func(w *Windows[int, uint64]) {
			w.batch.known = false
		}},
	}
	if err := restoreFrom(t, liveWindows(t)); err != nil {
		t.Fatalf("an undamaged engine: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := liveWindows(t)
			tt.damage(w)

			err := restoreFrom(t, w)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}
