package stream_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	restoreSeed      = 20261113
	restoreScenarios = 400
	windowsSection   = checkpoint.Tag(1)
)

// cellCodec keeps the request counts; latency sketches belong to the arena, which pkg/sketch
// checkpoints on its own, so a restored cell starts with an empty sketch.
type cellCodec struct {
	capacity int
}

func (cellCodec) WriteKey(w *checkpoint.Writer, key int) {
	w.Count(key)
}

func (c cellCodec) ReadKey(r *checkpoint.Reader) int {
	return r.Index(c.capacity)
}

func (cellCodec) WriteCell(w *checkpoint.Writer, c *cell) {
	w.Uint(c.requests)
}

func (cellCodec) ReadCell(r *checkpoint.Reader, c *cell) {
	c.requests = r.Uint()
}

func checkpointOf(t *testing.T, r *run) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(windowsSection)
	r.watermark.WriteCheckpoint(w)
	r.windows.WriteCheckpoint(w, cellCodec{})
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreRun(t *testing.T, data []byte, options stream.Options) (*run, error) {
	t.Helper()
	reader := checkpoint.NewReader(bytes.NewReader(data))
	reader.Begin(windowsSection)
	watermark, err := stream.RestoreWatermark(config, reader)
	if err != nil {
		return nil, err
	}
	recorder := newRecorder(t)
	windows, err := stream.Restore(config, options, recorder, reader, cellCodec{capacity: options.Capacity})
	if err != nil {
		return nil, err
	}
	reader.End()
	return &run{t: t, watermark: watermark, windows: windows, recorder: recorder}, reader.Finish()
}

type afterRestart struct {
	Panes  []finalPane
	Closed []int
	Stats  stream.Stats
}

func continueWith(r *run, batches []batch, last batch) afterRestart {
	from, closedFrom := len(r.recorder.log), len(r.recorder.closed)
	for _, b := range batches {
		r.apply(b)
	}
	r.apply(last)
	stats := r.windows.Stats()
	stats.Capacity = 0
	return afterRestart{
		Panes:  r.recorder.log[from:],
		Closed: r.recorder.closed[closedFrom:],
		Stats:  stats,
	}
}

// TestRestoredWindowsContinueLikeTheOriginal: after a restore from a checkpoint taken between
// batches, the same batches give the same on-time, late and early panes with the same revisions,
// close the same series and count the same placements — in the original's capacity and in a
// larger one, where the grown series ids are handed out only after the saved free ones.
func TestRestoredWindowsContinueLikeTheOriginal(t *testing.T) {
	rng := rand.New(rand.NewPCG(restoreSeed, restoreSeed))
	for scenario := range restoreScenarios {
		generated := generate(rng)
		split := rng.IntN(len(generated.batches) + 1)
		early := rng.IntN(2) == 0
		original := newRun(t, early)
		for _, b := range generated.batches[:split] {
			original.apply(b)
		}
		data := checkpointOf(t, original)
		capacityAfter := capacity * (1 + rng.IntN(2))
		restored, err := restoreRun(t, data, stream.Options{Capacity: capacityAfter, Early: early})
		if err != nil {
			t.Fatalf("scenario %d: restore: %v", scenario, err)
		}
		if capacityAfter == capacity && !bytes.Equal(data, checkpointOf(t, restored)) {
			t.Fatalf("scenario %d: a restored engine writes other bytes", scenario)
		}

		flush := heartbeat(flushAfter(generated.last))
		want := continueWith(original, generated.batches[split:], flush)
		got := continueWith(restored, generated.batches[split:], flush)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("scenario %d, restored after batch %d into capacity %d (-original +restored):\n%s",
				scenario, split, capacityAfter, diff)
		}
	}
}

func TestASmallerCapacityRefusesTheCheckpoint(t *testing.T) {
	original := newRun(t, true)
	original.apply(randomBatch(rand.New(rand.NewPCG(1, 1)), epoch))

	_, err := restoreRun(t, checkpointOf(t, original), stream.Options{Capacity: capacity - 1, Early: true})

	if !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Errorf("err = %v, want the saved capacity refused", err)
	}
}
