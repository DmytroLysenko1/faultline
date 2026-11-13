package sketch_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

const (
	pressureSketches   = 24
	pressureBudget     = 4096
	pressureSeed       = 20261113
	pressureScenarios  = 60
	pressureSteps      = 20_000
	pressureRelease    = 2_000
	roomyBudget        = 8 * pressureBudget
	pressureMagnitudes = 40
	arenaSection       = checkpoint.Tag(1)
)

type workload struct {
	arena    *sketch.Arena
	sketches []sketch.Sketch
}

// step adds a random sample or releases a sketch: many sketches with wide value ranges fight for a
// small arena, so blocks fragment, sketches collapse and samples drop.
func (w *workload) step(rng *rand.Rand) {
	target := &w.sketches[rng.IntN(len(w.sketches))]
	if rng.IntN(pressureRelease) == 0 {
		w.arena.Release(target)
		return
	}
	magnitude := int64(1) << rng.IntN(pressureMagnitudes)
	w.arena.Add(target, magnitude+rng.Int64N(magnitude)-1)
}

func (w *workload) observe() any {
	type observed struct {
		Stats       sketch.ArenaStats
		Counts      []uint64
		Percentiles []sketch.Percentiles
	}
	got := observed{Stats: w.arena.Stats}
	for i := range w.sketches {
		percentiles, _ := w.arena.Percentiles(&w.sketches[i])
		got.Counts = append(got.Counts, w.sketches[i].Count)
		got.Percentiles = append(got.Percentiles, percentiles)
	}
	return got
}

func (w *workload) checkpoint(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	writer := checkpoint.NewWriter(&out, 1)
	writer.Begin(arenaSection)
	w.arena.WriteCheckpoint(writer)
	for i := range w.sketches {
		w.arena.WriteSketch(writer, &w.sketches[i])
	}
	writer.End()
	if err := writer.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreWorkload(budget int, data []byte, sketches int) (*workload, error) {
	reader := checkpoint.NewReader(bytes.NewReader(data))
	reader.Begin(arenaSection)
	restore, err := sketch.RestoreArena(budget, reader)
	if err != nil {
		return nil, err
	}
	restored := &workload{
		arena:    restore.Arena,
		sketches: make([]sketch.Sketch, sketches),
	}
	for i := range restored.sketches {
		restore.ReadSketch(reader, &restored.sketches[i])
	}
	restore.Finish(reader)
	reader.End()
	return restored, reader.Finish()
}

// TestARestoredArenaContinuesLikeTheOriginal: the free lists keep their order and every sketch keeps
// its block, length and collapse mark, so the same adds after a restore fragment, collapse and drop
// exactly as in the arena that never stopped.
func TestARestoredArenaContinuesLikeTheOriginal(t *testing.T) {
	rng := rand.New(rand.NewPCG(pressureSeed, pressureSeed))
	pressured := 0
	defer func() {
		if pressured < pressureScenarios/2 {
			t.Errorf("only %d of %d scenarios put the arena under pressure", pressured, pressureScenarios)
		}
	}()
	for scenario := range pressureScenarios {
		original := newWorkload(t, pressureBudget)
		for range rng.IntN(pressureSteps) {
			original.step(rng)
		}
		written := original.checkpoint(t)
		restored, err := restoreWorkload(pressureBudget, written, pressureSketches)
		if err != nil {
			t.Fatalf("scenario %d: restore: %v", scenario, err)
		}
		if !bytes.Equal(written, restored.checkpoint(t)) {
			t.Fatalf("scenario %d: the restored arena writes other bytes: free lists or blocks moved", scenario)
		}
		if diff := cmp.Diff(original.observe(), restored.observe()); diff != "" {
			t.Fatalf("scenario %d: restore changed the arena (-original +restored):\n%s", scenario, diff)
		}
		seed := rng.Uint64()
		originalRNG, restoredRNG := rand.New(rand.NewPCG(seed, 1)), rand.New(rand.NewPCG(seed, 1))
		for range pressureSteps {
			original.step(originalRNG)
			restored.step(restoredRNG)
		}
		if original.arena.Stats.CollapsedSketches == 0 {
			continue
		}
		pressured++
		if diff := cmp.Diff(original.observe(), restored.observe()); diff != "" {
			t.Fatalf("scenario %d: the restored arena diverged (-original +restored):\n%s", scenario, diff)
		}
	}
}

// TestAnArenaRestoresIntoALargerBudget: the old blocks keep their offsets and the new half of the
// arena joins the free lists, so every sketch reads the same and new sketches fit where they did not.
func TestAnArenaRestoresIntoALargerBudget(t *testing.T) {
	rng := rand.New(rand.NewPCG(pressureSeed, 2))
	original := newWorkload(t, pressureBudget)
	for range pressureSteps {
		original.step(rng)
	}

	if original.arena.Stats.DroppedSamples == 0 {
		t.Fatalf("the workload never filled the small arena: %+v", original.arena.Stats)
	}
	grown, err := restoreWorkload(roomyBudget, original.checkpoint(t), pressureSketches)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	want, got := original.observe(), grown.observe()
	if diff := cmp.Diff(want, got, cmp.FilterPath(func(path cmp.Path) bool {
		return path.Last().String() == ".Budget"
	}, cmp.Ignore())); diff != "" {
		t.Errorf("growing the budget changed the sketches (-original +grown):\n%s", diff)
	}
	if grown.arena.Stats.Budget != roomyBudget {
		t.Errorf("budget = %d, want %d", grown.arena.Stats.Budget, roomyBudget)
	}
	dropped := grown.arena.Stats.DroppedSamples
	for range pressureSteps {
		grown.step(rng)
	}
	if grown.arena.Stats.DroppedSamples != dropped {
		t.Errorf("the grown arena dropped %d samples: its new half never joined the free lists",
			grown.arena.Stats.DroppedSamples-dropped)
	}
	for i := range grown.sketches {
		grown.arena.Release(&grown.sketches[i])
	}
	if grown.arena.Stats.InUse != 0 {
		t.Errorf("after releasing every sketch %d entries stay in use", grown.arena.Stats.InUse)
	}
}

func TestASmallerBudgetIsRejected(t *testing.T) {
	original := newWorkload(t, pressureBudget)

	_, err := restoreWorkload(pressureBudget/2, original.checkpoint(t), pressureSketches)

	if !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Errorf("err = %v, want a refused budget", err)
	}
}

func newWorkload(t *testing.T, budget int) *workload {
	t.Helper()
	arena, err := sketch.NewArena(budget)
	if err != nil {
		t.Fatalf("NewArena: %v", err)
	}
	return &workload{
		arena:    arena,
		sketches: make([]sketch.Sketch, pressureSketches),
	}
}
