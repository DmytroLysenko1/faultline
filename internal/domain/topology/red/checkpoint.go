package red

import (
	"math"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

func (c *Counts) WriteCheckpoint(w *checkpoint.Writer) {
	w.Uint(c.Requests)
	w.Uint(c.Failed)
}

func (c *Counts) ReadCheckpoint(r *checkpoint.Reader) {
	c.Requests, c.Failed = r.Uint(), r.Uint()
	if c.Failed > c.Requests || c.Requests > math.MaxInt64 {
		r.Fail("more failed requests than requests, or a count near wrapping")
	}
}

func (c *Cell) WriteCheckpoint(w *checkpoint.Writer, arena *sketch.Arena) {
	c.Counts.WriteCheckpoint(w)
	arena.WriteSketch(w, &c.Latency)
}

func (c *Cell) ReadCheckpoint(r *checkpoint.Reader, arena *sketch.ArenaRestore) {
	c.Counts.ReadCheckpoint(r)
	arena.ReadSketch(r, &c.Latency)
	if c.Latency.Count > c.Requests {
		r.Fail("more latency samples than requests")
	}
}

func (s WindowStats) WriteCheckpoint(w *checkpoint.Writer) {
	w.Int(s.StartUnixNano)
	w.Uint(uint64(s.Kind))
	w.Uint(uint64(s.Revision))
	w.Uint(s.Requests)
	w.Uint(s.Failed)
	w.Uint(s.LatencySamples)
	for _, value := range []int64{int64(s.P50), int64(s.P95), int64(s.P99), int64(s.Observed)} {
		w.Int(value)
	}
}

func ReadWindowStats(r *checkpoint.Reader) WindowStats {
	return WindowStats{
		StartUnixNano:  r.Int(),
		Kind:           PaneKind(r.Uint8(uint8(PaneLate))),
		Revision:       r.Uint32(math.MaxUint32),
		Requests:       r.Uint(),
		Failed:         r.Uint(),
		LatencySamples: r.Uint(),
		P50:            r.Duration(),
		P95:            r.Duration(),
		P99:            r.Duration(),
		Observed:       r.Duration(),
	}
}
