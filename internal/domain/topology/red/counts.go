package red

import (
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

type Counts struct {
	Requests uint64
	Failed   uint64
}

func (c *Counts) Record(failed bool) {
	c.Requests++
	if failed {
		c.Failed++
	}
}

type Cell struct {
	Counts
	Latency sketch.Sketch
}

func (c *Cell) Release(arena *sketch.Arena) {
	arena.Release(&c.Latency)
	*c = Cell{}
}

func (c *Cell) Record(arena *sketch.Arena, failed bool, latency time.Duration, latencyKnown bool) {
	c.Counts.Record(failed)
	if latencyKnown {
		arena.Add(&c.Latency, int64(latency))
	}
}
