package stream_test

import (
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

type expected struct {
	requests uint64
	values   []int64
}

type counts struct {
	LateDropped uint64
	NoWatermark uint64
	Reassigned  uint64
}

type oracle struct {
	cells  map[cellKey]*expected
	counts counts
}

type clock struct {
	watermark    time.Time
	maxCandidate time.Time
	known        bool
}

func (c *clock) advance(b batch) time.Time {
	candidate := candidateOf(b)
	if !candidate.Known {
		return c.maxCandidate
	}
	if !c.known || candidate.Time.After(c.maxCandidate) {
		c.maxCandidate = candidate.Time
	}
	if mark := candidate.Time.Add(-delay); !c.known || mark.After(c.watermark) {
		c.watermark = mark
	}
	c.known = true
	return candidate.Time
}

func recompute(batches []batch) oracle {
	result := oracle{cells: make(map[cellKey]*expected)}
	var c clock
	for _, b := range batches {
		anchor := c.advance(b)
		for _, e := range b.events {
			result.place(e, c, anchor)
		}
	}
	return result
}

func (o *oracle) place(e event, c clock, anchor time.Time) {
	if !c.known {
		o.counts.NoWatermark++
		return
	}
	t := e.at
	if e.untimed {
		t = anchor
	}
	if c.watermark.Sub(t) > lateness {
		o.counts.LateDropped++
		return
	}
	o.add(cellKey{key: e.key, window: o.windowOf(t, c, anchor)}, e.latency)
}

func (o *oracle) windowOf(t time.Time, c clock, anchor time.Time) int64 {
	firstLive := config.WindowOf(c.watermark.Add(-lateness)).ID
	last := firstLive + int64(config.Slots()) - 1
	id := config.WindowOf(t).ID
	if id <= last {
		return id
	}
	o.counts.Reassigned++
	return min(max(config.WindowOf(anchor).ID, firstLive), last)
}

func (o *oracle) add(key cellKey, latency int64) {
	if o.cells[key] == nil {
		o.cells[key] = &expected{}
	}
	o.cells[key].requests++
	o.cells[key].values = append(o.cells[key].values, latency)
}

func (o oracle) sketchOf(arena *sketch.Arena, key cellKey) *sketch.Sketch {
	s := new(sketch.Sketch)
	for _, value := range o.cells[key].values {
		arena.Add(s, value)
	}
	return s
}
