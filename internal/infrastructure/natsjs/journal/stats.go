package journal

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const latencyBoundCount = 11

const latencyBuckets = latencyBoundCount + 1

type Bounds [latencyBoundCount]time.Duration

func LatencyBounds() Bounds {
	return Bounds{
		time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
		25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond,
		500 * time.Millisecond, time.Second, 2 * time.Second,
	}
}

type Histogram [latencyBuckets]uint64

type Stats struct {
	Published      uint64
	Duplicates     uint64
	Failed         [natsjs.ReasonCount]uint64
	EncodeTooLarge uint64
	Doomed         uint64
	Closing        uint64
	Interrupted    uint64
	PubAck         Histogram
	Append         Histogram
}

type histogram [latencyBuckets]atomic.Uint64

func (h *histogram) observe(took time.Duration) {
	for bucket, bound := range LatencyBounds() {
		if took <= bound {
			h[bucket].Add(1)
			return
		}
	}
	h[latencyBoundCount].Add(1)
}

func (h *histogram) snapshot() Histogram {
	var counts Histogram
	for bucket := range h {
		counts[bucket] = h[bucket].Load()
	}
	return counts
}

type counters struct {
	published      atomic.Uint64
	duplicates     atomic.Uint64
	failed         [natsjs.ReasonCount]atomic.Uint64
	encodeTooLarge atomic.Uint64
	doomed         atomic.Uint64
	closing        atomic.Uint64
	interrupted    atomic.Uint64
	pubAck         histogram
	append         histogram
}

func (c *counters) record(took time.Duration, duplicate bool, err error) {
	c.pubAck.observe(took)
	if err != nil {
		c.failed[reasonOf(err)].Add(1)
		return
	}
	if duplicate {
		c.duplicates.Add(1)
		return
	}
	c.published.Add(1)
}

func reasonOf(err error) natsjs.Reason {
	failure, classified := errors.AsType[*natsjs.PublishError](err)
	if !classified || int(failure.Reason) >= natsjs.ReasonCount {
		return natsjs.ReasonUnexpected
	}
	return failure.Reason
}

func (c *counters) observeAppend(took time.Duration) {
	c.append.observe(took)
}

func (c *counters) snapshot() Stats {
	stats := Stats{
		Published:      c.published.Load(),
		Duplicates:     c.duplicates.Load(),
		EncodeTooLarge: c.encodeTooLarge.Load(),
		Doomed:         c.doomed.Load(),
		Closing:        c.closing.Load(),
		Interrupted:    c.interrupted.Load(),
		PubAck:         c.pubAck.snapshot(),
		Append:         c.append.snapshot(),
	}
	for reason := range c.failed {
		stats.Failed[reason] = c.failed[reason].Load()
	}
	return stats
}
