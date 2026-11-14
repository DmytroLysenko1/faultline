package feed

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

type StopReason uint32

const (
	StopNone StopReason = iota
	StopClosed
	StopLeaseLost
	StopIdentity
	StopCorrupt
	StopConsumerLost
	stopReasonCount
)

const unknownStopReason = "unknown"

var stopReasonNames = [stopReasonCount]string{
	StopNone:         "none",
	StopClosed:       "closed",
	StopLeaseLost:    "lease_lost",
	StopIdentity:     "identity",
	StopCorrupt:      "corrupt",
	StopConsumerLost: "consumer_lost",
}

func (r StopReason) String() string {
	if r >= stopReasonCount {
		return unknownStopReason
	}
	return stopReasonNames[r]
}

type stopRule struct {
	target error
	reason StopReason
}

var stopRules = []stopRule{
	{target: processing.ErrFeedClosed, reason: StopClosed},
	{target: ErrLeaseLost, reason: StopLeaseLost},
	{target: ErrStreamIdentity, reason: StopIdentity},
	{target: ErrCorruptRecord, reason: StopCorrupt},
}

func stopReasonOf(cause error) StopReason {
	for _, rule := range stopRules {
		if errors.Is(cause, rule.target) {
			return rule.reason
		}
	}
	return StopConsumerLost
}

type Stats struct {
	Read                  uint64
	DeliveredBatches      uint64
	Heartbeats            uint64
	Held                  uint64
	Released              uint64
	ConsumerResets        uint64
	IdentityCheckFailures uint64
	SequenceGapRecords    uint64
	ReceivedAtClamped     uint64
	ClockSkewMax          time.Duration
	PendingRecords        uint64
	Lag                   time.Duration
	Stop                  StopReason
}

type counters struct {
	reads            atomic.Uint64
	delivered        atomic.Uint64
	heartbeats       atomic.Uint64
	held             atomic.Uint64
	released         atomic.Uint64
	resets           atomic.Uint64
	identityFailures atomic.Uint64
	gaps             atomic.Uint64
	clamped          atomic.Uint64
	skew             atomic.Int64
	pending          atomic.Uint64
	stamped          atomic.Int64
	stop             atomic.Uint32
}

func (c *counters) read(next *message) {
	c.reads.Add(1)
	c.held.Store(1)
	c.pending.Store(next.pending)
	c.stamped.Store(next.stamped.UnixNano())
}

func (c *counters) handed(heartbeat bool) {
	c.held.Store(0)
	if heartbeat {
		c.heartbeats.Add(1)
		return
	}
	c.delivered.Add(1)
}

func (c *counters) skewed(skew time.Duration) {
	if int64(skew) > c.skew.Load() {
		c.skew.Store(int64(skew))
	}
}

func (c *counters) snapshot() Stats {
	return Stats{
		Read:                  c.reads.Load(),
		DeliveredBatches:      c.delivered.Load(),
		Heartbeats:            c.heartbeats.Load(),
		Held:                  c.held.Load(),
		Released:              c.released.Load(),
		ConsumerResets:        c.resets.Load(),
		IdentityCheckFailures: c.identityFailures.Load(),
		SequenceGapRecords:    c.gaps.Load(),
		ReceivedAtClamped:     c.clamped.Load(),
		ClockSkewMax:          time.Duration(c.skew.Load()),
		PendingRecords:        c.pending.Load(),
		Lag:                   c.lag(),
		Stop:                  StopReason(c.stop.Load()),
	}
}

func (c *counters) lag() time.Duration {
	stamped := c.stamped.Load()
	if stamped == 0 || c.pending.Load() == 0 && c.held.Load() == 0 {
		return 0
	}
	return max(time.Since(time.Unix(0, stamped)), 0)
}
