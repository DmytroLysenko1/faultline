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
	StopUnknownVersion
	StopSystemicCorruption
	StopDeadLetterRejected
	StopConsumerLost
	stopReasonCount
)

const unknownStopReason = "unknown"

var stopReasonNames = [stopReasonCount]string{
	StopNone:               "none",
	StopClosed:             "closed",
	StopLeaseLost:          "lease_lost",
	StopIdentity:           "identity",
	StopUnknownVersion:     "unknown_version",
	StopSystemicCorruption: "systemic_corruption",
	StopDeadLetterRejected: "dead_letter_rejected",
	StopConsumerLost:       "consumer_lost",
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
	{target: processing.ErrLeaseLost, reason: StopLeaseLost},
	{target: ErrStreamIdentity, reason: StopIdentity},
	{target: ErrUnknownRecordVersion, reason: StopUnknownVersion},
	{target: ErrSystemicCorruption, reason: StopSystemicCorruption},
	{target: ErrDeadLetterRejected, reason: StopDeadLetterRejected},
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
	Read                     uint64
	DeliveredBatches         uint64
	Heartbeats               uint64
	Held                     uint64
	Released                 uint64
	ConsumerResets           uint64
	IdentityCheckFailures    uint64
	SequenceGapRecords       uint64
	ReceivedAtClamped        uint64
	ClockSkewMax             time.Duration
	PendingRecords           uint64
	Lag                      time.Duration
	Stop                     StopReason
	DeadLettered             uint64
	DeadLetterStalled        time.Time
	DeadLetterLastError      DeadLetterClass
	HeartbeatsWritten        uint64
	HeartbeatConflicts       uint64
	HeartbeatConflictsUnread uint64
	HeartbeatFailures        uint64
	HeartbeatsUnconditional  uint64
	PurgedThrough            uint64
	PurgeFailures            uint64
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
	dead             atomic.Uint64
	stalledSince     atomic.Int64
	deadLetterError  atomic.Pointer[DeadLetterClass]

	heartbeatsWritten        atomic.Uint64
	heartbeatConflicts       atomic.Uint64
	heartbeatConflictsUnread atomic.Uint64
	heartbeatFailures        atomic.Uint64
	heartbeatsUnconditional  atomic.Uint64

	purged        atomic.Uint64
	purgeFailures atomic.Uint64
}

func (c *counters) deadLettered() {
	c.held.Store(0)
	c.dead.Add(1)
}

func (c *counters) stalled(class DeadLetterClass) {
	c.noteDeadLetterError(class)
	c.stalledSince.CompareAndSwap(0, time.Now().UnixNano())
}

func (c *counters) unstalled() {
	c.stalledSince.Store(0)
}

func (c *counters) noteDeadLetterError(class DeadLetterClass) {
	c.deadLetterError.Store(&class)
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

func (c *counters) snapshot() Stats {
	return Stats{
		Read:                     c.reads.Load(),
		DeliveredBatches:         c.delivered.Load(),
		Heartbeats:               c.heartbeats.Load(),
		Held:                     c.held.Load(),
		Released:                 c.released.Load(),
		ConsumerResets:           c.resets.Load(),
		IdentityCheckFailures:    c.identityFailures.Load(),
		SequenceGapRecords:       c.gaps.Load(),
		ReceivedAtClamped:        c.clamped.Load(),
		ClockSkewMax:             time.Duration(c.skew.Load()),
		PendingRecords:           c.pending.Load(),
		Lag:                      c.lag(),
		Stop:                     StopReason(c.stop.Load()),
		DeadLettered:             c.dead.Load(),
		DeadLetterStalled:        c.stalledAt(),
		DeadLetterLastError:      c.lastDeadLetterError(),
		HeartbeatsWritten:        c.heartbeatsWritten.Load(),
		HeartbeatConflicts:       c.heartbeatConflicts.Load(),
		HeartbeatConflictsUnread: c.heartbeatConflictsUnread.Load(),
		HeartbeatFailures:        c.heartbeatFailures.Load(),
		HeartbeatsUnconditional:  c.heartbeatsUnconditional.Load(),
		PurgedThrough:            c.purged.Load(),
		PurgeFailures:            c.purgeFailures.Load(),
	}
}

func (c *counters) stalledAt() time.Time {
	since := c.stalledSince.Load()
	if since == 0 {
		return time.Time{}
	}
	return time.Unix(0, since)
}

func (c *counters) lastDeadLetterError() DeadLetterClass {
	if class := c.deadLetterError.Load(); class != nil {
		return *class
	}
	return ""
}

func (c *counters) lag() time.Duration {
	stamped := c.stamped.Load()
	if stamped == 0 || c.pending.Load() == 0 && c.held.Load() == 0 {
		return 0
	}
	return max(time.Since(time.Unix(0, stamped)), 0)
}
