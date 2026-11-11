package traffic

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type event struct {
	subject   subjectKey
	owner     handle.Node
	interval  eventtime.Interval
	operation operation.Operation
	failed    bool
}

func (t *Traffic) Begin(received batch.Batch) {
	candidate := markOf(received.Candidate())
	current := t.watermark.Advance(received.ReceivedAt, candidate)
	anchor := t.watermark.Anchor(candidate, received.ReceivedAt)
	t.untimedAt = t.acceptedFrom(current, anchor)
	t.subjects.Begin(current, anchor)
	t.operations.Begin(current, anchor)
	t.overflow.Begin(current, anchor)
	if received.Heartbeat {
		t.counters.Heartbeats++
	}
}

func (t *Traffic) End() {
	t.subjects.End()
	t.operations.End()
	t.overflow.End()
}

func (t *Traffic) Fire() {
	if reached := t.watermark.MaxReceivedAt; reached.Known {
		t.subjects.FireEarly(reached.Time)
	}
}

func (t *Traffic) RecordCall(edge handle.Edge, callee handle.Node, observed *call.Call) {
	if edge == handle.None {
		return
	}
	t.book.noteEdge(edge, edgeEnds{
		Caller:     observed.Caller,
		Callee:     observed.Callee,
		CalleeNode: callee,
	})
	t.book.noteNode(callee, observed.Callee)
	t.record(&event{
		subject: subjectKey{
			Subject: int32(edge),
			Kind:    SubjectEdge,
		},
		owner:     callee,
		interval:  observed.Interval,
		operation: observed.Operation,
		failed:    observed.Failed,
	})
}

func (t *Traffic) RecordServed(node handle.Node, observed *span.Span) {
	if node == handle.None {
		return
	}
	t.book.noteNode(node, observed.Service)
	t.record(&event{
		subject: subjectKey{
			Subject: int32(node),
			Kind:    SubjectNode,
		},
		owner:     node,
		interval:  observed.Interval,
		operation: observed.Operation,
		failed:    observed.Failed,
	})
}

func (t *Traffic) record(observed *event) {
	at := t.eventTime(observed.interval)
	t.recordSubject(observed, at)
	t.recordOperation(observed, at)
}

func (t *Traffic) eventTime(interval eventtime.Interval) time.Time {
	end, timed := interval.End()
	if timed {
		return end
	}
	t.counters.Untimed++
	return t.untimedAt
}

func (t *Traffic) acceptedFrom(current stream.Mark, anchor time.Time) time.Time {
	if !current.Known {
		return anchor
	}
	return later(anchor, current.Time.Add(-t.limits.Windows.AllowedLateness))
}

func later(first, second time.Time) time.Time {
	if second.After(first) {
		return second
	}
	return first
}

func (t *Traffic) recordSubject(observed *event, at time.Time) {
	cell, _, _ := t.subjects.Locate(observed.subject, at)
	if cell == nil {
		return
	}
	latency, known := observed.interval.Latency()
	if !known {
		t.counters.LatencyUnknown++
	}
	cell.Record(t.arena, observed.failed, latency, known)
}

func (t *Traffic) recordOperation(observed *event, at time.Time) {
	if placement := t.operations.Classify(at); placement.Dropped() {
		t.operations.Drop(placement)
		return
	}
	t.operationCounts(observed, at).Record(observed.failed)
}

func (t *Traffic) operationCounts(observed *event, at time.Time) *red.Counts {
	id, known := t.registry.Lookup(observed.owner, observed.operation)
	if key := observed.subject.withOperation(id); known && t.operations.Has(key) {
		counts, _, _ := t.operations.Locate(key, at)
		return counts
	}
	if counts, admitted := t.admit(observed, at); admitted {
		return counts
	}
	counts, _, _ := t.overflow.Locate(observed.subject, at)
	return counts
}

func (t *Traffic) admit(observed *event, at time.Time) (*red.Counts, bool) {
	if t.operations.Len() >= t.operations.Capacity() {
		return nil, false
	}
	id, admitted := t.registry.Acquire(observed.owner, observed.operation)
	if !admitted {
		return nil, false
	}
	counts, _, created := t.operations.Locate(observed.subject.withOperation(id), at)
	if !created {
		panic("traffic: an acquired operation found its series already open")
	}
	return counts, true
}

func markOf(at time.Time, known bool) stream.Mark {
	return stream.Mark{
		Time:  at,
		Known: known,
	}
}
