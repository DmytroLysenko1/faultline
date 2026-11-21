package topology_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	filledNodes          = 16
	filledFanOut         = 2
	filledEdges          = filledNodes * filledFanOut
	filledPending        = 1 << 12
	filledTombstones     = 1 << 11
	filledSeries         = 128
	filledOperationsEach = 8
	filledSketchEntries  = 1 << 12
	filledWindows        = 7
	filledCallsPerCell   = 16
	filledCells          = filledWindows * filledEdges
	filledRounds         = 2
	filledRoundGap       = 2 * time.Minute
	filledEndLead        = time.Millisecond
	filledClockAge       = time.Duration(1) << 62
	filledSecondFraction = time.Second - time.Nanosecond
	filledEarliestStart  = 1
	skewedPending        = 1 << 14
	skewedSketchEntries  = 1 << 11
	fewestTombstones     = 1
	fewestPending        = 1
	fewPending           = 16
	skewedTombstones     = 1 << 14
	defaultRatioPending  = 1 << 10
	defaultRatioOneTo    = 10
	addedEntries         = 1 << 12
	filledFailOneIn      = 4
	filledNameDigits     = 8
	filledCalleeSpanID   = 2
	filledWindowSize     = 10 * time.Second
)

type filledCall struct {
	trace     uint64
	caller    int
	callee    int
	start     uint64
	end       time.Time
	operation int
	failed    bool
}

type filledPlan struct {
	callsPerCell int
}

type filledShape struct {
	Pending, Tombstones, Nodes, Edges, Series, SketchEntries int
}

func filledLimits() *topology.Limits {
	limits := builderLimits()
	limits.MaxPending, limits.MaxTombstones = filledPending, filledTombstones
	limits.MaxNodes, limits.MaxEdges = filledNodes, filledEdges
	limits.MaxSeries, limits.MaxOperationsPerService = filledSeries, filledOperationsEach
	limits.SketchEntries = filledSketchEntries
	return limits
}

func longestOperation(owner, index int) operation.Operation {
	prefix := fmt.Sprintf("%0*d", filledNameDigits, owner*filledOperationsEach+index)
	return operation.Operation{
		Protocol: operation.ProtocolHTTP,
		Name:     prefix + strings.Repeat("x", operation.MaxNameBytes-len(prefix)),
	}
}

func filledHalf(tb testing.TB, spanKind kind.Kind, service int, observed *filledCall) span.Span {
	tb.Helper()
	fields := span.Fields{
		TraceID: telemetrytest.NumberedTrace(observed.trace),
		SpanID:  ids.SpanID{telemetrytest.CallerSpanID},
		Kind:    spanKind,
	}
	if spanKind == kind.Server {
		fields.SpanID, fields.ParentSpanID = ids.SpanID{filledCalleeSpanID}, ids.SpanID{telemetrytest.CallerSpanID}
	}
	half := telemetrytest.MustSpan(tb, telemetrytest.WorstResource(service), &fields)
	half.Interval = eventtime.FromUnixNanos(observed.start, unixNanos(observed.end))
	half.Operation = longestOperation(observed.callee, observed.operation)
	half.Failed = observed.failed && spanKind == kind.Server
	return half
}

func unixNanos(at time.Time) uint64 {
	return uint64(at.UnixNano())
}

func planFor(limits *topology.Limits) filledPlan {
	return filledPlan{callsPerCell: max(filledCallsPerCell, (limits.MaxTombstones+filledCells-1)/filledCells)}
}

func (p filledPlan) calls() int {
	return filledCells * p.callsPerCell
}

func (p filledPlan) spreadStart(end time.Time, variant int) uint64 {
	return filledEarliestStart + (unixNanos(end)-filledEarliestStart)/uint64(p.callsPerCell)*uint64(variant)
}

func (p filledPlan) roundCalls(round int, at time.Time) []filledCall {
	return lo.Times(p.calls(), func(index int) filledCall {
		variant := index % p.callsPerCell
		edge := index / p.callsPerCell % filledEdges
		window := index / (p.callsPerCell * filledEdges)
		caller := edge / filledFanOut
		end := at.Add(-time.Duration(window)*filledWindowSize - filledEndLead)
		return filledCall{
			trace:     uint64(round*p.calls() + index),
			caller:    caller,
			callee:    (caller + edge%filledFanOut + 1) % filledNodes,
			start:     p.spreadStart(end, variant),
			end:       end,
			operation: variant % filledOperationsEach,
			failed:    index%filledFailOneIn == 0,
		}
	})
}

func (p filledPlan) round(tb testing.TB, round int, at time.Time) batch.Batch {
	tb.Helper()
	spans := lo.FlatMap(p.roundCalls(round, at), func(observed filledCall, _ int) []span.Span {
		return []span.Span{
			filledHalf(tb, kind.Client, observed.caller, &observed),
			filledHalf(tb, kind.Server, observed.callee, &observed),
		}
	})
	received, err := batch.New(spans, at)
	if err != nil {
		tb.Fatalf("batch.New: %v", err)
	}
	return received
}

func (p filledPlan) pendingHalves(tb testing.TB, pending int, at time.Time) batch.Batch {
	tb.Helper()
	spans := lo.Times(pending, func(index int) span.Span {
		return filledHalf(tb, kind.Client, index%filledNodes, &filledCall{
			trace:     uint64(filledRounds*p.calls() + index),
			callee:    index % filledNodes,
			start:     filledEarliestStart,
			end:       at.Add(-filledEndLead),
			operation: index % filledOperationsEach,
		})
	})
	received, err := batch.New(spans, at)
	if err != nil {
		tb.Fatalf("batch.New: %v", err)
	}
	return received
}

func matcherLimits(pending, tombstones int) *topology.Limits {
	limits := filledLimits()
	limits.MaxPending, limits.MaxTombstones = pending, tombstones
	return limits
}

func skewedTowardPending() *topology.Limits {
	limits := matcherLimits(skewedPending, fewestTombstones)
	limits.SketchEntries = skewedSketchEntries
	return limits
}

func filledBuilder(tb testing.TB, limits *topology.Limits) *topology.Builder {
	tb.Helper()
	builder, err := topology.NewBuilder(limits, telemetrytest.Epoch())
	if err != nil {
		tb.Fatalf("NewBuilder: %v", err)
	}
	builder.Apply(batch.Heartbeat(telemetrytest.Epoch()))
	plan := planFor(limits)
	var at time.Time
	for round := range filledRounds {
		at = telemetrytest.Epoch().Add(filledClockAge + time.Duration(round+1)*filledRoundGap + filledSecondFraction)
		builder.Apply(plan.round(tb, round, at))
		builder.Fire()
	}
	builder.Apply(plan.pendingHalves(tb, limits.MaxPending, at))
	builder.Fire()
	return builder
}

func sectionBytes(image []byte) map[checkpoint.Tag]int64 {
	_, parts := sections(image)
	return lo.SliceToMap(parts, func(part []byte) (checkpoint.Tag, int64) {
		return checkpoint.Tag(part[0]), int64(len(part))
	})
}

func shapeOf(builder *topology.Builder) filledShape {
	snapshot := builder.Snapshot()
	return filledShape{
		Pending:       builder.Matcher.Pending.Len(),
		Tombstones:    builder.Matcher.Tombstones.Size,
		Nodes:         len(snapshot.Nodes),
		Edges:         len(snapshot.Edges),
		Series:        snapshot.TrafficStats.Operations.Series,
		SketchEntries: snapshot.TrafficStats.Sketch.InUse,
	}
}

func TestTheCheckpointSectionsAreTheOnesTheBuilderWrites(t *testing.T) {
	steady, _ := steadyBuilder(t)

	err := checkpoint.Walk(bytes.NewReader(writeTB(t, steady)), topology.CheckpointSections()...)

	if err != nil {
		t.Fatalf("Walk with CheckpointSections: %v", err)
	}
}

func limitShape(limits *topology.Limits) filledShape {
	return filledShape{
		Pending:       limits.MaxPending,
		Tombstones:    limits.MaxTombstones,
		Nodes:         limits.MaxNodes,
		Edges:         limits.MaxEdges,
		Series:        limits.MaxSeries,
		SketchEntries: limits.SketchEntries,
	}
}

func filledTB(tb testing.TB, limits *topology.Limits) *topology.Builder {
	tb.Helper()
	builder := filledBuilder(tb, limits)
	if diff := cmp.Diff(limitShape(limits), shapeOf(builder)); diff != "" {
		tb.Fatalf("filled builder (-want +got):\n%s", diff)
	}
	return builder
}

func TestAFilledCheckpointFitsTheWorstImageOfItsLimits(t *testing.T) {
	tests := []struct {
		name   string
		limits *topology.Limits
	}{
		{name: "every limit filled", limits: filledLimits()},
		{name: "the smallest matcher", limits: matcherLimits(fewestPending, fewestTombstones)},
		{name: "skewed toward pending", limits: skewedTowardPending()},
		{name: "skewed toward tombstones", limits: matcherLimits(fewPending, skewedTombstones)},
		{name: "cmd default ratio 1:10", limits: matcherLimits(defaultRatioPending, defaultRatioPending*defaultRatioOneTo)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image := writeTB(t, filledTB(t, tt.limits))

			assertSectionsFit(t, sectionBytes(image), tt.limits.WorstSectionBytes())
			if worst := tt.limits.WorstCheckpointBytes(); int64(len(image)) > worst {
				t.Errorf("image of %d bytes is over the worst image %d of its limits", len(image), worst)
			}
		})
	}
}

func assertSectionsFit(t *testing.T, written, worst map[checkpoint.Tag]int64) {
	t.Helper()
	for _, tag := range []checkpoint.Tag{topology.SectionMatcher, topology.SectionGraph, topology.SectionTraffic} {
		if written[tag] > worst[tag] {
			t.Errorf("section %d of %d bytes is over its share %d of the worst image", tag, written[tag], worst[tag])
		}
	}
}

func TestEachMatcherPartPaysForItsOwnEntries(t *testing.T) {
	tests := []struct {
		name   string
		raised *topology.Limits
	}{
		{name: "pending halves", raised: matcherLimits(fewPending+addedEntries, fewestTombstones)},
		{name: "tombstones", raised: matcherLimits(fewPending, fewestTombstones+addedEntries)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := matcherLimits(fewPending, fewestTombstones)

			written := matcherSectionBytes(t, tt.raised) - matcherSectionBytes(t, base)
			share := tt.raised.WorstSectionBytes()[topology.SectionMatcher] -
				base.WorstSectionBytes()[topology.SectionMatcher]

			if written > share {
				t.Errorf("%d more %s write %d bytes, over their share %d", addedEntries, tt.name, written, share)
			}
		})
	}
}

func matcherSectionBytes(t *testing.T, limits *topology.Limits) int64 {
	t.Helper()
	return sectionBytes(writeTB(t, filledTB(t, limits)))[topology.SectionMatcher]
}

func TestTheWorstImageGrowsWithEveryLimitItCovers(t *testing.T) {
	tests := []struct {
		name  string
		raise func(l *topology.Limits)
	}{
		{name: "pending halves", raise: func(l *topology.Limits) { l.MaxPending *= 2 }},
		{name: "tombstones", raise: func(l *topology.Limits) { l.MaxTombstones *= 2 }},
		{name: "nodes", raise: func(l *topology.Limits) { l.MaxNodes *= 2 }},
		{name: "edges", raise: func(l *topology.Limits) { l.MaxEdges *= 2 }},
		{name: "series", raise: func(l *topology.Limits) { l.MaxSeries *= 2 }},
		{name: "sketch entries", raise: func(l *topology.Limits) { l.SketchEntries *= 2 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := builderLimits()
			raised := builderLimits()
			tt.raise(raised)

			if before, after := base.WorstCheckpointBytes(), raised.WorstCheckpointBytes(); after <= before {
				t.Fatalf("worst image %d after raising %s, want more than %d", after, tt.name, before)
			}
		})
	}
}
