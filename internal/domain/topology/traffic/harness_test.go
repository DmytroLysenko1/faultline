package traffic_test

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	windowSize    = 10 * time.Second
	delay         = 30 * time.Second
	lateness      = 30 * time.Second
	slackWindows  = 2
	earlyPeriod   = time.Second
	testNodes     = 8
	testEdges     = 8
	testSeries    = 64
	perService    = 3
	sketchEntries = 4 * sketch.MaxEntriesPerSketch
	flushAhead    = time.Hour
	callLatency   = 5 * time.Millisecond

	frontendNode = handle.Node(0)
	cartNode     = handle.Node(1)
	frontendCart = handle.Edge(0)
)

// base is a window start, so offsets below read as positions inside windows.
var base = time.Unix(1_700_000_000, 0)

func testLimits() *traffic.Limits {
	return &traffic.Limits{
		Windows:         stream.Config{Size: windowSize, Delay: delay, AllowedLateness: lateness, SlackWindows: slackWindows},
		EarlyFirePeriod: earlyPeriod,
		MaxNodes:        testNodes,
		MaxEdges:        testEdges,
		MaxSeries:       testSeries,
		MaxPerService:   perService,
		SketchEntries:   sketchEntries,
	}
}

type collector struct {
	finals []traffic.Final
}

func (c *collector) Final(final *traffic.Final) {
	c.finals = append(c.finals, *final)
}

type harness struct {
	t       *testing.T
	traffic *traffic.Traffic
	sink    *collector
}

func newHarness(t *testing.T, limits *traffic.Limits) *harness {
	t.Helper()
	sink := &collector{}
	built, err := traffic.New(limits, sink)
	if err != nil {
		t.Fatalf("traffic.New: %v", err)
	}
	return &harness{t: t, traffic: built, sink: sink}
}

func endingAt(t *testing.T, end time.Time) span.Span {
	t.Helper()
	return telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
		f.SpanID = ids.SpanID{1}
		f.Start = uint64(end.Add(-callLatency).UnixNano())
		f.End = uint64(end.UnixNano())
	})))
}

// begin opens a batch received at receivedAt whose latest event ended at latest.
func (h *harness) begin(receivedAt, latest time.Time) {
	h.t.Helper()
	received, err := batch.New([]span.Span{endingAt(h.t, latest)}, receivedAt)
	if err != nil {
		h.t.Fatalf("batch.New: %v", err)
	}
	h.traffic.Begin(received)
}

func (h *harness) batch(receivedAt, latest time.Time, record func()) {
	h.t.Helper()
	h.begin(receivedAt, latest)
	record()
	h.traffic.End()
}

func (h *harness) heartbeat(at time.Time) {
	h.traffic.Begin(batch.Heartbeat(at))
	h.traffic.End()
}

func (h *harness) flush() {
	h.heartbeat(base.Add(flushAhead))
}

func timed(end time.Time) eventtime.Interval {
	return eventtime.FromUnixNanos(uint64(end.Add(-callLatency).UnixNano()), uint64(end.UnixNano()))
}

func httpOperation(name string) operation.Operation {
	return operation.Operation{Protocol: operation.ProtocolHTTP, Name: name}
}

func frontendToCart(at time.Time, op operation.Operation, failed bool) *call.Call {
	return &call.Call{
		Caller:    telemetrytest.Service("frontend"),
		Callee:    telemetrytest.Service("cart"),
		Failed:    failed,
		Interval:  timed(at),
		Operation: op,
	}
}

func (h *harness) call(at time.Time, op operation.Operation, failed bool) {
	h.traffic.RecordCall(frontendCart, cartNode, frontendToCart(at, op, failed))
}

func servedBy(t *testing.T, service string, at time.Time, op operation.Operation) *span.Span {
	t.Helper()
	served := telemetrytest.Server(t, 1, 2, service, nil)
	served.Interval, served.Operation = timed(at), op
	return &served
}

func (h *harness) finalsOf(kind traffic.SeriesKind) []traffic.Final {
	var matching []traffic.Final
	for i := range h.sink.finals {
		if h.sink.finals[i].Series.Kind == kind {
			matching = append(matching, h.sink.finals[i])
		}
	}
	return matching
}

func edgeSeries(kind traffic.SeriesKind) traffic.SeriesKey {
	return traffic.SeriesKey{
		Kind:    kind,
		Subject: traffic.SubjectEdge,
		Caller:  telemetrytest.Service("frontend"),
		Callee:  telemetrytest.Service("cart"),
	}
}

func operationSeries(subject *traffic.SeriesKey, op operation.Operation) traffic.SeriesKey {
	series := *subject
	series.Kind, series.Protocol, series.Operation = traffic.SeriesOperation, op.Protocol, op.Name
	return series
}
