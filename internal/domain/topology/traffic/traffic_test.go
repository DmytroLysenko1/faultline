package traffic_test

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const uniquePaths = 10

func TestNewRejectsLimitsTheEnginesCannotHold(t *testing.T) {
	tests := []struct {
		Name    string
		Args    func(*traffic.Limits)
		WantErr error
	}{
		{Name: "valid", Args: func(*traffic.Limits) {}},
		{Name: "bad window config", Args: func(l *traffic.Limits) { l.Windows.Size = 0 }, WantErr: traffic.ErrInvalidLimits},
		{Name: "no early period", Args: func(l *traffic.Limits) { l.EarlyFirePeriod = 0 }, WantErr: traffic.ErrInvalidLimits},
		{Name: "early period past the window", Args: func(l *traffic.Limits) { l.EarlyFirePeriod = windowSize + 1 },
			WantErr: traffic.ErrInvalidLimits},
		{Name: "no nodes", Args: func(l *traffic.Limits) { l.MaxNodes = 0 }, WantErr: traffic.ErrInvalidLimits},
		{Name: "no edges", Args: func(l *traffic.Limits) { l.MaxEdges = 0 }, WantErr: traffic.ErrInvalidLimits},
		{Name: "no series", Args: func(l *traffic.Limits) { l.MaxSeries = 0 }, WantErr: traffic.ErrInvalidLimits},
		{Name: "no operations per service", Args: func(l *traffic.Limits) { l.MaxPerService = 0 },
			WantErr: traffic.ErrInvalidLimits},
		{Name: "sketch budget below one sketch", Args: func(l *traffic.Limits) { l.SketchEntries = 1 },
			WantErr: traffic.ErrInvalidLimits},
		{Name: "more subjects than an engine holds, before any allocation",
			Args: func(l *traffic.Limits) { l.MaxNodes = math.MaxInt }, WantErr: traffic.ErrInvalidLimits},
		{Name: "more series than an engine holds", Args: func(l *traffic.Limits) { l.MaxSeries = stream.MaxCapacity + 1 },
			WantErr: traffic.ErrInvalidLimits},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			limits := testLimits()
			tt.Args(limits)
			if _, err := traffic.New(limits, nil); !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestACallIsCountedForItsEdgeAndItsOperation(t *testing.T) {
	h := newHarness(t, testLimits())
	route := httpOperation("GET /cart/{id}")
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.call(base.Add(time.Second), route, false)
		h.call(base.Add(2*time.Second), route, true)
	})
	h.flush()

	type final struct {
		Series                    traffic.SeriesKey
		Kind                      red.PaneKind
		Requests, Failed, Samples uint64
		Observed                  time.Duration
	}
	var got []final
	for _, emitted := range h.sink.finals {
		got = append(got, final{emitted.Series, emitted.Stats.Kind, emitted.Stats.Requests, emitted.Stats.Failed,
			emitted.Stats.LatencySamples, emitted.Stats.Observed})
	}
	subject := edgeSeries(traffic.SeriesSubject)
	want := []final{
		{Series: subject, Kind: red.PaneOnTime, Requests: 2, Failed: 1, Samples: 2, Observed: windowSize},
		{Series: operationSeries(&subject, route), Kind: red.PaneOnTime, Requests: 2, Failed: 1, Observed: windowSize},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("finals (-want +got):\n%s", diff)
	}
}

func TestAServedRequestIsTheNodesOwnSeries(t *testing.T) {
	h := newHarness(t, testLimits())
	route := httpOperation("GET /cart/{id}")
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.traffic.RecordServed(cartNode, servedBy(t, "cart", base.Add(time.Second), route))
	})
	h.flush()

	node := traffic.SeriesKey{Kind: traffic.SeriesSubject, Subject: traffic.SubjectNode, Callee: telemetrytest.Service("cart")}
	got := make([]traffic.SeriesKey, 0, len(h.sink.finals))
	for _, final := range h.sink.finals {
		got = append(got, final.Series)
	}
	if diff := cmp.Diff([]traffic.SeriesKey{node, operationSeries(&node, route)}, got); diff != "" {
		t.Errorf("series (-want +got):\n%s", diff)
	}
}

func TestSeriesKeysKeepSubjectsUnnamedOperationsAndProtocolsApart(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.call(base.Add(time.Second), operation.Operation{}, false)
		h.call(base.Add(time.Second), operation.Operation{Protocol: operation.ProtocolGRPC, Name: "cart.Cart/Get"}, false)
		h.call(base.Add(time.Second), operation.Operation{Protocol: operation.ProtocolRPC, Name: "cart.Cart/Get"}, false)
	})
	h.flush()

	seen := make(map[traffic.SeriesKey]int, len(h.sink.finals))
	for _, final := range h.sink.finals {
		seen[final.Series]++
	}
	if len(seen) != 4 {
		t.Errorf("%d distinct series keys %v, want the subject and three operations apart", len(seen), seen)
	}
}

func TestOperationsPastTheLimitGoToOverflowAndEveryEventIsOfferedOnce(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		for n := range uniquePaths {
			h.call(base.Add(time.Second), httpOperation(fmt.Sprintf("GET /item-%d", n)), false)
		}
	})
	overLimit := h.traffic.Stats().OwnersOverLimit
	h.flush()

	stats := h.traffic.Stats()
	type observed struct {
		OperationSeries, OverflowRequests int
		OverLimit                         []resource.ServiceRef
		OfferedSplit                      bool
	}
	want := observed{OperationSeries: perService, OverflowRequests: uniquePaths - perService,
		OverLimit: []resource.ServiceRef{telemetrytest.Service("cart")}, OfferedSplit: true}
	got := observed{
		OperationSeries:  len(h.finalsOf(traffic.SeriesOperation)),
		OverflowRequests: int(h.finalsOf(traffic.SeriesOverflow)[0].Stats.Requests),
		OverLimit:        overLimit,
		OfferedSplit:     stats.Operations.Offered+stats.Overflow.Offered == stats.Subjects.Offered,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("cardinality (-want +got):\n%s", diff)
	}
}

func TestALateEventWithAUniquePathTakesNoOperationReference(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Minute), base.Add(5*time.Minute), func() {
		h.call(base.Add(time.Second), httpOperation("GET /late"), false)
	})

	stats := h.traffic.Stats()
	type observed struct {
		Operations           int
		LateDroppedSubjects  uint64
		LateDroppedOperation uint64
	}
	want := observed{LateDroppedSubjects: 1, LateDroppedOperation: 1}
	got := observed{stats.Cardinality.Operations, stats.Subjects.LateDropped, stats.Operations.LateDropped}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("late event (-want +got):\n%s", diff)
	}
}

func TestAFullOperationEngineSpillsIntoOverflow(t *testing.T) {
	limits := testLimits()
	limits.MaxSeries = 1
	h := newHarness(t, limits)
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /a"), false)
		h.traffic.RecordServed(frontendNode, servedBy(t, "frontend", base.Add(time.Second), httpOperation("GET /")))
	})

	stats := h.traffic.Stats()
	if !stats.SeriesLimitReached || stats.Operations.Series != 1 || stats.Overflow.Series != 1 {
		t.Errorf("full engine: limit reached %v, operation series %d, overflow series %d; want true, 1, 1",
			stats.SeriesLimitReached, stats.Operations.Series, stats.Overflow.Series)
	}
}

func TestAFullFlushReleasesEverySeriesReferenceAndSketchBlock(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		for n := range uniquePaths {
			h.call(base.Add(time.Duration(n)*time.Second/2), httpOperation(fmt.Sprintf("GET /item-%d", n)), n%2 == 0)
			h.traffic.RecordServed(cartNode, servedBy(t, "cart", base.Add(time.Second), httpOperation("GET /")))
		}
	})
	h.flush()

	stats := h.traffic.Stats()
	type released struct {
		Operations, OwnersOverLimit, SubjectSeries, OperationSeries, OverflowSeries, SketchInUse int
	}
	got := released{stats.Cardinality.Operations, stats.Cardinality.OwnersOverLimit, stats.Subjects.Series,
		stats.Operations.Series, stats.Overflow.Series, stats.Sketch.InUse}
	if diff := cmp.Diff(released{}, got); diff != "" {
		t.Errorf("after a full flush (-want +got):\n%s", diff)
	}
}

func TestAnUntimedEventFallsInTheWindowOfTheBatchAnchor(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		untimed := frontendToCart(base, httpOperation("GET /"), false)
		untimed.Interval = eventtime.Interval{}
		h.traffic.RecordCall(frontendCart, cartNode, untimed)
	})
	h.flush()

	stats := h.traffic.Stats()
	subject := h.finalsOf(traffic.SeriesSubject)[0]
	type observed struct {
		WindowStart             time.Time
		Requests, Samples       uint64
		Untimed, LatencyUnknown uint64
	}
	want := observed{WindowStart: base, Requests: 1, Untimed: 1, LatencyUnknown: 1}
	got := observed{subject.Pane.Window.Start, subject.Stats.Requests, subject.Stats.LatencySamples, stats.Untimed,
		stats.LatencyUnknown}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("untimed event (-want +got):\n%s", diff)
	}
}

func TestEventsOfSubjectsTheGraphRefusedAreNotRecorded(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.traffic.RecordCall(handle.None, handle.None, frontendToCart(base.Add(time.Second), httpOperation("GET /"), false))
		h.traffic.RecordServed(handle.None, servedBy(t, "cart", base.Add(time.Second), httpOperation("GET /")))
	})

	if offered := h.traffic.Stats().Subjects.Offered; offered != 0 {
		t.Errorf("subjects offered %d events, want 0 for subjects the graph refused", offered)
	}
}

func TestHeartbeatsAndFinalsAreCounted(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		h.call(base.Add(time.Second), httpOperation("GET /"), false)
	})
	h.flush()

	stats := h.traffic.Stats()
	got := [2]uint64{stats.Heartbeats, stats.Finals}
	if diff := cmp.Diff([2]uint64{1, uint64(len(h.sink.finals))}, got); diff != "" {
		t.Errorf("[heartbeats, finals] (-want +got):\n%s", diff)
	}
}
