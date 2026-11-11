package traffic_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

const (
	paymentNode  = handle.Node(2)
	frontendPays = handle.Edge(1)
	wiringPrefix = "traffic: "
)

func TestAnOwnerAdmitsNewOperationsAgainOnceItsOldOnesAreForgotten(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		for n := range uniquePaths {
			h.call(base.Add(time.Second), httpOperation(fmt.Sprintf("GET /old-%d", n)), false)
		}
	})
	h.flush()
	afterFlush := h.traffic.Stats().OwnersOverLimit
	later := base.Add(flushAhead + time.Minute)
	h.batch(later, later, func() {
		h.call(later.Add(-time.Second), httpOperation("GET /new"), false)
	})

	stats := h.traffic.Stats()
	got := [3]int{len(afterFlush), stats.Operations.Series, stats.Overflow.Series}
	if diff := cmp.Diff([3]int{0, 1, 0}, got); diff != "" {
		t.Errorf("[owners over limit after flush, operation series, overflow series] (-want +got):\n%s", diff)
	}
}

func TestAQuietServiceNextToANoisyOneKeepsItsOperations(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(5*time.Second), base.Add(5*time.Second), func() {
		for n := range uniquePaths {
			h.call(base.Add(time.Second), httpOperation(fmt.Sprintf("GET /noise-%d", n)), false)
		}
		quiet := frontendToCart(base.Add(time.Second), httpOperation("POST /pay"), false)
		quiet.Callee = telemetrytest.Service("payment")
		h.traffic.RecordCall(frontendPays, paymentNode, quiet)
	})
	h.flush()

	overflowCallees := make([]resource.ServiceRef, 0, 1)
	for _, final := range h.finalsOf(traffic.SeriesOverflow) {
		overflowCallees = append(overflowCallees, final.Series.Callee)
	}
	if diff := cmp.Diff([]resource.ServiceRef{telemetrytest.Service("cart")}, overflowCallees); diff != "" {
		t.Errorf("callees with an overflow series (-want +got):\n%s", diff)
	}
}

func TestAnUntimedSpanOfAnOldCatchUpBatchIsNotLostAsLate(t *testing.T) {
	h := newHarness(t, testLimits())
	h.batch(base.Add(200*time.Second), base.Add(200*time.Second), func() {})
	h.batch(base.Add(201*time.Second), base.Add(100*time.Second), func() {
		untimed := frontendToCart(base, httpOperation("GET /"), false)
		untimed.Interval = eventtime.Interval{}
		h.traffic.RecordCall(frontendCart, cartNode, untimed)
	})

	stats := h.traffic.Stats()
	got := [2]uint64{stats.Subjects.LateDropped, stats.Subjects.OnTime + stats.Subjects.LateAccepted}
	if diff := cmp.Diff([2]uint64{0, 1}, got); diff != "" {
		t.Errorf("[late dropped, accepted] (-want +got):\n%s", diff)
	}
}

func TestAHandlePastTheLimitsIsAWiringPanic(t *testing.T) {
	h := newHarness(t, testLimits())
	h.begin(base.Add(5*time.Second), base.Add(5*time.Second))
	defer func() {
		message, wiring := recover().(string)
		if !wiring || !strings.HasPrefix(message, wiringPrefix) {
			t.Errorf("panic = %q, want a wiring panic starting with %q", message, wiringPrefix)
		}
	}()
	h.traffic.RecordCall(handle.Edge(testEdges), cartNode, frontendToCart(base.Add(time.Second), httpOperation("GET /"), false))
}

type countingSink struct {
	finals int
}

func (s *countingSink) Final(*traffic.Final) {
	s.finals++
}

func mallocs() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Mallocs
}

func TestClosingWindowsDoesNotAllocateAFinalPerPane(t *testing.T) {
	sink := &countingSink{}
	built, err := traffic.New(testLimits(), sink)
	if err != nil {
		t.Fatalf("traffic.New: %v", err)
	}
	h := &harness{t: t, traffic: built}
	h.begin(base.Add(5*time.Second), base.Add(5*time.Second))
	for edge := range testEdges {
		built.RecordCall(handle.Edge(edge), cartNode, frontendToCart(base.Add(time.Second), httpOperation("GET /"), false))
	}
	built.End()
	heartbeat := batch.Heartbeat(base.Add(flushAhead))

	before := mallocs()
	built.Begin(heartbeat)
	built.End()
	allocated := mallocs() - before

	if sink.finals < 2*testEdges || allocated >= uint64(sink.finals) {
		t.Errorf("closing windows made %d allocations for %d finals, want fewer than one per final", allocated, sink.finals)
	}
}
