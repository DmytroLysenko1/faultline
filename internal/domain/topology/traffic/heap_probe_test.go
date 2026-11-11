//go:build !race

package traffic_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

const (
	probeNodes   = 1_000
	probeEdges   = 5_000
	probeSeries  = 10_000
	probeSketch  = 1 << 16
	probeLimitOf = 500
)

func probeLimits() *traffic.Limits {
	limits := testLimits()
	limits.MaxNodes, limits.MaxEdges, limits.MaxSeries = probeNodes, probeEdges, probeSeries
	limits.MaxPerService, limits.SketchEntries = probeLimitOf, probeSketch
	return limits
}

func longestName(n int) string {
	prefix := fmt.Sprintf("GET /%08d/", n)
	return prefix + strings.Repeat("x", operation.MaxNameBytes-len(prefix))
}

// fullTraffic keeps one View copy on top of the model, as a snapshot would. It fills every
// subject, every operation series with the longest names and the overflow engine, and fires
// early so every subject has a live summary.
func fullTraffic(t *testing.T, limits *traffic.Limits) any {
	t.Helper()
	h := newHarness(t, limits)
	at := base.Add(time.Second)
	h.begin(base.Add(5*time.Second), base.Add(5*time.Second))
	for edge := range limits.MaxEdges {
		observed := frontendToCart(at, httpOperation(longestName(edge)), false)
		observed.Callee = telemetrytest.Service(fmt.Sprintf("callee-%d", edge%limits.MaxNodes))
		h.traffic.RecordCall(handle.Edge(edge), handle.Node(edge%limits.MaxNodes), observed)
	}
	for n := range limits.MaxSeries + limits.MaxNodes {
		node := n % limits.MaxNodes
		h.traffic.RecordServed(handle.Node(node), servedBy(t, fmt.Sprintf("callee-%d", node), at,
			httpOperation(longestName(limits.MaxEdges+n))))
	}
	h.traffic.End()
	h.traffic.Fire()
	view := h.traffic.View()
	return [2]any{h, view}
}

func TestWorstCaseHeapCoversEverySubjectSeriesAndOperationWithTheLongestNames(t *testing.T) {
	limits := probeLimits()
	grown := telemetrytest.HeapGrowth(func() any {
		return fullTraffic(t, limits)
	})
	model := uint64(limits.WorstCaseHeapBytes() + limits.ViewHeapBytes())
	if grown > model {
		t.Errorf("traffic took %d bytes with every subject and series full, model %d", grown, model)
	}
}
