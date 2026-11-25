package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	secretOperation = "GET /secret-op-7f3a"
	balancedPairs   = 40
	latePairs       = 5
	fewOperations   = 2
	flushAhead      = time.Hour
	maxStatsLine    = 4 << 10
	manyOwners      = 20
	reportedAt      = time.Minute
	specLagSlack    = 30 * time.Second
	specRaiseRatio  = 0.01
	subjectsBase    = 100
	operationsBase  = 200
	overflowBase    = 300
	watermarkBehind = 45 * time.Second
)

func engineStats(base uint64) stream.Stats {
	return stream.Stats{
		Series:       int(base),
		Capacity:     int(base + 1),
		Offered:      base + 2,
		OnTime:       base + 3,
		LateAccepted: base + 4,
		LateDropped:  base + 5,
		NoWatermark:  base + 6,
		Reassigned:   base + 7,
		PanesEarly:   base + 8,
		PanesOnTime:  base + 9,
		PanesLate:    base + 10,
		Retired:      base + 11,
	}
}

func sampleTraffic() traffic.Stats {
	return traffic.Stats{
		Subjects:   engineStats(subjectsBase),
		Operations: engineStats(operationsBase),
		Overflow:   engineStats(overflowBase),
		Sketch: sketch.ArenaStats{
			Budget:            8_388_608,
			InUse:             4_096,
			CollapsedAdds:     41,
			CollapsedSketches: 42,
			DroppedSamples:    43,
		},
		Cardinality: cardinality.Stats{
			Operations:      51,
			OwnersOverLimit: 52,
		},
		OwnersOverLimit: []resource.ServiceRef{
			{
				Namespace: "shop",
				Name:      "cart",
			},
		},
		SeriesLimitReached: true,
		Untimed:            61,
		LatencyUnknown:     62,
		Heartbeats:         63,
		Finals:             64,
	}
}

func sampleSnapshot() *topology.Snapshot {
	return &topology.Snapshot{
		Revision: 7,
		Nodes: []topology.NodeView{
			{
				Node: graph.Node{
					Served: 70,
				},
			},
			{
				Node: graph.Node{
					Served: 71,
				},
			},
		},
		Edges: []topology.EdgeView{
			{
				Edge: graph.Edge{
					Calls: 80,
				},
			},
		},
		WindowSize:     10 * time.Second,
		Watermark:      telemetrytest.Epoch(),
		WatermarkKnown: true,
		TrafficStats:   sampleTraffic(),
		PairingStats: matcher.Stats{
			Observed:         21,
			Matched:          5,
			Duplicate:        1,
			Unpairable:       2,
			Evicted:          3,
			Expired:          4,
			Pending:          1,
			ExpiredCaller:    2,
			EvictedCaller:    1,
			TombstoneEvicted: 6,
		},
		GraphStats: graph.Stats{
			GraphOverflow:      8,
			UnansweredOverflow: 9,
			NodeOverflow:       10,
		},
	}
}

func partitionStatsLine(t *testing.T, snapshot *topology.Snapshot) (map[string]any, string) {
	t.Helper()
	graph, err := memgraph.New(snapshot)
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	logs := &logBuffer{}
	part := newPartitionUnit(&partitionConfig{}, nil, graph, nil, testLogger(logs))
	part.pipeline = &processing.Pipeline{}
	part.clock = fixedClock{
		now: telemetrytest.Epoch().Add(reportedAt),
	}
	hooks := []layer{
		{
			name:        partitionLabel(0),
			statsGroups: part.statsGroups,
		},
	}
	served := func() bool { return true }
	reporter := statsLayer(func() []layer { return hooks }, served, selfalert.Thresholds{}, testLogger(logs))
	reporter.unit.(*statsUnit).report(t.Context(), false)
	return logs.event(t, eventPipelineStats), logs.String()
}

func nameOperations(spans []span.Span) {
	for index := range spans {
		spans[index].Operation = operation.Operation{
			Protocol: operation.ProtocolHTTP,
			Name:     fmt.Sprintf("%s-%d", secretOperation, index),
		}
	}
}

func secretTraffic(t *testing.T) *topology.Snapshot {
	t.Helper()
	limits := topologytest.Limits()
	limits.MaxOperationsPerService = fewOperations
	builder, err := topology.NewBuilder(&limits, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	end := telemetrytest.Epoch().Add(time.Second)
	traffic := processingtest.TimedPairs(t, 1, balancedPairs, end)
	nameOperations(traffic.Spans)
	builder.Apply(traffic)
	closed := end.Add(flushAhead)
	builder.Apply(batch.Heartbeat(closed))
	late := processingtest.TimedPairs(t, balancedPairs+1, latePairs, end)
	nameOperations(late.Spans)
	arrived, err := batch.New(late.Spans, closed)
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	builder.Apply(arrived)
	return builder.Snapshot()
}

func counter(group map[string]any, engine, name string) float64 {
	return group[engine].(map[string]any)[name].(float64)
}

func engineBalanced(windows map[string]any, engine string) bool {
	arrived := counter(windows, engine, "on_time") + counter(windows, engine, "late_accepted") +
		counter(windows, engine, "late_dropped") + counter(windows, engine, "no_watermark")
	return counter(windows, engine, "offered") == arrived
}

func TestOneStatsLineBalancesI6I10AndI11(t *testing.T) {
	line, _ := partitionStatsLine(t, secretTraffic(t))
	windows, graph := line["windows"].(map[string]any), line["graph"].(map[string]any)
	subjectsOffered := counter(windows, "subjects", "offered")
	got := map[string]bool{
		"I6 subjects":   engineBalanced(windows, "subjects"),
		"I6 operations": engineBalanced(windows, "operations"),
		"I6 overflow":   engineBalanced(windows, "overflow"),
		"I10":           graph["calls_total"].(float64)+graph["served_total"].(float64) == subjectsOffered,
		"I11 offered": counter(windows, "operations", "offered")+counter(windows, "overflow", "offered") ==
			subjectsOffered,
		"I11 late": counter(windows, "operations", "late_dropped") == counter(windows, "subjects", "late_dropped"),
		"traffic":  subjectsOffered > 0,
		"late":     counter(windows, "subjects", "late_dropped") > 0,
		"overflow": counter(windows, "overflow", "offered") > 0,
		"on time":  counter(windows, "operations", "on_time") > 0,
	}
	want := map[string]bool{
		"I6 subjects": true, "I6 operations": true, "I6 overflow": true, "I10": true,
		"I11 offered": true, "I11 late": true, "traffic": true, "late": true, "overflow": true, "on time": true,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("formulas from one line (-want +got):\n%s\nline: %v", diff, line)
	}
}

func TestTheLogNeverNamesAnOperationAndStaysSmall(t *testing.T) {
	_, raw := partitionStatsLine(t, secretTraffic(t))
	if strings.Contains(raw, "secret-op") {
		t.Errorf("the log names an operation:\n%s", raw)
	}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if len(line) > maxStatsLine {
			t.Errorf("log line of %d bytes, want at most %d", len(line), maxStatsLine)
		}
	}
}

func crowd(count int) []resource.ServiceRef {
	owners := make([]resource.ServiceRef, 0, count)
	for index := range count {
		owners = append(owners, resource.ServiceRef{
			Namespace: "shop",
			Name:      strings.Repeat("s", index+1),
		})
	}
	return owners
}

func TestASampleReadsTheSubjectsEngine(t *testing.T) {
	snapshot := sampleSnapshot()
	snapshot.TrafficStats.OwnersOverLimit = crowd(manyOwners)
	now := telemetrytest.Epoch().Add(reportedAt)
	want := &selfalert.Sample{
		Now:                now,
		Watermark:          telemetrytest.Epoch(),
		WatermarkKnown:     true,
		Offered:            subjectsBase + 2,
		Reassigned:         subjectsBase + 7,
		LateDropped:        subjectsBase + 5,
		OwnersOverLimit:    52,
		Owners:             crowd(maxListedOwners),
		SeriesLimitReached: true,
		DroppedSamples:     43,
	}
	got := &selfalert.Sample{
		Now: now,
	}
	fillSample(snapshot, got)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("sample (-want +got):\n%s", diff)
	}
}

func TestAlertLinesCarryTheirStateValuesAndReason(t *testing.T) {
	logs := &logBuffer{}
	logTransitions(t.Context(), testLogger(logs), []selfalert.Transition{
		{
			Event:  selfalert.LateDropRatioHigh,
			Raised: true,
			Reading: selfalert.Reading{
				Value:     0.02,
				Threshold: specRaiseRatio,
				Volume:    3_000,
			},
		},
		{
			Event:      selfalert.LateDropRatioHigh,
			LowTraffic: true,
			Reading: selfalert.Reading{
				Threshold: specRaiseRatio,
				Volume:    12,
			},
		},
	})
	var got []map[string]any
	for _, record := range logs.all(t, selfalert.LateDropRatioHigh) {
		delete(record, "time")
		got = append(got, record)
	}
	want := []map[string]any{
		{
			"msg": selfalert.LateDropRatioHigh, "level": "WARN", "state": stateRaised,
			"value": 0.02, "threshold": specRaiseRatio, "volume": 3000.0,
		},
		{
			"msg": selfalert.LateDropRatioHigh, "level": "INFO", "state": stateCleared,
			"value": 0.0, "threshold": specRaiseRatio, "volume": 12.0, "reason": reasonLowTraffic,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("alert lines (-want +got):\n%s", diff)
	}
}

func TestTheLagThresholdAllowsTheDelayTheIdleGapAndOneFire(t *testing.T) {
	graph, err := loadGraph(environment(map[string]string{}))
	if err != nil {
		t.Fatalf("loadGraph: %v", err)
	}
	partition := partitionConfig{
		Limits:    graph.Topology,
		IdleAfter: graph.IdleAfter,
	}
	want := defaultWatermarkDelay + defaultIdleAdvanceAfter + defaultEarlyFirePeriod + specLagSlack
	if got := processorThresholds(&partition).WatermarkLag; got != want {
		t.Errorf("watermark lag threshold = %s, want %s", got, want)
	}
}

func TestAnUnknownWatermarkLeavesTheLagOut(t *testing.T) {
	snapshot := sampleSnapshot()
	snapshot.WatermarkKnown = false
	line, _ := partitionStatsLine(t, snapshot)
	windows := line["windows"].(map[string]any)
	for _, key := range []string{"watermark", "watermark_lag_seconds"} {
		if value, found := windows[key]; found {
			t.Errorf("windows.%s = %v while the watermark is unknown, want it left out", key, value)
		}
	}
}
