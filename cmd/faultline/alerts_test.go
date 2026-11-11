package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
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
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
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
)

type heldTimings struct {
	held processing.Timings
}

func (f *heldTimings) TakeTimings() processing.Timings {
	taken := f.held
	f.held = processing.Timings{}
	return taken
}

type alertLine struct {
	Msg      string
	Level    string
	State    string
	Services []any
}

func alertLines(t *testing.T, logs *logBuffer) []alertLine {
	t.Helper()
	var lines []alertLine
	for _, record := range logs.records(t) {
		state, isAlert := record["state"].(string)
		if !isAlert {
			continue
		}
		services, _ := record["services"].([]any)
		lines = append(lines, alertLine{
			Msg:      record["msg"].(string),
			Level:    record["level"].(string),
			State:    state,
			Services: services,
		})
	}
	return lines
}

func TestAReportWarnsOnceForEveryRaisedAlert(t *testing.T) {
	logs := &logBuffer{}
	reporter := sampleReporter(logs, fixedSnapshots{snapshot: sampleSnapshot()})
	reporter.report(t.Context(), false)
	reporter.report(t.Context(), false)
	want := []alertLine{
		{
			Msg:      "windows.cardinality_limit_reached",
			Level:    "WARN",
			State:    stateRaised,
			Services: []any{"shop/cart"},
		},
		{
			Msg:   "windows.series_limit_reached",
			Level: "WARN",
			State: stateRaised,
		},
		{
			Msg:   "windows.sketch_budget_exhausted",
			Level: "WARN",
			State: stateRaised,
		},
	}
	if diff := cmp.Diff(want, alertLines(t, logs)); diff != "" {
		t.Errorf("alert lines over two reports of one snapshot (-want +got):\n%s", diff)
	}
}

// TestTheFinalReportDoesNotJudgeAShortInterval: the stop comes seconds after the last report, so
// its deltas say nothing; it logs the state without raising or clearing.
func TestTheFinalReportDoesNotJudgeAShortInterval(t *testing.T) {
	logs := &logBuffer{}
	sampleReporter(logs, fixedSnapshots{snapshot: sampleSnapshot()}).report(t.Context(), true)
	if lines := alertLines(t, logs); len(lines) != 0 {
		t.Errorf("final report logged alert lines %+v, want none", lines)
	}
	alerts := logs.all(t, "pipeline.stats")[0]["alerts"].(map[string]any)
	for event, raised := range alerts {
		if raised != false {
			t.Errorf("alert %s = %v after a final report alone, want false", event, raised)
		}
	}
}

func TestAFailedSnapshotKeepsTheTimingsForTheNextReport(t *testing.T) {
	logs := &logBuffer{}
	reporter := sampleReporter(logs, fixedSnapshots{err: errSnapshotUnavailable})
	reporter.timings = &heldTimings{
		held: sampleTimings(),
	}
	reporter.report(t.Context(), false)
	reporter.snapshots = fixedSnapshots{snapshot: sampleSnapshot()}
	reporter.report(t.Context(), false)
	got := logs.all(t, "pipeline.stats")[0]["pipeline"]
	want := map[string]any{
		"iteration_max_us": 9000.0, "apply_max_us": 2500.0, "fire_max_us": 700.0, "publish_max_us": 3000.0,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("timings in the report after a failed one (-want +got):\n%s", diff)
	}
}

func calmSnapshot() *topology.Snapshot {
	snapshot := sampleSnapshot()
	snapshot.TrafficStats.OwnersOverLimit = nil
	snapshot.TrafficStats.Cardinality.OwnersOverLimit = 0
	snapshot.TrafficStats.SeriesLimitReached = false
	return snapshot
}

// TestAlertsRaiseThenClearThroughTheReporter: a hot snapshot, then two calm ones; the log tells the
// whole story, Warn when raised, Info when cleared, and the state group follows.
func TestAlertsRaiseThenClearThroughTheReporter(t *testing.T) {
	logs := &logBuffer{}
	reporter := sampleReporter(logs, fixedSnapshots{snapshot: sampleSnapshot()})
	reporter.report(t.Context(), false)
	reporter.snapshots = fixedSnapshots{snapshot: calmSnapshot()}
	reporter.report(t.Context(), false)
	reporter.report(t.Context(), false)
	var got []alertLine
	for _, line := range alertLines(t, logs) {
		line.Services = nil
		got = append(got, line)
	}
	var want []alertLine
	for _, level := range []string{"WARN", "INFO"} {
		state := map[string]string{"WARN": stateRaised, "INFO": stateCleared}[level]
		for _, event := range []string{
			"windows.cardinality_limit_reached", "windows.series_limit_reached", "windows.sketch_budget_exhausted",
		} {
			want = append(want, alertLine{Msg: event, Level: level, State: state})
		}
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("alert lines (-want +got):\n%s", diff)
	}
	stats := logs.all(t, "pipeline.stats")
	for event, raised := range stats[len(stats)-1]["alerts"].(map[string]any) {
		if raised != false {
			t.Errorf("alert %s = %v after two calm reports, want false", event, raised)
		}
	}
}

func TestASecondRunOfTheReporterIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reporter := sampleReporter(&logBuffer{}, fixedSnapshots{snapshot: sampleSnapshot()})
		ctx, cancel := context.WithCancel(t.Context())
		var first sync.WaitGroup
		running := make(chan error, 1)
		first.Go(func() {
			running <- reporter.Run(ctx)
		})
		synctest.Wait()
		second := reporter.Run(ctx)
		cancel()
		first.Wait()
		if !errors.Is(second, errReporterStarted) {
			t.Errorf("second Run = %v, want %v", second, errReporterStarted)
		}
		if err := <-running; err != nil {
			t.Errorf("first Run = %v, want nil", err)
		}
	})
}

func nameOperations(spans []span.Span) {
	for index := range spans {
		spans[index].Operation = operation.Operation{
			Protocol: operation.ProtocolHTTP,
			Name:     fmt.Sprintf("%s-%d", secretOperation, index),
		}
	}
}

// secretTraffic sends pairs with more distinct operations than a service may keep, closes their
// windows, then sends pairs from the closed past: every term of I6, I10 and I11 is non-zero.
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

// TestOneStatsLineBalancesI6I10AndI11: the demo checks the formulas from a single line, so the line
// alone must carry every term of them, read from one snapshot.
func TestOneStatsLineBalancesI6I10AndI11(t *testing.T) {
	logs := &logBuffer{}
	sampleReporter(logs, fixedSnapshots{snapshot: secretTraffic(t)}).report(t.Context(), false)
	line := logs.all(t, "pipeline.stats")[0]
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
	logs := &logBuffer{}
	sampleReporter(logs, fixedSnapshots{snapshot: secretTraffic(t)}).report(t.Context(), false)
	raw := logs.String()
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

// TestASampleReadsTheSubjectsEngine: the alert rules judge events of subjects, the engine that sees
// every call and served request once; the other two engines split the same events.
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
	if diff := cmp.Diff(want, sampleOf(snapshot, now)); diff != "" {
		t.Errorf("sample (-want +got):\n%s", diff)
	}
}

func TestAlertLinesCarryTheirStateValuesAndReason(t *testing.T) {
	logs := &logBuffer{}
	reporter := sampleReporter(logs, fixedSnapshots{snapshot: sampleSnapshot()})
	reporter.logTransitions(t.Context(), []selfalert.Transition{
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

// TestTheLagThresholdAllowsTheDelayTheIdleGapAndOneFire: with no traffic the pipeline sees the
// heartbeat only when Receive returns, up to one Fire period after it was due.
func TestTheLagThresholdAllowsTheDelayTheIdleGapAndOneFire(t *testing.T) {
	cfg := new(defaultConfig())
	want := defaultWatermarkDelay + defaultIdleAdvanceAfter + defaultEarlyFirePeriod + specLagSlack
	if got := alertThresholds(cfg).WatermarkLag; got != want {
		t.Errorf("watermark lag threshold = %s, want %s", got, want)
	}
}

func TestAnUnknownWatermarkLeavesTheLagOut(t *testing.T) {
	logs := &logBuffer{}
	snapshot := sampleSnapshot()
	snapshot.WatermarkKnown = false
	sampleReporter(logs, fixedSnapshots{snapshot: snapshot}).report(t.Context(), false)
	windows := logs.all(t, "pipeline.stats")[0]["windows"].(map[string]any)
	for _, key := range []string{"watermark", "watermark_lag_seconds"} {
		if value, found := windows[key]; found {
			t.Errorf("windows.%s = %v while the watermark is unknown, want it left out", key, value)
		}
	}
}
