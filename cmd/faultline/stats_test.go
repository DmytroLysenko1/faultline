package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	slowFinalWrite  = time.Second
	subjectsBase    = 100
	operationsBase  = 200
	overflowBase    = 300
	watermarkBehind = 45 * time.Second
)

var errSnapshotUnavailable = errors.New("snapshot unavailable")

type finalStats struct {
	Observed float64
	Matched  float64
	Accepted float64
	Batches  float64
	Balanced bool
}

func finalStatsOf(t *testing.T, logs *logBuffer) finalStats {
	t.Helper()
	var finals []map[string]any
	for _, record := range logs.all(t, "pipeline.stats") {
		if record["final"] == true {
			finals = append(finals, record)
		}
	}
	if len(finals) != 1 {
		t.Fatalf("final pipeline.stats records = %d, want 1", len(finals))
	}
	pairing := finals[0]["pairing"].(map[string]any)
	return finalStats{
		Observed: pairing["observed"].(float64),
		Matched:  pairing["matched"].(float64),
		Accepted: finals[0]["ingest"].(map[string]any)["accepted_spans"].(float64),
		Batches:  finals[0]["journal"].(map[string]any)["batches"].(float64),
		Balanced: balanced(pairing),
	}
}

func balanced(pairing map[string]any) bool {
	accounted := matcher.SpansPerCall*pairing["matched"].(float64) + pairing["duplicate"].(float64) +
		pairing["unpairable"].(float64) + pairing["evicted"].(float64) + pairing["expired"].(float64) +
		pairing["pending"].(float64)
	return pairing["observed"].(float64) == accounted
}

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

func engineRecord(base float64) map[string]any {
	return map[string]any{
		"series":                base,
		"capacity":              base + 1,
		"offered":               base + 2,
		"on_time":               base + 3,
		"late_accepted":         base + 4,
		"late_dropped":          base + 5,
		"no_watermark":          base + 6,
		"clock_skew_reassigned": base + 7,
		"panes_early":           base + 8,
		"panes_on_time":         base + 9,
		"panes_late":            base + 10,
		"retired":               base + 11,
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
			{Node: graph.Node{Served: 70}},
			{Node: graph.Node{Served: 71}},
		},
		Edges: []topology.EdgeView{
			{Edge: graph.Edge{Calls: 80}},
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

func sampleReporter(logs *logBuffer, snapshots snapshotSource) *statsReporter {
	return &statsReporter{
		snapshots: snapshots,
		ingest: fixedCounters{
			stats: ingest.Stats{
				AcceptedSpans:       30,
				RejectedSpans:       4,
				OverloadedResponses: 2,
				TooLargeResponses:   1,
			},
		},
		journal: fixedJournal{
			stats: memjournal.Stats{
				Bytes:   4096,
				Batches: 3,
				Spans:   9,
			},
		},
		timings: fixedTimings{
			timings: sampleTimings(),
		},
		alerts: testAlerts(),
		clock: fixedClock{
			now: telemetrytest.Epoch().Add(watermarkBehind),
		},
		logger:   testLogger(logs),
		interval: statsInterval,
	}
}

func sampleTimings() processing.Timings {
	return processing.Timings{
		Iteration: 9 * time.Millisecond,
		Apply:     2500 * time.Microsecond,
		Fire:      700 * time.Microsecond,
		Publish:   3 * time.Millisecond,
	}
}

func runReporter(t *testing.T, reporter *statsReporter) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- reporter.Run(ctx)
	}()
	return cancel, done
}

func finalFlags(t *testing.T, logs *logBuffer) []bool {
	t.Helper()
	var flags []bool
	for _, record := range logs.all(t, "pipeline.stats") {
		flags = append(flags, record["final"].(bool))
	}
	return flags
}

func TestStatsRecordCarriesEveryCounter(t *testing.T) {
	logs := &logBuffer{}
	sampleReporter(logs, fixedSnapshots{snapshot: sampleSnapshot()}).report(t.Context(), false)
	records := logs.all(t, "pipeline.stats")
	if len(records) != 1 {
		t.Fatalf("pipeline.stats records = %d, want 1", len(records))
	}
	got := records[0]
	for _, envelope := range []string{"time", "level", "msg"} {
		delete(got, envelope)
	}
	want := map[string]any{
		"final":    false,
		"revision": 7.0,
		"nodes":    2.0,
		"edges":    1.0,
		"pairing": map[string]any{
			"observed": 21.0, "matched": 5.0, "duplicate": 1.0, "unpairable": 2.0, "evicted": 3.0,
			"expired": 4.0, "pending": 1.0, "expired_caller": 2.0, "evicted_caller": 1.0, "tombstone_evicted": 6.0,
		},
		"graph": map[string]any{
			"graph_overflow": 8.0, "unanswered_overflow": 9.0, "node_overflow": 10.0,
			"calls_total": 80.0, "served_total": 141.0,
		},
		"windows": map[string]any{
			"window_size_seconds": 10.0, "watermark_known": true,
			"watermark": telemetrytest.Epoch().UTC().Format(time.RFC3339Nano), "watermark_lag_seconds": 45.0,
			"heartbeats": 63.0, "finals": 64.0, "untimed": 61.0, "latency_unknown": 62.0,
			"subjects": engineRecord(subjectsBase), "operations": engineRecord(operationsBase),
			"overflow": engineRecord(overflowBase),
		},
		"cardinality": map[string]any{
			"operations": 51.0, "operation_series": float64(operationsBase), "overflow_series": float64(overflowBase),
			"services_over_limit": 52.0, "series_limit_reached": true,
		},
		"sketch": map[string]any{
			"entries_in_use": 4096.0, "budget": 8388608.0, "collapsed_adds": 41.0, "collapsed_sketches": 42.0,
			"dropped_samples": 43.0,
		},
		"pipeline": map[string]any{
			"iteration_max_us": 9000.0, "apply_max_us": 2500.0, "fire_max_us": 700.0, "publish_max_us": 3000.0,
		},
		"alerts": map[string]any{
			"windows.clock_skew_ratio_high": false, "windows.late_drop_ratio_high": false,
			"windows.watermark_lag_high": false, "windows.cardinality_limit_reached": true,
			"windows.series_limit_reached": true, "windows.sketch_budget_exhausted": true,
		},
		"ingest": map[string]any{
			"accepted_spans": 30.0, "rejected_spans": 4.0, "overloaded_responses": 2.0, "too_large_responses": 1.0,
		},
		"journal": map[string]any{
			"bytes": 4096.0, "batches": 3.0, "spans": 9.0,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("pipeline.stats (-want +got):\n%s", diff)
	}
}

func TestStatsReporterWritesWhileIdleAndOnceMoreOnStop(t *testing.T) {
	tests := []struct {
		Name string
		Args time.Duration
		Want []bool
	}{
		{
			Name: "nothing before the first interval",
			Args: statsInterval - time.Nanosecond,
			Want: []bool{true},
		},
		{
			Name: "one record per interval without traffic",
			Args: 2 * statsInterval,
			Want: []bool{false, false, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &logBuffer{}
				cancel, done := runReporter(t, sampleReporter(logs, fixedSnapshots{snapshot: sampleSnapshot()}))
				time.Sleep(tt.Args)
				synctest.Wait()
				cancel()
				if err := <-done; err != nil {
					t.Fatalf("Run = %v, want nil", err)
				}
				if diff := cmp.Diff(tt.Want, finalFlags(t, logs)); diff != "" {
					t.Errorf("final flags of pipeline.stats (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestStatsFailureIsWarnedAndTheNextTickStillReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		snapshots := &flakySnapshots{
			failures: 1,
			snapshot: sampleSnapshot(),
		}
		cancel, done := runReporter(t, sampleReporter(logs, snapshots))
		time.Sleep(2 * statsInterval)
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		got := reporterOutcome{
			Warnings: len(logs.all(t, "pipeline.stats_failed")),
			Flags:    finalFlags(t, logs),
		}
		want := reporterOutcome{
			Warnings: 1,
			Flags:    []bool{false, true},
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("reporter (-want +got):\n%s", diff)
		}
	})
}

func TestStopOnATickIsNotAFailure(t *testing.T) {
	logs := &logBuffer{}
	store, err := memgraph.New(sampleSnapshot())
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	sampleReporter(logs, store).report(stopped, false)
	got := reporterOutcome{
		Warnings: len(logs.all(t, "pipeline.stats_failed")),
		Flags:    finalFlags(t, logs),
	}
	want := reporterOutcome{
		Flags: []bool{false},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("tick read with a cancelled context (-want +got):\n%s", diff)
	}
}

type reporterOutcome struct {
	Warnings int
	Flags    []bool
}

type flakySnapshots struct {
	failures int
	snapshot *topology.Snapshot
}

func (f *flakySnapshots) Latest(context.Context) (*topology.Snapshot, error) {
	if f.failures > 0 {
		f.failures--
		return nil, errSnapshotUnavailable
	}
	return f.snapshot, nil
}

func TestSlowComponentsAtTheEndOfShutdown(t *testing.T) {
	tests := []struct {
		Name    string
		Args    func(h *harness)
		Want    []string
		WantErr error
	}{
		{
			Name: "tracing flush past its budget is forced and keeps the exit code",
			Args: func(h *harness) {
				h.tracing.hang = true
			},
			Want: []string{componentTracing},
		},
		{
			Name: "reporter that does not stop is reported",
			Args: func(h *harness) {
				h.sys.stats = runnerFunc(func(context.Context) error {
					<-h.release
					return nil
				})
			},
			Want:    []string{componentStats},
			WantErr: errNotStopped,
		},
		{
			Name: "reporter finishing its final write during the http stage is not forced",
			Args: func(h *harness) {
				h.api.hangShutdown = true
				h.sys.stats = runnerFunc(func(ctx context.Context) error {
					<-ctx.Done()
					time.Sleep(slowFinalWrite)
					return nil
				})
			},
			Want: []string{componentAPI},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, drainingPipeline, 0)
				tt.Args(h)
				_, err := runHarness(t, h, signalled)
				close(h.release)
				synctest.Wait()
				if !errors.Is(err, tt.WantErr) {
					t.Fatalf("run = %v, want %v", err, tt.WantErr)
				}
				var forced []string
				for _, record := range h.logs.all(t, "lifecycle.forced_stop") {
					forced = append(forced, record["component"].(string))
				}
				if diff := cmp.Diff(tt.Want, forced); diff != "" {
					t.Errorf("forced stops (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestShutdownBudgetFitsTenSeconds(t *testing.T) {
	budget := defaultShutdownBudget()
	worst := budget.OTLPGrace + budget.ExitGrace + budget.Drain + budget.CancelGrace + budget.HTTPGrace +
		budget.ExitGrace + budget.TraceFlush
	if want := 9900 * time.Millisecond; worst != want {
		t.Errorf("worst-case shutdown = %v, want %v (10 s minus room for Stop, Close and logs)", worst, want)
	}
}
