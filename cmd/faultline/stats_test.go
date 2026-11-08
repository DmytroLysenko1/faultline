package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const slowFinalWrite = time.Second

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

func sampleSnapshot() *topology.Snapshot {
	return &topology.Snapshot{
		Revision: 7,
		Nodes:    make([]graph.Node, 2),
		Edges:    make([]graph.Edge, 1),
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
		logger:   testLogger(logs),
		interval: statsInterval,
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
