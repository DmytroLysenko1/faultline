package replay_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	finalsGoldenPath = "test/demo/otel-demo/replay-finals.golden.json"
	flushAhead       = time.Hour
	// maxEntriesPerSeries is B4: real traffic needs units to tens of sketch entries per subject
	// series across its live windows; the arena budget assumes it.
	maxEntriesPerSeries = 16
	fireEveryFew        = 7
	oneOperationEach    = 1
)

var (
	seriesKindNames = map[traffic.SeriesKind]string{
		traffic.SeriesSubject:   "subject",
		traffic.SeriesOperation: "operation",
		traffic.SeriesOverflow:  "overflow",
	}
	subjectKindNames = map[traffic.SubjectKind]string{
		traffic.SubjectNode: "node",
		traffic.SubjectEdge: "edge",
	}
	protocolNames = map[operation.Protocol]string{
		operation.ProtocolUnspecified: "",
		operation.ProtocolHTTP:        "http",
		operation.ProtocolGRPC:        "grpc",
		operation.ProtocolRPC:         "rpc",
	}
	paneKindNames = map[stream.PaneKind]string{
		stream.PaneEarly:  "early",
		stream.PaneOnTime: "on_time",
		stream.PaneLate:   "late",
	}
)

type goldenFinal struct {
	Series         string    `json:"series"`
	WindowStart    time.Time `json:"window_start"`
	Pane           string    `json:"pane"`
	Revision       uint32    `json:"revision"`
	Requests       uint64    `json:"requests"`
	Failed         uint64    `json:"failed"`
	LatencySamples uint64    `json:"latency_samples"`
	P50            string    `json:"p50"`
	P95            string    `json:"p95"`
	P99            string    `json:"p99"`
	Observed       string    `json:"observed"`
}

type finalsCollector struct {
	t      *testing.T
	got    []goldenFinal
	finals []traffic.Final
}

func nameOf[K comparable](t *testing.T, names map[K]string, key K) string {
	t.Helper()
	name, known := names[key]
	if !known {
		t.Fatalf("no golden name for %T %v", key, key)
	}
	return name
}

func (c *finalsCollector) Final(final *traffic.Final) {
	series, t := &final.Series, c.t
	c.finals = append(c.finals, *final)
	c.got = append(c.got, goldenFinal{
		Series: fmt.Sprintf("%s %s %s/%s → %s/%s %s %q", nameOf(t, seriesKindNames, series.Kind),
			nameOf(t, subjectKindNames, series.Subject), series.Caller.Namespace, series.Caller.Name,
			series.Callee.Namespace, series.Callee.Name, nameOf(t, protocolNames, series.Protocol), series.Operation),
		WindowStart:    final.Pane.Window.Start.UTC(),
		Pane:           nameOf(t, paneKindNames, final.Pane.Kind),
		Revision:       final.Stats.Revision,
		Requests:       final.Stats.Requests,
		Failed:         final.Stats.Failed,
		LatencySamples: final.Stats.LatencySamples,
		P50:            final.Stats.P50.String(),
		P95:            final.Stats.P95.String(),
		P99:            final.Stats.P99.String(),
		Observed:       final.Stats.Observed.String(),
	})
}

// replayFinals replays the dump, then a heartbeat far ahead closes every window.
func replayFinals(t *testing.T, limits *topology.Limits, afterApply func(*topology.Builder)) (*finalsCollector,
	*topology.Snapshot,
) {
	t.Helper()
	collector := &finalsCollector{t: t}
	builder, last := replayInto(t, limits, otlptest.DemoLines(t), afterApply, topology.WithFinalSink(collector))
	builder.Apply(batch.Heartbeat(last.Add(flushAhead)))
	return collector, builder.Snapshot()
}

func readFinalsGolden(t *testing.T, got []goldenFinal) []goldenFinal {
	t.Helper()
	path := filepath.Join(otlptest.ModuleRoot(t), finalsGoldenPath)
	if os.Getenv(updateGoldenEnv) != "" {
		t.Logf("regenerating %s", path)
		encoded, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatalf("encode finals: %v", err)
		}
		if err := os.WriteFile(path, append(encoded, '\n'), goldenFileMode); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (regenerate with %s=1): %v", path, updateGoldenEnv, err)
	}
	var decoded []goldenFinal
	if err := json.Unmarshal(stored, &decoded); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return decoded
}

func noFire(*topology.Builder) {}

func fireEvery(records int) func(*topology.Builder) {
	applied := 0
	return func(builder *topology.Builder) {
		applied++
		if applied%records == 0 {
			builder.Fire()
		}
	}
}

// TestReplayFinalsMatchTheGolden is checkpoint 1: the final panes of the demo dump are a function
// of the journal (I8). Fire shows early panes, so every cadence must still give the same finals.
func TestReplayFinalsMatchTheGolden(t *testing.T) {
	quiet, _ := replayFinals(t, defaultLimits(), noFire)
	want := readFinalsGolden(t, quiet.got)
	if len(want) == 0 {
		t.Fatal("the demo dump closed no window")
	}
	tests := []struct {
		Name      string
		Args      func(*topology.Builder)
		WantEarly bool
	}{
		{Name: "without Fire", Args: noFire},
		{Name: "with Fire after every record", Args: fireEvery(1), WantEarly: true},
		{Name: "with Fire after every few records", Args: fireEvery(fireEveryFew), WantEarly: true},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			collector, snapshot := replayFinals(t, defaultLimits(), tt.Args)
			if diff := cmp.Diff(want, collector.got); diff != "" {
				t.Errorf("finals (-golden +got); regenerate with %s=1 only after reviewing the diff:\n%s",
					updateGoldenEnv, diff)
			}
			if early := snapshot.TrafficStats.Subjects.PanesEarly > 0; early != tt.WantEarly {
				t.Errorf("early panes shown = %v, want %v: Fire must have run for the case to prove I8", early,
					tt.WantEarly)
			}
		})
	}
}

// TestReplayFinalsHoldTheSummaryInvariants checks every final against the value-object rules, so
// a swapped percentile or a failure count above the requests fails beyond the rows that differ.
func TestReplayFinalsHoldTheSummaryInvariants(t *testing.T) {
	collector, _ := replayFinals(t, defaultLimits(), noFire)
	for i := range collector.finals {
		stats := &collector.finals[i].Stats
		ordered := stats.P50 <= stats.P95 && stats.P95 <= stats.P99
		if !ordered || stats.Failed > stats.Requests || stats.LatencySamples > stats.Requests {
			t.Errorf("final %d breaks the summary rules: %+v", i, *stats)
		}
	}
}

type finalKey struct {
	Series traffic.SeriesKey
	Start  int64
}

// acceptedByFinals sums the requests of the last final of every (series, window) per engine:
// subjects on one side, operations and overflow on the other.
func acceptedByFinals(finals []traffic.Final) (subjects, operations uint64) {
	last := make(map[finalKey]uint64, len(finals))
	for i := range finals {
		last[finalKey{Series: finals[i].Series, Start: finals[i].Stats.StartUnixNano}] = finals[i].Stats.Requests
	}
	for key, requests := range last {
		if key.Series.Kind == traffic.SeriesSubject {
			subjects += requests
			continue
		}
		operations += requests
	}
	return subjects, operations
}

type accounting struct {
	SubjectsOffered, OperationsOffered, Accepted   uint64
	SubjectFinals, OperationFinals                 uint64
	SubjectSeries, OperationSeries, OverflowSeries int
	SketchInUse, Operations                        int
}

// TestReplayFinalsAccountForEveryEvent: after the flush every event the graph counted went to one
// subject engine and one operation engine (I10, I11), the finals add up to the accepted events,
// and nothing is held (I12). Tight limits push operations into overflow series.
func TestReplayFinalsAccountForEveryEvent(t *testing.T) {
	tight := defaultLimits()
	tight.MaxOperationsPerService = oneOperationEach
	tests := []struct {
		Name         string
		Args         *topology.Limits
		WantOverflow bool
	}{
		{Name: "default limits", Args: defaultLimits()},
		{Name: "one operation per service", Args: tight, WantOverflow: true},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			collector, snapshot := replayFinals(t, tt.Args, noFire)
			graphEvents := servedAndCalled(snapshot)
			if graphEvents == 0 {
				t.Fatal("the demo dump produced no calls or served requests")
			}
			stats := &snapshot.TrafficStats
			subjects, operations := acceptedByFinals(collector.finals)
			want := accounting{
				SubjectsOffered:   graphEvents,
				OperationsOffered: graphEvents,
				Accepted:          graphEvents,
				SubjectFinals:     graphEvents,
				OperationFinals:   graphEvents,
			}
			got := accounting{
				SubjectsOffered:   stats.Subjects.Offered,
				OperationsOffered: stats.Operations.Offered + stats.Overflow.Offered,
				Accepted:          stats.Subjects.OnTime + stats.Subjects.LateAccepted,
				SubjectFinals:     subjects,
				OperationFinals:   operations,
				SubjectSeries:     stats.Subjects.Series,
				OperationSeries:   stats.Operations.Series,
				OverflowSeries:    stats.Overflow.Series,
				SketchInUse:       stats.Sketch.InUse,
				Operations:        stats.Cardinality.Operations,
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("accounting after the flush (-want +got):\n%s", diff)
			}
			if overflow := stats.Overflow.Offered > 0; overflow != tt.WantOverflow {
				t.Errorf("overflow offered %d, want overflow %v", stats.Overflow.Offered, tt.WantOverflow)
			}
		})
	}
}

func servedAndCalled(snapshot *topology.Snapshot) uint64 {
	var events uint64
	for i := range snapshot.Edges {
		events += snapshot.Edges[i].Calls
	}
	for i := range snapshot.Nodes {
		events += snapshot.Nodes[i].Served
	}
	return events
}

// TestReplaySketchEntriesPerSeriesStaySmall checks B4 on the demo dump at its peak: 4.4 entries
// per subject series were measured, the bound leaves room for busier demo traffic.
func TestReplaySketchEntriesPerSeriesStaySmall(t *testing.T) {
	var peak float64
	replayFinals(t, defaultLimits(), func(builder *topology.Builder) {
		stats := builder.Snapshot().TrafficStats
		if stats.Subjects.Series > 0 {
			peak = max(peak, float64(stats.Sketch.InUse)/float64(stats.Subjects.Series))
		}
	})
	t.Logf("B4: at most %.1f sketch entries per subject series", peak)
	if peak == 0 || peak > maxEntriesPerSeries {
		t.Errorf("B4: peak %.1f sketch entries per subject series, want (0, %d]", peak, maxEntriesPerSeries)
	}
}
