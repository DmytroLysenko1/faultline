package replay_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/spanmap"
	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	goldenPath      = "test/demo/otel-demo/replay.golden.json"
	requiredPath    = "test/demo/otel-demo/required-edges.txt"
	optionalPath    = "test/demo/otel-demo/optional-edges.txt"
	updateGoldenEnv = "FAULTLINE_UPDATE_GOLDEN"
	goldenFileMode  = 0o644
	pairingTTL      = 10 * time.Second
	maxPending      = 100_000
	tombstoneTTL    = 60 * time.Second
	maxTombstones   = 1_000_000
	maxNodes        = 10_000
	maxEdges        = 50_000
)

type collectingJournal struct {
	lock    sync.Mutex
	clock   *monotonicClock
	batches []batch.Batch
}

func (j *collectingJournal) Append(_ context.Context, spans []span.Span) error {
	j.lock.Lock()
	defer j.lock.Unlock()
	received, err := batch.New(spans, j.clock.Now())
	if err != nil {
		return fmt.Errorf("collect batch: %w", err)
	}
	j.batches = append(j.batches, received)
	return nil
}

func (j *collectingJournal) Take() []batch.Batch {
	j.lock.Lock()
	defer j.lock.Unlock()
	taken := j.batches
	j.batches = nil
	return taken
}

type monotonicClock struct {
	now time.Time
}

func (c *monotonicClock) Now() time.Time {
	return c.now
}

func (c *monotonicClock) AdvanceTo(at time.Time) {
	if at.After(c.now) {
		c.now = at
	}
}

func lastEnd(traces ptrace.Traces) time.Time {
	var latest time.Time
	for _, resourceSpans := range traces.ResourceSpans().All() {
		for _, scopeSpans := range resourceSpans.ScopeSpans().All() {
			for _, observed := range scopeSpans.Spans().All() {
				latest = later(latest, observed.EndTimestamp().AsTime())
			}
		}
	}
	return latest
}

func later(first, second time.Time) time.Time {
	if second.After(first) {
		return second
	}
	return first
}

func defaultLimits() *topology.Limits {
	limits := topologytest.Limits()
	limits.PairingTTL, limits.MaxPending = pairingTTL, maxPending
	limits.TombstoneTTL, limits.MaxTombstones = tombstoneTTL, maxTombstones
	limits.MaxNodes, limits.MaxEdges = maxNodes, maxEdges
	return &limits
}

func replay(t *testing.T, limits *topology.Limits, lines []ptrace.Traces) *topology.Snapshot {
	t.Helper()
	builder, _ := replayInto(t, limits, lines, func(*topology.Builder) {})
	return builder.Snapshot()
}

// replayInto feeds the dump through ingest and the builder, calling afterApply after every journal
// record, and returns the builder with the journal time of the last record.
func replayInto(t *testing.T, limits *topology.Limits, lines []ptrace.Traces, afterApply func(*topology.Builder),
	options ...topology.Option,
) (*topology.Builder, time.Time) {
	t.Helper()
	clock := &monotonicClock{}
	clock.AdvanceTo(lastEnd(lines[0]))
	builder, err := topology.NewBuilder(limits, clock.Now(), options...)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	journal := &collectingJournal{clock: clock}
	accept := ingest.NewAcceptBatch(journal, operation.NewNormalizer(nil))
	for _, line := range lines {
		clock.AdvanceTo(lastEnd(line))
		if _, err := accept.Execute(t.Context(), spanmap.NewSource(line)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		for _, received := range journal.Take() {
			builder.Apply(received)
			afterApply(builder)
		}
	}
	return builder, clock.Now()
}

type goldenNode struct {
	ID                    string `json:"id"`
	UnansweredCalls       uint64 `json:"unanswered_calls"`
	UnansweredFailedCalls uint64 `json:"unanswered_failed_calls"`
	Served                uint64 `json:"served"`
}

type goldenEdge struct {
	Source      string `json:"source"`
	Target      string `json:"target"`
	Calls       uint64 `json:"calls"`
	FailedCalls uint64 `json:"failed_calls"`
}

type golden struct {
	Revision    uint64        `json:"revision"`
	GeneratedAt time.Time     `json:"generated_at"`
	Nodes       []goldenNode  `json:"nodes"`
	Edges       []goldenEdge  `json:"edges"`
	Pairing     matcher.Stats `json:"pairing"`
	Graph       graph.Stats   `json:"graph"`
}

func goldenOf(snapshot *topology.Snapshot) golden {
	result := golden{
		Revision:    snapshot.Revision,
		GeneratedAt: snapshot.GeneratedAt.UTC(),
		Pairing:     snapshot.PairingStats,
		Graph:       snapshot.GraphStats,
	}
	for i := range snapshot.Nodes {
		node := &snapshot.Nodes[i]
		result.Nodes = append(result.Nodes, goldenNode{
			ID:                    node.ID,
			UnansweredCalls:       node.UnansweredCalls,
			UnansweredFailedCalls: node.UnansweredFailedCalls,
			Served:                node.Served,
		})
	}
	for i := range snapshot.Edges {
		edge := &snapshot.Edges[i]
		result.Edges = append(result.Edges, goldenEdge{
			Source:      edge.Source,
			Target:      edge.Target,
			Calls:       edge.Calls,
			FailedCalls: edge.FailedCalls,
		})
	}
	return result
}

func readGolden(t *testing.T, path string, encoded []byte) golden {
	t.Helper()
	if os.Getenv(updateGoldenEnv) != "" {
		if err := os.WriteFile(path, encoded, goldenFileMode); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (regenerate with %s=1): %v", path, updateGoldenEnv, err)
	}
	var decoded golden
	if err := json.Unmarshal(stored, &decoded); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return decoded
}

func TestReplayOfTheDemoDumpMatchesTheGolden(t *testing.T) {
	root := otlptest.ModuleRoot(t)
	got := goldenOf(replay(t, defaultLimits(), otlptest.DemoLines(t)))
	encoded, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("encode replay: %v", err)
	}
	want := readGolden(t, filepath.Join(root, goldenPath), append(encoded, '\n'))
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("replay (-golden +got); regenerate with %s=1 only after reviewing the diff:\n%s", updateGoldenEnv, diff)
	}
}

func TestReplayAccountsForEverySpan(t *testing.T) {
	stats := replay(t, defaultLimits(), otlptest.DemoLines(t)).PairingStats
	accounted := 2*stats.Matched + stats.Duplicate + stats.Unpairable + stats.Evicted + stats.Expired + stats.Pending
	if stats.Observed == 0 || accounted != stats.Observed {
		t.Errorf("I1 broken on the demo dump: observed %d, accounted %d (%+v)", stats.Observed, accounted, stats)
	}
}

func TestReplayEdgesStayWithinTheReference(t *testing.T) {
	root := otlptest.ModuleRoot(t)
	expected, err := reference.Load(filepath.Join(root, requiredPath), filepath.Join(root, optionalPath))
	if err != nil {
		t.Fatalf("load reference: %v", err)
	}
	var edges []reference.Edge
	for _, edge := range replay(t, defaultLimits(), otlptest.DemoLines(t)).Edges {
		edges = append(edges, reference.Edge{
			Source: edge.Source,
			Target: edge.Target,
		})
	}
	if result := expected.Check(edges); !result.Passed() {
		t.Errorf("replay against the reference: missing %v, phantoms %v, without namespace %v",
			result.Missing, result.Phantoms, result.Unqualified)
	}
}
