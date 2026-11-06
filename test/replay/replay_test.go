package replay_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/spanmap"
)

const (
	goldenPath       = "test/demo/otel-demo/replay.golden.json"
	requiredPath     = "test/demo/otel-demo/required-edges.txt"
	optionalPath     = "test/demo/otel-demo/optional-edges.txt"
	updateGoldenEnv  = "FAULTLINE_UPDATE_GOLDEN"
	commentPrefix    = "#"
	reasonSeparator  = " # "
	goldenFileMode   = 0o644
	pairingTTL       = 10 * time.Second
	maxPending       = 100_000
	tombstoneTTL     = 60 * time.Second
	maxTombstones    = 1_000_000
	maxNodes         = 10_000
	maxEdges         = 50_000
	edgeFieldsInLine = 2
)

type collectingJournal struct {
	lock    sync.Mutex
	batches []batch.Batch
}

func (j *collectingJournal) Append(_ context.Context, received batch.Batch) error {
	j.lock.Lock()
	defer j.lock.Unlock()
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

func defaultLimits() topology.Limits {
	return topology.Limits{
		PairingTTL:    pairingTTL,
		MaxPending:    maxPending,
		TombstoneTTL:  tombstoneTTL,
		MaxTombstones: maxTombstones,
		MaxNodes:      maxNodes,
		MaxEdges:      maxEdges,
	}
}

func replay(t *testing.T, limits topology.Limits, lines []ptrace.Traces) *topology.Snapshot {
	t.Helper()
	clock := &monotonicClock{}
	clock.AdvanceTo(lastEnd(lines[0]))
	builder, err := topology.NewBuilder(limits, clock.Now())
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	journal := &collectingJournal{}
	accept := ingest.NewAcceptBatch(journal, clock)
	for _, line := range lines {
		clock.AdvanceTo(lastEnd(line))
		if _, err := accept.Execute(t.Context(), spanmap.NewSource(line)); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		for _, received := range journal.Take() {
			builder.Apply(received)
		}
	}
	return builder.Snapshot()
}

type goldenNode struct {
	ID                    string `json:"id"`
	UnansweredCalls       uint64 `json:"unanswered_calls"`
	UnansweredFailedCalls uint64 `json:"unanswered_failed_calls"`
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
	for _, node := range snapshot.Nodes {
		result.Nodes = append(result.Nodes, goldenNode{
			ID:                    node.ID,
			UnansweredCalls:       node.UnansweredCalls,
			UnansweredFailedCalls: node.UnansweredFailedCalls,
		})
	}
	for _, edge := range snapshot.Edges {
		result.Edges = append(result.Edges, goldenEdge(edge))
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

type edgeKey struct {
	Source string
	Target string
}

func readEdges(t *testing.T, path string) map[edgeKey]bool {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	}()
	edges := map[edgeKey]bool{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if key, ok := parseEdge(t, scanner.Text()); ok {
			edges[key] = true
		}
	}
	return edges
}

func parseEdge(t *testing.T, line string) (edgeKey, bool) {
	t.Helper()
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, commentPrefix) {
		return edgeKey{}, false
	}
	edge, _, _ := strings.Cut(line, reasonSeparator)
	fields := strings.Fields(edge)
	if len(fields) != edgeFieldsInLine {
		t.Fatalf("edge line %q is not '<source> <target>'", line)
	}
	return edgeKey{
		Source: fields[0],
		Target: fields[1],
	}, true
}

func TestReplayEdgesStayWithinTheReference(t *testing.T) {
	root := otlptest.ModuleRoot(t)
	required := readEdges(t, filepath.Join(root, requiredPath))
	optional := readEdges(t, filepath.Join(root, optionalPath))
	seen := map[edgeKey]bool{}
	for _, edge := range replay(t, defaultLimits(), otlptest.DemoLines(t)).Edges {
		key := edgeKey{
			Source: edge.Source,
			Target: edge.Target,
		}
		seen[key] = true
		if !required[key] && !optional[key] {
			t.Errorf("phantom edge %s -> %s is in neither reference list", key.Source, key.Target)
		}
	}
	for key := range required {
		if !seen[key] {
			t.Errorf("required edge %s -> %s is missing from the replay", key.Source, key.Target)
		}
	}
}
