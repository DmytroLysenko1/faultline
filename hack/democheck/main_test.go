package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/cli"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	cart        = "svc:otel-demo/cart"
	checkout    = "svc:otel-demo/checkout"
	flagd       = "svc:otel-demo/flagd"
	frontend    = "svc:otel-demo/frontend"
	bareCart    = "svc:/cart"
	fileMode    = 0o600
	revision    = 7
	callsEach   = 3
	required    = "# required\n" + frontend + " " + checkout + "\n" + checkout + " " + cart + "\n"
	optional    = "# optional\n" + cart + " " + flagd + " # flags are cached\n"
	unjustified = "# optional\n" + cart + " " + flagd + "\n"
	goldenPath  = "test/demo/otel-demo/replay.golden.json"
)

type fixture struct {
	edges    []reference.Edge
	stats    string
	optional string
	graph    string
	extra    []string
}

func edge(source, target string) reference.Edge {
	return reference.Edge{Source: source, Target: target}
}

func passingEdges() []reference.Edge {
	return []reference.Edge{edge(frontend, checkout), edge(checkout, cart), edge(cart, flagd)}
}

func graphOfEdges(edges []reference.Edge, calls uint64) *faultlinev1.Graph {
	var ids []string
	for _, each := range edges {
		ids = append(ids, each.Source, each.Target)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	built := &faultlinev1.Graph{Revision: revision}
	for _, id := range ids {
		built.Nodes = append(built.Nodes, &faultlinev1.Node{Id: id, Service: &faultlinev1.ServiceRef{Name: id}})
	}
	for _, each := range edges {
		built.Edges = append(built.Edges, &faultlinev1.Edge{
			SourceIndex: uint32(slices.Index(ids, each.Source)),
			TargetIndex: uint32(slices.Index(ids, each.Target)),
			Calls:       calls,
		})
	}
	return built
}

func statsLog(t *testing.T, record map[string]any) string {
	t.Helper()
	record["msg"] = statsMessage
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}
	return `{"msg":"lifecycle.state","state":"serving"}` + "\n" + string(encoded) + "\n"
}

func balancedStats(t *testing.T, matched uint64, statsRevision uint64) string {
	t.Helper()
	return statsLog(t, map[string]any{
		"revision": statsRevision,
		"pairing":  map[string]uint64{"observed": 2*matched + 3, "matched": matched, "unpairable": 3},
		"graph":    map[string]uint64{"graph_overflow": 0},
	})
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), fileMode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func renderGraph(t *testing.T, built *faultlinev1.Graph) string {
	t.Helper()
	var out bytes.Buffer
	if err := cli.JSON(&out, built); err != nil {
		t.Fatalf("render graph: %v", err)
	}
	return out.String()
}

type outcome struct {
	Code  int
	Lines []string
}

func runFixture(t *testing.T, given *fixture) (outcome, string) {
	t.Helper()
	dir := t.TempDir()
	graph := cmpOr(given.graph, renderGraph(t, graphOfEdges(given.edges, callsEach)))
	args := append([]string{
		"-graph", writeFile(t, dir, "graph.json", graph),
		"-required", writeFile(t, dir, "required.txt", required),
		"-optional", writeFile(t, dir, "optional.txt", cmpOr(given.optional, optional)),
	}, given.extra...)
	var stdout, stderr bytes.Buffer
	code := run(args, strings.NewReader(given.stats), &stdout, &stderr)
	return outcome{Code: code, Lines: strings.Split(strings.TrimSpace(stdout.String()), "\n")}, stderr.String()
}

func cmpOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func TestDemoCheckVerdicts(t *testing.T) {
	tests := []struct {
		Name string
		Args func(t *testing.T) fixture
		Want outcome
	}{
		{
			Name: "every required edge, one optional, both invariants hold",
			Args: func(t *testing.T) fixture {
				return fixture{edges: passingEdges(), stats: balancedStats(t, 3*callsEach, revision)}
			},
			Want: outcome{Code: exitOK, Lines: []string{"2/2 required, 1 optional, 3 edges, I1 ok, I3 ok"}},
		},
		{
			Name: "older stats skip I3",
			Args: func(t *testing.T) fixture {
				return fixture{edges: passingEdges(), stats: balancedStats(t, 1, revision-1)}
			},
			Want: outcome{Code: exitOK, Lines: []string{"2/2 required, 1 optional, 3 edges, I1 ok, I3 skipped"}},
		},
		{
			Name: "a required edge is missing",
			Args: func(t *testing.T) fixture {
				return fixture{edges: passingEdges()[1:], stats: balancedStats(t, 2*callsEach, revision)}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"missing required edge: " + frontend + " -> " + checkout,
				"1/2 required, 1 optional, 2 edges, I1 ok, I3 ok",
			}},
		},
		{
			Name: "a phantom edge",
			Args: func(t *testing.T) fixture {
				edges := append(passingEdges(), edge(frontend, flagd))
				return fixture{edges: edges, stats: balancedStats(t, 4*callsEach, revision)}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"phantom edge: " + frontend + " -> " + flagd,
				"2/2 required, 1 optional, 4 edges, I1 ok, I3 ok",
			}},
		},
		{
			Name: "one node lost its namespace",
			Args: func(t *testing.T) fixture {
				edges := append(passingEdges(), edge(checkout, bareCart))
				return fixture{edges: edges, stats: balancedStats(t, 4*callsEach, revision)}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"phantom edge: " + checkout + " -> " + bareCart,
				"node without namespace: " + bareCart,
				"2/2 required, 1 optional, 4 edges, I1 ok, I3 ok",
			}},
		},
		{
			Name: "the demo runs in another namespace",
			Args: func(t *testing.T) fixture {
				edges := []reference.Edge{edge("svc:demo/frontend", "svc:demo/checkout")}
				return fixture{edges: edges, stats: balancedStats(t, callsEach, revision)}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"missing required edge: " + checkout + " -> " + cart,
				"missing required edge: " + frontend + " -> " + checkout,
				"phantom edge: svc:demo/frontend -> svc:demo/checkout",
				"no node is in svc:otel-demo/: is the demo installed in another namespace?",
				"0/2 required, 0 optional, 1 edges, I1 ok, I3 ok",
			}},
		},
		{
			Name: "I1 does not balance",
			Args: func(t *testing.T) fixture {
				stats := statsLog(t, map[string]any{
					"revision": revision - 1,
					"pairing":  map[string]uint64{"observed": 10, "matched": 4},
				})
				return fixture{edges: passingEdges(), stats: stats}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I1 observed != 2*matched+duplicate+unpairable+evicted+expired+pending: 10 != 8",
				"2/2 required, 1 optional, 3 edges, I1 broken, I3 skipped",
			}},
		},
		{
			Name: "I3 does not balance on the same revision",
			Args: func(t *testing.T) fixture {
				return fixture{edges: passingEdges(), stats: balancedStats(t, 3*callsEach+1, revision)}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I3 sum of edge calls+graph_overflow != matched: 9 != 10",
				"2/2 required, 1 optional, 3 edges, I1 ok, I3 broken",
			}},
		},
		{
			Name: "required-only ignores phantoms and the missing log",
			Args: func(*testing.T) fixture {
				return fixture{edges: append(passingEdges(), edge(frontend, flagd)), extra: []string{"-required-only"}}
			},
			Want: outcome{Code: exitOK, Lines: []string{"2/2 required"}},
		},
		{
			Name: "required-only reports a missing edge",
			Args: func(*testing.T) fixture {
				return fixture{edges: passingEdges()[1:], extra: []string{"-required-only"}}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"missing required edge: " + frontend + " -> " + checkout,
				"1/2 required",
			}},
		},
		{
			Name: "required-only names the wrong namespace",
			Args: func(*testing.T) fixture {
				edges := []reference.Edge{edge("svc:demo/frontend", "svc:demo/checkout")}
				return fixture{edges: edges, extra: []string{"-required-only"}}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"missing required edge: " + checkout + " -> " + cart,
				"missing required edge: " + frontend + " -> " + checkout,
				"no node is in svc:otel-demo/: is the demo installed in another namespace?",
				"0/2 required",
			}},
		},
		{
			Name: "graph overflow larger than matched is reported, not wrapped",
			Args: func(t *testing.T) fixture {
				stats := statsLog(t, map[string]any{
					"revision": revision,
					"pairing":  map[string]uint64{"observed": 2, "matched": 1},
					"graph":    map[string]uint64{"graph_overflow": 5},
				})
				return fixture{edges: passingEdges(), stats: stats}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I3 sum of edge calls+graph_overflow != matched: 14 != 1",
				"2/2 required, 1 optional, 3 edges, I1 ok, I3 broken",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			given := tt.Args(t)
			got, stderr := runFixture(t, &given)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("democheck (-want +got):\n%s\nstderr: %s", diff, stderr)
			}
		})
	}
}

func TestDemoCheckRejectsBrokenInput(t *testing.T) {
	tests := []struct {
		Name string
		Args fixture
		Want string
	}{
		{Name: "no pipeline.stats in the log", Args: fixture{edges: passingEdges(), stats: "{}\n"}, Want: errNoStats.Error()},
		{Name: "graph is not JSON", Args: fixture{graph: "{not json", stats: "{}\n"}, Want: "parse graph"},
		{
			Name: "optional edge without a reason",
			Args: fixture{edges: passingEdges(), optional: unjustified},
			Want: reference.ErrUnjustified.Error(),
		},
		{
			Name: "edge refers to a missing node",
			Args: fixture{graph: `{"nodes":[],"edges":[{"source_index":0,"target_index":1}]}`},
			Want: errDanglingEdge.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, stderr := runFixture(t, &tt.Args)
			if got.Code != exitBadInput || !strings.Contains(stderr, tt.Want) {
				t.Errorf("democheck = %d with stderr %q, want %d and %q", got.Code, stderr, exitBadInput, tt.Want)
			}
		})
	}
}

func TestDemoCheckNeedsAGraph(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, strings.NewReader(""), &stdout, &stderr); code != exitBadInput {
		t.Errorf("democheck without -graph = %d, want %d", code, exitBadInput)
	}
}

type goldenGraph struct {
	Revision uint64 `json:"revision"`
	Edges    []struct {
		Source string `json:"source"`
		Target string `json:"target"`
		Calls  uint64 `json:"calls"`
	} `json:"edges"`
	Pairing map[string]uint64 `json:"pairing"`
	Graph   map[string]uint64 `json:"graph"`
}

func TestReplayOfTheDemoDumpPassesTheDemoCheck(t *testing.T) {
	root := otlptest.ModuleRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, goldenPath))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden goldenGraph
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	var edges []reference.Edge
	for _, each := range golden.Edges {
		edges = append(edges, edge(each.Source, each.Target))
	}
	built := graphOfEdges(edges, 0)
	built.Revision = golden.Revision
	for index, each := range golden.Edges {
		built.Edges[index].Calls = each.Calls
	}
	stats := statsLog(t, map[string]any{
		"revision": golden.Revision,
		"pairing":  lowerKeys(golden.Pairing),
		"graph":    map[string]uint64{"graph_overflow": golden.Graph["GraphOverflow"]},
	})
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-graph", writeFile(t, dir, "graph.json", renderGraph(t, built)),
		"-required", filepath.Join(root, defaultRequired),
		"-optional", filepath.Join(root, defaultOptional),
	}, strings.NewReader(stats), &stdout, &stderr)
	if code != exitOK {
		t.Errorf("democheck on the replay = %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func lowerKeys(counts map[string]uint64) map[string]uint64 {
	lowered := make(map[string]uint64, len(counts))
	for key, value := range counts {
		lowered[strings.ToLower(key)] = value
	}
	return lowered
}
