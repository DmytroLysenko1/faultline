package cli_test

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/simple"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/cli"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

const (
	updateGoldenEnv = "FAULTLINE_UPDATE_GOLDEN"
	goldenDir       = "testdata"
	goldenFileMode  = 0o600
	labelKey        = "label"
	nodePrefix      = "svc:"
	demoNamespace   = "demo"
)

type service struct {
	Namespace string
	Name      string
}

type namedNode struct {
	service    service
	unanswered uint64
	failed     uint64
}

type call struct {
	source int
	target int
	calls  uint64
	failed uint64
	label  string
}

type renderCase struct {
	Name    string
	Nodes   []namedNode
	Calls   []call
	Labels  map[string]string
	WithRED bool
}

const (
	redWindow  = 10 * time.Second
	redLatency = 3 * time.Millisecond
)

// withRED fills the summaries the way a running server does: a live window with latency samples,
// a final one without them (no percentiles), an edge final after one late pane.
func withRED(built *faultlinev1.Graph) *faultlinev1.Graph {
	window := built.GetGeneratedAt().AsTime().Truncate(redWindow)
	stats := func(
		kind faultlinev1.PaneKind,
		start time.Time,
		revision uint32,
		failed, samples uint64,
	) *faultlinev1.WindowStats {
		summary := &faultlinev1.WindowStats{
			Start:          timestamppb.New(start),
			Kind:           kind,
			Revision:       revision,
			Requests:       12,
			FailedRequests: failed,
			LatencySamples: samples,
			Observed:       durationpb.New(redWindow),
		}
		if samples > 0 {
			summary.P50, summary.P95, summary.P99 = durationpb.New(redLatency), durationpb.New(redLatency),
				durationpb.New(redLatency)
		}
		return summary
	}
	built.WindowSize = durationpb.New(redWindow)
	built.Watermark = timestamppb.New(window.Add(-redWindow))
	built.Nodes[0].ServedRequests = 5
	built.Nodes[0].Live = stats(faultlinev1.PaneKind_PANE_KIND_EARLY, window, 0, 2, 12)
	built.Nodes[0].Final = stats(faultlinev1.PaneKind_PANE_KIND_ON_TIME, window.Add(-redWindow), 1, 2, 0)
	built.Edges[0].Final = stats(faultlinev1.PaneKind_PANE_KIND_LATE, window.Add(-redWindow), 2, 2, 12)
	built.Edges[1].Live = stats(faultlinev1.PaneKind_PANE_KIND_EARLY, window, 0, 0, 12)
	return built
}

func (tt *renderCase) graph() *faultlinev1.Graph {
	built := graphOf(tt.Nodes, tt.Calls)
	if tt.WithRED {
		return withRED(built)
	}
	return built
}

func nodeID(of service) string {
	return nodePrefix + url.PathEscape(of.Namespace) + "/" + url.PathEscape(of.Name)
}

func graphOf(nodes []namedNode, calls []call) *faultlinev1.Graph {
	built := &faultlinev1.Graph{
		Revision:    42,
		GeneratedAt: timestamppb.New(time.Date(2026, time.November, 8, 0, 0, 0, 0, time.UTC)),
	}
	for _, node := range nodes {
		built.Nodes = append(built.Nodes, &faultlinev1.Node{
			Id: nodeID(node.service),
			Service: &faultlinev1.ServiceRef{
				Namespace: node.service.Namespace,
				Name:      node.service.Name,
			},
			UnansweredCalls:       node.unanswered,
			UnansweredFailedCalls: node.failed,
		})
	}
	for _, edge := range calls {
		built.Edges = append(built.Edges, &faultlinev1.Edge{
			SourceIndex: uint32(edge.source),
			TargetIndex: uint32(edge.target),
			Calls:       edge.calls,
			FailedCalls: edge.failed,
		})
	}
	return built
}

func evilNames() []string {
	return []string{
		`say "hi"`, `back\slash`, `ends\`, "slash\\\nnewline", `\N`, `\G`, `\l`,
		"line\nbreak", "car\rriage", "\x1b[31mred", `{};->--`, "a<b>", "пробел ✓",
		`back\nslash`, "a&amp;b", "&#x1B;[31m", "&lt;b&gt;", "&", "rtl\u202Eevil", "line\u2028sep",
	}
}

func evilCase() renderCase {
	nodes := []namedNode{
		{service: service{Name: "bare"}},
		{service: service{Namespace: "team/a", Name: "svc"}},
	}
	labels := map[string]string{
		nodeID(nodes[0].service): "bare",
		nodeID(nodes[1].service): "team/a/svc",
	}
	for _, name := range evilNames() {
		evil := namedNode{service: service{Namespace: demoNamespace, Name: name}}
		nodes = append(nodes, evil)
		labels[nodeID(evil.service)] = demoNamespace + "/" + displayed(name)
	}
	return renderCase{
		Name:   "evil",
		Nodes:  nodes,
		Calls:  []call{{source: 2, target: 3, calls: 1, label: "calls 1, failed 0"}},
		Labels: labels,
	}
}

func displayed(name string) string {
	replacements := map[string]string{
		"car\rriage":    `car\x0Driage`,
		"\x1b[31mred":   `\x1B[31mred`,
		"rtl\u202Eevil": `rtl\u202Eevil`,
		"line\u2028sep": `line\u2028sep`,
	}
	if shown, found := replacements[name]; found {
		return shown
	}
	return name
}

func renderCases() []renderCase {
	demo := []namedNode{
		{service: service{Namespace: demoNamespace, Name: "cart"}, unanswered: 3, failed: 1},
		{service: service{Namespace: demoNamespace, Name: "checkout"}, unanswered: 1},
		{service: service{Namespace: demoNamespace, Name: "frontend"}},
	}
	return []renderCase{
		{
			Name:   "empty",
			Labels: map[string]string{},
		},
		{
			Name:  "demo",
			Nodes: demo,
			Calls: []call{
				{source: 2, target: 1, calls: 12, failed: 2, label: "calls 12, failed 2\n1.2/s err 16.7% p95 3ms"},
				{source: 1, target: 0, calls: 5, label: "calls 5, failed 0\n~1.2/s err 0.0% p95 3ms"},
			},
			Labels: map[string]string{
				nodeID(demo[0].service): "demo/cart\nserved 1.2/s err 16.7%\nunanswered 3 (failed 1)",
				nodeID(demo[1].service): "demo/checkout\nunanswered 1 (failed 0)",
				nodeID(demo[2].service): "demo/frontend",
			},
			WithRED: true,
		},
		evilCase(),
	}
}

func rendered(t *testing.T, render cli.Renderer, built *faultlinev1.Graph) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := render(&out, built); err != nil {
		t.Fatalf("render: %v", err)
	}
	return out.Bytes()
}

func compareGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join(goldenDir, name)
	if os.Getenv(updateGoldenEnv) != "" {
		if err := os.WriteFile(path, got, goldenFileMode); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (regenerate with %s=1): %v", path, updateGoldenEnv, err)
	}
	if diff := cmp.Diff(string(want), string(got)); diff != "" {
		t.Errorf("%s (-golden +got); regenerate with %s=1 only after reviewing the diff:\n%s", path, updateGoldenEnv, diff)
	}
}

type dotNode struct {
	graph.Node
	id    string
	attrs map[string]string
}

func (n *dotNode) SetDOTID(id string) {
	n.id = id
}

func (n *dotNode) SetAttribute(attribute encoding.Attribute) error {
	n.attrs[attribute.Key] = attribute.Value
	return nil
}

type dotEdge struct {
	graph.Edge
	attrs map[string]string
}

func (e *dotEdge) SetAttribute(attribute encoding.Attribute) error {
	e.attrs[attribute.Key] = attribute.Value
	return nil
}

type dotGraph struct {
	*simple.DirectedGraph
}

func (g dotGraph) NewNode() graph.Node {
	return &dotNode{
		Node:  g.DirectedGraph.NewNode(),
		attrs: map[string]string{},
	}
}

func (g dotGraph) NewEdge(from, to graph.Node) graph.Edge {
	return &dotEdge{
		Edge:  g.DirectedGraph.NewEdge(from, to),
		attrs: map[string]string{},
	}
}

type parsedDOT struct {
	Labels map[string]string
	Edges  [][3]string
	Extra  []string
}

func parseDOT(t *testing.T, source []byte) parsedDOT {
	t.Helper()
	parsed := dotGraph{DirectedGraph: simple.NewDirectedGraph()}
	if err := dot.Unmarshal(source, parsed); err != nil {
		t.Fatalf("gonum cannot parse the DOT: %v\n%s", err, source)
	}
	got := parsedDOT{Labels: map[string]string{}}
	for nodes := parsed.Nodes(); nodes.Next(); {
		node := nodes.Node().(*dotNode)
		got.Labels[node.id] = node.attrs[labelKey]
		got.Extra = append(got.Extra, extraAttributes(node.id, node.attrs)...)
	}
	for edges := parsed.Edges(); edges.Next(); {
		edge := edges.Edge().(*dotEdge)
		got.Edges = append(got.Edges, [3]string{edge.From().(*dotNode).id, edge.To().(*dotNode).id, edge.attrs[labelKey]})
		got.Extra = append(got.Extra, extraAttributes("edge", edge.attrs)...)
	}
	return got
}

func extraAttributes(owner string, attrs map[string]string) []string {
	var extra []string
	for key := range attrs {
		if key != labelKey {
			extra = append(extra, owner+"."+key)
		}
	}
	return extra
}

func expectedDOT(tt *renderCase) parsedDOT {
	want := parsedDOT{Labels: map[string]string{}}
	for id, label := range tt.Labels {
		want.Labels[id] = strings.ReplaceAll(label, "&", "&amp;")
	}
	for _, edge := range tt.Calls {
		want.Edges = append(want.Edges, [3]string{
			nodeID(tt.Nodes[edge.source].service),
			nodeID(tt.Nodes[edge.target].service),
			strings.ReplaceAll(edge.label, "&", "&amp;"),
		})
	}
	return want
}

func sortedEdges() cmp.Option {
	return cmpopts.SortSlices(func(a, b [3]string) bool {
		return a[0]+a[1] < b[0]+b[1]
	})
}

func TestDOTIsGoldenAndParsesBackToTheSameGraph(t *testing.T) {
	for _, tt := range renderCases() {
		t.Run(tt.Name, func(t *testing.T) {
			got := rendered(t, cli.DOT, tt.graph())
			compareGolden(t, tt.Name+".dot", got)
			if diff := cmp.Diff(expectedDOT(&tt), parseDOT(t, got), sortedEdges()); diff != "" {
				t.Errorf("DOT parsed by gonum (-want +got):\n%s", diff)
			}
		})
	}
}

func TestJSONIsGoldenStableAndParsesBackToTheSameGraph(t *testing.T) {
	for _, tt := range renderCases() {
		t.Run(tt.Name, func(t *testing.T) {
			built := tt.graph()
			got := rendered(t, cli.JSON, built)
			compareGolden(t, tt.Name+".json", got)
			if again := rendered(t, cli.JSON, built); !bytes.Equal(got, again) {
				t.Errorf("two renders of one graph differ:\n%s\n%s", got, again)
			}
			parsed := &faultlinev1.Graph{}
			if err := protojson.Unmarshal(got, parsed); err != nil {
				t.Fatalf("protojson.Unmarshal: %v", err)
			}
			if diff := cmp.Diff(built, parsed, protocmp.Transform()); diff != "" {
				t.Errorf("JSON round trip (-want +got):\n%s", diff)
			}
		})
	}
}
