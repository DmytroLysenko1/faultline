package main

import (
	"fmt"
	"strings"

	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const spansPerCall = 2

type verdict struct {
	onlyMissing bool
	required    int
	optional    int
	edges       int
	result      reference.Result
	noNamespace bool
	i1          *balance
	i3          *balance
	red         *redVerdict
}

type balance struct {
	left  uint64
	right uint64
}

func (b *balance) holds() bool {
	return b == nil || b.left == b.right
}

func newVerdict(expected *reference.Reference, shown *shownGraph, result reference.Result, stats *statsRecord) verdict {
	built := verdict{
		required:    len(expected.Required),
		optional:    seenOptional(expected, shown.edges),
		edges:       len(shown.edges),
		result:      result,
		noNamespace: len(shown.edges) > 0 && len(result.Unqualified) == distinctNodes(shown.edges),
		i1:          i1Of(&stats.Pairing),
	}
	if stats.Revision == shown.revision {
		built.i3 = &balance{left: shown.calls + stats.Graph.GraphOverflow, right: stats.Pairing.Matched}
	}
	return built
}

func requiredOnly(expected *reference.Reference, shown *shownGraph, result reference.Result) verdict {
	return verdict{
		onlyMissing: true,
		required:    len(expected.Required),
		result:      reference.Result{Missing: result.Missing},
		noNamespace: len(shown.edges) > 0 && len(result.Unqualified) == distinctNodes(shown.edges),
	}
}

func i1Of(counts *pairing) *balance {
	accounted := spansPerCall*counts.Matched + counts.Duplicate + counts.Unpairable + counts.Evicted +
		counts.Expired + counts.Pending
	return &balance{left: counts.Observed, right: accounted}
}

func seenOptional(expected *reference.Reference, edges []reference.Edge) int {
	seen := 0
	for _, edge := range edges {
		if expected.Optional[edge] {
			seen++
		}
	}
	return seen
}

func distinctNodes(edges []reference.Edge) int {
	nodes := map[string]bool{}
	for _, edge := range edges {
		nodes[edge.Source] = true
		nodes[edge.Target] = true
	}
	return len(nodes)
}

func (v *verdict) Passed() bool {
	return v.result.Passed() && v.i1.holds() && v.i3.holds() && v.red.passed()
}

func (v *verdict) String() string {
	var out strings.Builder
	writeEdges(&out, "missing required edge", v.result.Missing)
	writeEdges(&out, "phantom edge", v.result.Phantoms)
	v.writeNamespace(&out)
	writeBalance(&out, "I1 observed != 2*matched+duplicate+unpairable+evicted+expired+pending", v.i1)
	writeBalance(&out, "I3 sum of edge calls+graph_overflow != matched", v.i3)
	v.red.writeProblems(&out)
	if v.onlyMissing {
		fmt.Fprintf(&out, "%d/%d required\n", v.required-len(v.result.Missing), v.required)
		v.red.writeSummary(&out)
		return out.String()
	}
	fmt.Fprintf(&out, "%d/%d required, %d optional, %d edges, I1 %s, I3 %s\n",
		v.required-len(v.result.Missing), v.required, v.optional, v.edges, state(v.i1), state(v.i3))
	v.red.writeSummary(&out)
	return out.String()
}

func (v *verdict) writeNamespace(out *strings.Builder) {
	if v.noNamespace {
		fmt.Fprintf(out, "no node is in %s: is the demo installed in another namespace?\n", reference.Namespace)
		return
	}
	for _, node := range v.result.Unqualified {
		fmt.Fprintf(out, "node without namespace: %s\n", node)
	}
}

func writeEdges(out *strings.Builder, label string, edges []reference.Edge) {
	for _, edge := range edges {
		fmt.Fprintf(out, "%s: %s -> %s\n", label, edge.Source, edge.Target)
	}
}

func writeBalance(out *strings.Builder, label string, check *balance) {
	if !check.holds() {
		fmt.Fprintf(out, "%s: %d != %d\n", label, check.left, check.right)
	}
}

func state(check *balance) string {
	switch {
	case check == nil:
		return "skipped"
	case check.holds():
		return "ok"
	default:
		return "broken"
	}
}
