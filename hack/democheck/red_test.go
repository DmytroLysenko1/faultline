package main

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/durationpb"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	redFlag      = "-red"
	finalWindow  = 10 * time.Second
	passingStats = "RED: finals on 2/2 required edges, 2/2 receivers, 11/11 stats checks"
	passingGraph = "2/2 required, 1 optional, 3 edges, I1 ok, I3 ok"
	maxCount     = ^uint64(0)
)

func finalOf(requests, samples uint64) *faultlinev1.WindowStats {
	return &faultlinev1.WindowStats{
		Kind:           faultlinev1.PaneKind_PANE_KIND_ON_TIME,
		Revision:       1,
		Requests:       requests,
		LatencySamples: samples,
		Observed:       durationpb.New(finalWindow),
	}
}

func withFinals(built *faultlinev1.Graph) *faultlinev1.Graph {
	for _, node := range built.Nodes {
		node.Final = finalOf(callsEach, 0)
	}
	for _, each := range built.Edges {
		each.Final = finalOf(callsEach, callsEach)
	}
	return built
}

func counts(offered, onTime, lateAccepted, lateDropped, noWatermark uint64) map[string]any {
	return map[string]any{
		"offered":       offered,
		"on_time":       onTime,
		"late_accepted": lateAccepted,
		"late_dropped":  lateDropped,
		"no_watermark":  noWatermark,
		"series":        5,
		"capacity":      10,
	}
}

func redRecord() map[string]any {
	return map[string]any{
		"revision": revision,
		"pairing": map[string]any{
			"observed":   2*3*callsEach + 3,
			"matched":    3 * callsEach,
			"unpairable": 3,
		},
		"graph": map[string]any{
			"graph_overflow": 0,
			"calls_total":    60,
			"served_total":   40,
		},
		"windows": map[string]any{
			"subjects":   counts(100, 90, 5, 0, 5),
			"operations": counts(80, 72, 4, 0, 4),
			"overflow":   counts(20, 18, 1, 0, 1),
		},
		"cardinality": map[string]any{
			"operations":          12,
			"services_over_limit": 0,
		},
	}
}

func set(record map[string]any, path string, value any) map[string]any {
	keys := strings.Split(path, ".")
	group := record
	for _, key := range keys[:len(keys)-1] {
		group = group[key].(map[string]any)
	}
	group[keys[len(keys)-1]] = value
	return record
}

func drop(record map[string]any, key string) map[string]any {
	delete(record, key)
	return record
}

func redFixture(t *testing.T, record map[string]any, built *faultlinev1.Graph) fixture {
	t.Helper()
	return fixture{
		graph: renderGraph(t, built),
		stats: statsLog(t, record),
		extra: []string{redFlag},
	}
}

func finalGraph() *faultlinev1.Graph {
	return withFinals(graphOfEdges(passingEdges(), callsEach))
}

func nodeNamed(built *faultlinev1.Graph, id string) *faultlinev1.Node {
	for _, node := range built.Nodes {
		if node.Id == id {
			return node
		}
	}
	return nil
}

func edgeNamed(built *faultlinev1.Graph, source, target string) *faultlinev1.Edge {
	for _, each := range built.Edges {
		if built.Nodes[each.SourceIndex].Id == source && built.Nodes[each.TargetIndex].Id == target {
			return each
		}
	}
	return nil
}

func TestDemoCheckWithREDChecksFinalsAndTheStepTwoAccounting(t *testing.T) {
	tests := []struct {
		Name string
		Args func(t *testing.T) fixture
		Want outcome
	}{
		{
			Name: "finals on every required edge and receiver, every formula holds",
			Args: func(t *testing.T) fixture {
				return redFixture(t, redRecord(), finalGraph())
			},
			Want: outcome{Code: exitOK, Lines: []string{passingGraph, passingStats}},
		},
		{
			Name: "a required edge whose final has no latency",
			Args: func(t *testing.T) fixture {
				built := finalGraph()
				edgeNamed(built, frontend, checkout).Final = finalOf(callsEach, 0)
				return redFixture(t, redRecord(), built)
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"required edge without a final window with latency: " + frontend + " -> " + checkout,
				passingGraph,
				"RED: finals on 1/2 required edges, 2/2 receivers, 11/11 stats checks",
			}},
		},
		{
			Name: "a required edge with only a live window",
			Args: func(t *testing.T) fixture {
				built := finalGraph()
				found := edgeNamed(built, checkout, cart)
				found.Live, found.Final = found.Final, nil
				return redFixture(t, redRecord(), built)
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"required edge without a final window with latency: " + checkout + " -> " + cart,
				passingGraph,
				"RED: finals on 1/2 required edges, 2/2 receivers, 11/11 stats checks",
			}},
		},
		{
			Name: "a receiver that served nothing in its final window",
			Args: func(t *testing.T) fixture {
				built := finalGraph()
				nodeNamed(built, cart).Final = finalOf(0, 0)
				return redFixture(t, redRecord(), built)
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"receiver without a final window: " + cart,
				passingGraph,
				"RED: finals on 2/2 required edges, 1/2 receivers, 11/11 stats checks",
			}},
		},
		{
			Name: "a caller without a final window is not a receiver",
			Args: func(t *testing.T) fixture {
				built := finalGraph()
				nodeNamed(built, frontend).Final = nil
				return redFixture(t, redRecord(), built)
			},
			Want: outcome{Code: exitOK, Lines: []string{passingGraph, passingStats}},
		},
		{
			Name: "I6 does not balance in one engine",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "windows.overflow.no_watermark", 2), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I6 overflow.offered != on_time+late_accepted+late_dropped+no_watermark: 20 != 21",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "I6 does not balance in the subjects engine alone",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "windows.subjects.no_watermark", 6), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I6 subjects.offered != on_time+late_accepted+late_dropped+no_watermark: 100 != 101",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "a required edge whose final has latency samples but no requests",
			Args: func(t *testing.T) fixture {
				built := finalGraph()
				edgeNamed(built, frontend, checkout).Final = finalOf(0, callsEach)
				return redFixture(t, redRecord(), built)
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"required edge without a final window with latency: " + frontend + " -> " + checkout,
				passingGraph,
				"RED: finals on 1/2 required edges, 2/2 receivers, 11/11 stats checks",
			}},
		},
		{
			Name: "a graph group without served_total fails I10",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				delete(record["graph"].(map[string]any), "served_total")
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"graph.calls_total, graph.served_total: missing in pipeline.stats",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "a cardinality group without services_over_limit fails both cardinality checks",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				delete(record["cardinality"].(map[string]any), "services_over_limit")
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"cardinality.operations, cardinality.services_over_limit: missing in pipeline.stats",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 9/10 stats checks",
			}},
		},
		{
			Name: "I10 does not balance",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "graph.served_total", 39), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I10 graph.calls_total+graph.served_total != subjects.offered: 99 != 100",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "I11 split of offered events does not balance",
			Args: func(t *testing.T) fixture {
				record := set(redRecord(), "windows.overflow", counts(21, 19, 1, 0, 1))
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I11 operations.offered+overflow.offered != subjects.offered: 101 != 100",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "I11 late drops differ between subjects and operations",
			Args: func(t *testing.T) fixture {
				record := set(redRecord(), "windows.operations", counts(80, 71, 4, 1, 4))
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I11 operations.late_dropped != subjects.late_dropped: 1 != 0",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "more operation series than capacity",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "windows.operations.series", 11), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I11 operations.series > operations.capacity: 11 > 10",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "operation series at capacity",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "windows.operations.series", 10), finalGraph())
			},
			Want: outcome{Code: exitOK, Lines: []string{passingGraph, passingStats}},
		},
		{
			Name: "late drops just under one percent pass",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				set(record, "windows.subjects", counts(1000, 986, 5, 9, 0))
				set(record, "windows.operations", counts(800, 787, 4, 9, 0))
				set(record, "windows.overflow", counts(200, 199, 1, 0, 0))
				set(record, "graph.calls_total", 600)
				set(record, "graph.served_total", 400)
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitOK, Lines: []string{passingGraph, passingStats}},
		},
		{
			Name: "late drops at one percent fail",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				set(record, "windows.subjects", counts(100, 89, 5, 1, 5))
				set(record, "windows.operations", counts(80, 71, 4, 1, 4))
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"subjects.late_dropped/offered >= 1%: 1 of 100",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "a line without traffic fails on the empty base, not on the late share",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				set(record, "windows.subjects", counts(0, 0, 0, 0, 0))
				set(record, "windows.operations", counts(0, 0, 0, 0, 0))
				set(record, "windows.overflow", counts(0, 0, 0, 0, 0))
				set(record, "graph.calls_total", 0)
				set(record, "graph.served_total", 0)
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"windows.subjects.offered == 0: 0",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "a late share that overflows uint64 when scaled still fails",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				set(record, "windows.subjects", counts(maxCount, 0, 0, 1<<62, maxCount-1<<62))
				set(record, "windows.operations", counts(maxCount, 0, 0, 1<<62, maxCount-1<<62))
				set(record, "windows.overflow", counts(0, 0, 0, 0, 0))
				set(record, "graph.calls_total", maxCount)
				set(record, "graph.served_total", 0)
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"subjects.late_dropped/offered >= 1%: 4611686018427387904 of 18446744073709551615",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "terms that wrap around uint64 do not balance by accident",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "windows.overflow", counts(20, 18, 1, maxCount, 2)), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"I6 overflow.offered != on_time+late_accepted+late_dropped+no_watermark: " + overflowDetail,
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "an operations engine without series fails instead of reading zero",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				delete(record["windows"].(map[string]any)["operations"].(map[string]any), "series")
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"windows.operations.series, windows.operations.capacity: missing in pipeline.stats",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "a missing subjects engine is named for I10 instead of the graph totals",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				delete(record["windows"].(map[string]any), "subjects")
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"windows.subjects, windows.operations, windows.overflow: missing in pipeline.stats",
				"windows.subjects for I10: missing in pipeline.stats",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 2/4 stats checks",
			}},
		},
		{
			Name: "services over the operations limit",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "cardinality.services_over_limit", 1), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"cardinality.services_over_limit != 0: 1",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "no operations were normalized",
			Args: func(t *testing.T) fixture {
				return redFixture(t, set(redRecord(), "cardinality.operations", 0), finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"cardinality.operations == 0: 0",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 10/11 stats checks",
			}},
		},
		{
			Name: "a log from step 1 without the step 2 groups fails instead of skipping",
			Args: func(t *testing.T) fixture {
				record := drop(drop(redRecord(), "windows"), "cardinality")
				set(record, "graph", map[string]any{"graph_overflow": 0})
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"windows.subjects, windows.operations, windows.overflow: missing in pipeline.stats",
				"graph.calls_total, graph.served_total: missing in pipeline.stats",
				"cardinality.operations, cardinality.services_over_limit: missing in pipeline.stats",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 0/3 stats checks",
			}},
		},
		{
			Name: "one missing engine fails the engine checks and leaves I10 to the totals",
			Args: func(t *testing.T) fixture {
				record := redRecord()
				delete(record["windows"].(map[string]any), "overflow")
				return redFixture(t, record, finalGraph())
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"windows.subjects, windows.operations, windows.overflow: missing in pipeline.stats",
				passingGraph,
				"RED: finals on 2/2 required edges, 2/2 receivers, 3/4 stats checks",
			}},
		},
		{
			Name: "required-only with RED waits for the finals and ignores the log",
			Args: func(t *testing.T) fixture {
				built := finalGraph()
				nodeNamed(built, checkout).Final = nil
				return fixture{graph: renderGraph(t, built), extra: []string{redFlag, "-required-only"}}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"receiver without a final window: " + checkout,
				"2/2 required",
				"RED: finals on 2/2 required edges, 1/2 receivers",
			}},
		},
		{
			Name: "required-only with RED passes once every final is there",
			Args: func(t *testing.T) fixture {
				return fixture{graph: renderGraph(t, finalGraph()), extra: []string{redFlag, "-required-only"}}
			},
			Want: outcome{Code: exitOK, Lines: []string{
				"2/2 required",
				"RED: finals on 2/2 required edges, 2/2 receivers",
			}},
		},
		{
			Name: "a service receiving two required edges counts as one receiver",
			Args: func(t *testing.T) fixture {
				edges := []reference.Edge{edge(frontend, cart), edge(checkout, cart)}
				return fixture{
					graph:    renderGraph(t, withFinals(graphOfEdges(edges, callsEach))),
					required: "# required\n" + frontend + " " + cart + "\n" + checkout + " " + cart + "\n",
					extra:    []string{redFlag, "-required-only"},
				}
			},
			Want: outcome{Code: exitOK, Lines: []string{
				"2/2 required",
				"RED: finals on 2/2 required edges, 1/1 receivers",
			}},
		},
		{
			Name: "a missing required edge has no final either",
			Args: func(t *testing.T) fixture {
				built := withFinals(graphOfEdges(passingEdges()[1:], callsEach))
				return fixture{graph: renderGraph(t, built), extra: []string{redFlag, "-required-only"}}
			},
			Want: outcome{Code: exitFailed, Lines: []string{
				"missing required edge: " + frontend + " -> " + checkout,
				"required edge without a final window with latency: " + frontend + " -> " + checkout,
				"1/2 required",
				"RED: finals on 1/2 required edges, 2/2 receivers",
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

func TestDemoCheckWithoutREDKeepsTheStepOneVerdict(t *testing.T) {
	record := drop(drop(redRecord(), "windows"), "cardinality")
	given := fixture{
		graph: renderGraph(t, graphOfEdges(passingEdges(), callsEach)),
		stats: statsLog(t, record),
	}
	got, stderr := runFixture(t, &given)
	want := outcome{Code: exitOK, Lines: []string{passingGraph}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("democheck (-want +got):\n%s\nstderr: %s", diff, stderr)
	}
}
