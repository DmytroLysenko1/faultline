//go:build !race

package graph_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
)

func fullGraph(t *testing.T, limits graph.Limits) *graph.Graph {
	t.Helper()
	services := make([]resource.ServiceRef, 0, limits.MaxNodes)
	for index := range limits.MaxNodes {
		services = append(services, telemetrytest.WorstService(index))
	}
	calls := make([]call.Call, 0, limits.MaxEdges)
	for index := range limits.MaxEdges {
		calls = append(calls, call.Call{
			Caller: services[index%limits.MaxNodes],
			Callee: services[(index/limits.MaxNodes+index+1)%limits.MaxNodes],
		})
	}
	built, err := graph.New(limits, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	built.Apply(&graph.Change{
		ReceivedAt: telemetrytest.Epoch(),
		Services:   services,
		Calls:      calls,
	})
	built.CurrentView()
	return built
}

func shapes() []graph.Limits {
	return []graph.Limits{
		{
			MaxNodes: 1_000,
			MaxEdges: 5_000,
		},
		{
			MaxNodes: 20_000,
			MaxEdges: 1,
		},
		{
			MaxNodes: 10_000,
			MaxEdges: 50_000,
		},
	}
}

func TestWorstCaseHeapCoversAFullGraphWithTheLongestStrings(t *testing.T) {
	for _, limits := range shapes() {
		t.Run(fmt.Sprintf("%d nodes, %d edges", limits.MaxNodes, limits.MaxEdges), func(t *testing.T) {
			grown := telemetrytest.HeapGrowth(func() any {
				return fullGraph(t, limits)
			})
			if model := uint64(limits.WorstCaseHeapBytes()); grown > model {
				t.Errorf("%+v: heap grew %d bytes, model %d", limits, grown, model)
			}
		})
	}
}

func TestViewHeapCoversOneCopyOfTheView(t *testing.T) {
	for _, limits := range shapes() {
		t.Run(fmt.Sprintf("%d nodes, %d edges", limits.MaxNodes, limits.MaxEdges), func(t *testing.T) {
			view := fullGraph(t, limits).CurrentView()
			grown := telemetrytest.HeapGrowth(func() any {
				return graph.View{
					Nodes: slices.Clone(view.Nodes),
					Edges: slices.Clone(view.Edges),
				}
			})
			if model := uint64(limits.ViewHeapBytes()); grown > model {
				t.Errorf("%+v: a view copy took %d bytes, model %d", limits, grown, model)
			}
		})
	}
}
