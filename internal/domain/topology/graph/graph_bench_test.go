package graph_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
)

const (
	benchNodes = 10_000
	benchEdges = 50_000
)

func BenchmarkHeapPerGraphEdge(b *testing.B) {
	names := lo.Times(benchNodes, func(i int) string {
		return fmt.Sprintf("service-%05d", i)
	})
	calls := lo.Times(benchEdges, func(i int) call.Call {
		return call.Call{
			Caller: telemetrytest.Service(names[i%benchNodes]),
			Callee: telemetrytest.Service(names[(i/benchNodes+i+1)%benchNodes]),
		}
	})
	change := graph.Change{
		ReceivedAt: telemetrytest.Epoch(),
		Services: lo.Map(names, func(name string, _ int) resource.ServiceRef {
			return telemetrytest.Service(name)
		}),
		Calls: calls,
	}
	limits := graph.Limits{
		MaxNodes: benchNodes,
		MaxEdges: benchEdges,
	}
	for b.Loop() {
		before := telemetrytest.HeapInUse()
		services, err := graph.New(limits, telemetrytest.Epoch())
		if err != nil {
			b.Fatalf("graph.New: %v", err)
		}
		services.Apply(&change)
		services.CurrentView()
		b.ReportMetric(float64(telemetrytest.HeapInUse()-before)/benchEdges, "B/edge(+nodes+view)")
		b.ReportMetric(float64(len(services.Edges)), "edges")
		runtime.KeepAlive(services)
	}
}
