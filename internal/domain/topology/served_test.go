package topology_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

func TestNodesCountTheRequestsTheyServed(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, 0,
		telemetrytest.Client(t, 1, 1, "frontend", nil),
		telemetrytest.Server(t, 1, 2, "cart", nil),
		telemetrytest.Server(t, 1, 2, "cart", nil),
		telemetrytest.Server(t, 2, 2, "cart", nil),
		telemetrytest.NewSpan(t, telemetrytest.SpanSpec{Kind: kind.Server, Trace: 3, SpanID: 1, Service: "frontend"}),
		telemetrytest.Client(t, 4, 1, "cart", map[string]string{"db.system.name": "postgresql"}),
		telemetrytest.NewSpan(t, telemetrytest.SpanSpec{Kind: kind.Internal, Trace: 5, SpanID: 1, Service: "cart"}),
	))

	snapshot := builder.Snapshot()
	got := make(map[string]uint64, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		got[node.Service.Name] = node.Served
	}
	want := map[string]uint64{
		"cart":     2,
		"frontend": 1,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("served per node (-want +got):\n%s", diff)
	}
	if !snapshot.PairingStats.Balanced() {
		t.Errorf("pairing stats unbalanced with a root server span counted as unpairable: %+v", snapshot.PairingStats)
	}
}

func TestARetriedCalleeAfterTheMatchIsServedOnce(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	builder.Apply(batchAt(t, 0, telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil)))
	builder.Apply(batchAt(t, time.Second, telemetrytest.Server(t, 1, 2, "cart", nil)))

	if got := servedOf(builder.Snapshot(), "cart"); got != 1 {
		t.Errorf("cart served %d, want 1: the retry after the match is a tombstoned duplicate", got)
	}
}

func TestAServedSpanOfARefusedNodeCountsOnlyAsNodeOverflow(t *testing.T) {
	limits := builderLimits()
	limits.MaxNodes = 1
	builder := newTestBuilder(t, limits)
	builder.Apply(batchAt(t, 0,
		telemetrytest.NewSpan(t, telemetrytest.SpanSpec{Kind: kind.Server, Trace: 1, SpanID: 1, Service: "frontend"}),
		telemetrytest.NewSpan(t, telemetrytest.SpanSpec{Kind: kind.Server, Trace: 2, SpanID: 1, Service: "cart"}),
	))

	snapshot := builder.Snapshot()
	type observed struct {
		Frontend     uint64
		NodeOverflow uint64
		Nodes        int
	}
	want := observed{Frontend: 1, NodeOverflow: 1, Nodes: 1}
	got := observed{servedOf(snapshot, "frontend"), snapshot.GraphStats.NodeOverflow, len(snapshot.Nodes)}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("refused node (-want +got):\n%s", diff)
	}
}

func servedOf(snapshot *topology.Snapshot, service string) uint64 {
	for i := range snapshot.Nodes {
		if found := &snapshot.Nodes[i]; found.Service.Name == service {
			return found.Served
		}
	}
	return 0
}
