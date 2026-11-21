package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
)

const tooSmallResponseBudget = 1

func TestTheAPIServesWithinTheConfiguredResponseBudget(t *testing.T) {
	store, err := memgraph.New(&topology.Snapshot{
		Nodes: []topology.NodeView{
			{Node: graph.NewNode(telemetrytest.Service("cart"))},
		},
	})
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	logger := testLogger(&logBuffer{})
	api, err := provideAPI(store, newLifecycle(logger), noop.NewTracerProvider(), logger, tooSmallResponseBudget)
	if err != nil {
		t.Fatalf("provideAPI: %v", err)
	}
	server := httptest.NewServer(api.(*http.Server).Handler)
	t.Cleanup(server.Close)
	client := faultlinev1connect.NewGraphServiceClient(server.Client(), server.URL)
	_, err = client.GetGraph(t.Context(), connect.NewRequest(&faultlinev1.GetGraphRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("GetGraph with a %d-byte budget = %v, want failed_precondition", tooSmallResponseBudget, err)
	}
}

func TestAPipelineThatCannotBeBuiltLeavesTheJournalClosed(t *testing.T) {
	cfg := e2eConfig(t)
	cfg.Journal.MaxBytes = 1
	var built storage
	_, err := assemble(&cfg, testLogger(&logBuffer{}), wiring{
		feedOf: func(stored storage) spanFeed {
			built = stored
			return stored.journal
		},
		tracing: selftrace.Disabled(),
	})
	if !errors.Is(err, processing.ErrInvalidCadence) {
		t.Fatalf("assemble = %v, want ErrInvalidCadence", err)
	}
	if _, err := built.journal.Receive(t.Context(), 1, closedWait); !errors.Is(err, processing.ErrFeedClosed) {
		t.Errorf("Receive from the journal of a failed assemble = %v, want it closed", err)
	}
}

const closedWait = time.Second
