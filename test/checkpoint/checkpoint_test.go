package checkpoint_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/application/query"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	bufferBytes     = 256 << 10
	journalBytes    = 64 << 20
	journalBatches  = 64
	callerSpan      = 1
	calleeSpan      = 2
	pollInterval    = 10 * time.Millisecond
	visibleDeadline = 5 * time.Second
	attrServiceName = "service.name"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fixedClock struct{}

func (fixedClock) Now() time.Time {
	return time.Unix(0, 0)
}

type system struct {
	journal  *memjournal.Journal
	otlp     *grpc.Server
	serving  sync.WaitGroup
	pipeline sync.WaitGroup
	runErr   error
	drain    func() error
	exporter ptraceotlp.GRPCClient
	graphs   faultlinev1connect.GraphServiceClient
}

func start(t *testing.T) *system {
	t.Helper()
	journal, err := memjournal.New(memjournal.Limits{
		MaxBytes:   journalBytes,
		MaxBatches: journalBatches,
	})
	if err != nil {
		t.Fatalf("memjournal.New: %v", err)
	}
	built := &system{
		journal: journal,
	}
	store := built.startPipeline(t)
	built.graphs = startAPI(t, store)
	built.exporter = built.startOTLP(t)
	built.drain = sync.OnceValue(built.stop)
	t.Cleanup(func() {
		if err := built.drain(); err != nil {
			t.Errorf("drain at cleanup: %v", err)
		}
	})
	return built
}

func (s *system) startPipeline(t *testing.T) *memgraph.Store {
	t.Helper()
	builder := processingtest.NewBuilder(t)
	store, err := memgraph.New(builder.Snapshot())
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	pipeline := processing.NewPipeline(s.journal, builder, store)
	s.pipeline.Go(func() {
		s.runErr = pipeline.Run(t.Context())
	})
	return store
}

func startAPI(t *testing.T, store *memgraph.Store) faultlinev1connect.GraphServiceClient {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	options := connectapi.DefaultOptions(logger)
	mux, err := connectapi.NewMux(graphservice.NewHandler(query.NewGetGraph(store), logger), options)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return faultlinev1connect.NewGraphServiceClient(server.Client(), server.URL, connect.WithGRPC())
}

func (s *system) startOTLP(t *testing.T) ptraceotlp.GRPCClient {
	t.Helper()
	settings := otlp.DefaultOptions(slog.New(slog.DiscardHandler), fixedClock{})
	s.otlp = otlp.NewServer(ingest.NewAcceptBatch(s.journal, fixedClock{}), settings)
	listener := bufconn.Listen(bufferBytes)
	s.serving.Go(func() {
		if err := s.otlp.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Serve: %v", err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return ptraceotlp.NewGRPCClient(conn)
}

func (s *system) stop() error {
	s.otlp.GracefulStop()
	s.serving.Wait()
	s.journal.Close()
	s.pipeline.Wait()
	return s.runErr
}

func callRequest() ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()
	appendSpan(traces, processingtest.Caller, ptrace.SpanKindClient, callerSpan, 0)
	appendSpan(traces, processingtest.Callee, ptrace.SpanKindServer, calleeSpan, callerSpan)
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func appendSpan(traces ptrace.Traces, service string, kind ptrace.SpanKind, spanID, parentID byte) {
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(attrServiceName, service)
	created := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	created.SetTraceID(pcommon.TraceID{
		1,
	})
	created.SetSpanID(pcommon.SpanID{
		spanID,
	})
	if parentID != 0 {
		created.SetParentSpanID(pcommon.SpanID{
			parentID,
		})
	}
	created.SetKind(kind)
}

type edge struct {
	Caller string
	Callee string
	Calls  uint64
}

func edgesOf(graph *faultlinev1.Graph) []edge {
	nodes := graph.GetNodes()
	var edges []edge
	for _, found := range graph.GetEdges() {
		edges = append(edges, edge{
			Caller: nodes[found.GetSourceIndex()].GetService().GetName(),
			Callee: nodes[found.GetTargetIndex()].GetService().GetName(),
			Calls:  found.GetCalls(),
		})
	}
	return edges
}

func waitForEdges(t *testing.T, graphs faultlinev1connect.GraphServiceClient) []edge {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), visibleDeadline)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		response, err := graphs.GetGraph(ctx, connect.NewRequest(&faultlinev1.GetGraphRequest{}))
		if err != nil {
			t.Fatalf("GetGraph while waiting %v for an edge: %v", visibleDeadline, err)
		}
		if edges := edgesOf(response.Msg.GetGraph()); len(edges) > 0 {
			return edges
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no edge visible within %v", visibleDeadline)
		case <-ticker.C:
		}
	}
}

func TestExportedCallReachesGetGraph(t *testing.T) {
	running := start(t)
	if _, err := running.exporter.Export(t.Context(), callRequest()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	want := []edge{
		{
			Caller: processingtest.Caller,
			Callee: processingtest.Callee,
			Calls:  1,
		},
	}
	if diff := cmp.Diff(want, waitForEdges(t, running.graphs)); diff != "" {
		t.Errorf("edges (-want +got):\n%s", diff)
	}
	if err := running.drain(); err != nil {
		t.Errorf("Run after drain = %v, want nil", err)
	}
}
