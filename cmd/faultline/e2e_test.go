package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
)

const (
	loopback         = "127.0.0.1:0"
	attrServiceName  = "service.name"
	callerSpan       = 1
	calleeSpan       = 2
	drainedExports   = 1000
	pollInterval     = 10 * time.Millisecond
	visibleDeadline  = 5 * time.Second
	shutdownDeadline = 10 * time.Second
	acceptWindow     = 200 * time.Millisecond
	healthPath       = "/healthz"
)

var errFeedBroken = errors.New("feed broken")

type running struct {
	sys      *system
	open     listeners
	cancel   context.CancelFunc
	result   chan error
	exporter ptraceotlp.GRPCClient
	graphs   faultlinev1connect.GraphServiceClient
	http1    *http.Client
	h2c      *http.Client
}

func e2eConfig(t *testing.T) Config {
	t.Helper()
	return configWith(func(cfg *Config) {
		cfg.OTLPAddr = loopback
		cfg.APIAddr = loopback
	})
}

func launchSystem(t *testing.T, cfg *Config, sys *system) *running {
	t.Helper()
	open, err := listen(t.Context(), cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	started := &running{
		sys:    sys,
		open:   open,
		cancel: cancel,
		result: make(chan error, 1),
		http1:  &http.Client{},
		h2c:    h2cClient(t),
	}
	go func() {
		started.result <- run(ctx, cancel, sys, open, defaultShutdownBudget())
	}()
	started.exporter = ptraceotlp.NewGRPCClient(dialOTLP(t, open.OTLP.Addr().String()))
	started.graphs = faultlinev1connect.NewGraphServiceClient(started.h2c, apiURL(open), connect.WithGRPC())
	return started
}

func (r *running) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.result:
		return err
	case <-time.After(shutdownDeadline):
		t.Fatalf("run did not return within %v", shutdownDeadline)
		return nil
	}
}

func apiURL(open listeners) string {
	return "http://" + open.API.Addr().String()
}

func h2cClient(t *testing.T) *http.Client {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{
		Protocols: protocols,
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{
		Transport: transport,
	}
}

func dialOTLP(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return conn
}

func callRequest(trace uint32) ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()
	appendSpan(traces, processingtest.Caller, ptrace.SpanKindClient, trace, callerSpan, 0)
	appendSpan(traces, processingtest.Callee, ptrace.SpanKindServer, trace, calleeSpan, callerSpan)
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func appendSpan(traces ptrace.Traces, service string, kind ptrace.SpanKind, trace uint32, spanID, parentID byte) {
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(attrServiceName, service)
	created := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	created.SetTraceID(pcommon.TraceID{
		byte(trace), byte(trace >> 8), byte(trace >> 16), 1,
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

func calls(t *testing.T, graphs faultlinev1connect.GraphServiceClient) uint64 {
	t.Helper()
	response, err := graphs.GetGraph(t.Context(), connect.NewRequest(&faultlinev1.GetGraphRequest{}))
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	var total uint64
	for _, found := range response.Msg.GetGraph().GetEdges() {
		total += found.GetCalls()
	}
	return total
}

func eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(visibleDeadline)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s not reached within %v", what, visibleDeadline)
		}
		time.Sleep(pollInterval)
	}
}

func healthStatus(t *testing.T, client *http.Client, open listeners) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, apiURL(open)+healthPath, http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close body: %v", err)
	}
	return response.StatusCode
}

func gatedSystem(t *testing.T, cfg *Config, feedOf feedSource) *system {
	t.Helper()
	sys, err := assemble(cfg, testLogger(&logBuffer{}), wiring{
		feedOf:  feedOf,
		tracing: selftrace.Disabled(),
	})
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	return sys
}

func gated(gate <-chan struct{}) feedSource {
	return func(stored storage) spanFeed {
		return feedFunc(func(ctx context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error) {
			select {
			case <-gate:
				return stored.journal.Receive(ctx, limit, maxWait)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	}
}

func broken(stored storage) spanFeed {
	return feedFunc(func(context.Context, int, time.Duration) ([]processing.Delivery, error) {
		return nil, errFeedBroken
	})
}

func TestExportedCallIsVisibleAndStopsCleanly(t *testing.T) {
	cfg := e2eConfig(t)
	logs := &logBuffer{}
	sys, err := newSystem(t.Context(), &cfg, testLogger(logs))
	if err != nil {
		t.Fatalf("newSystem: %v", err)
	}
	started := launchSystem(t, &cfg, sys)
	if _, err := started.exporter.Export(t.Context(), callRequest(1)); err != nil {
		t.Fatalf("Export: %v", err)
	}
	eventually(t, "edge in GetGraph", func() bool {
		return calls(t, started.graphs) == 1
	})
	if err := started.stop(t); err != nil {
		t.Errorf("run = %v, want nil", err)
	}
	if diff := cmp.Diff(finalStats{Observed: 2, Matched: 1, Accepted: 2, Batches: 0, Balanced: true},
		finalStatsOf(t, logs)); diff != "" {
		t.Errorf("final pipeline.stats (-want +got):\n%s", diff)
	}
}

func TestSignalDrainsEveryAcceptedExport(t *testing.T) {
	cfg := e2eConfig(t)
	gate := make(chan struct{})
	started := launchSystem(t, &cfg, gatedSystem(t, &cfg, gated(gate)))
	for trace := range uint32(drainedExports) {
		if _, err := started.exporter.Export(t.Context(), callRequest(trace)); err != nil {
			t.Fatalf("Export %d: %v", trace, err)
		}
	}
	started.cancel()
	eventually(t, "healthz 503", func() bool {
		return healthStatus(t, started.http1, started.open) == http.StatusServiceUnavailable
	})
	close(gate)
	if err := started.stop(t); err != nil {
		t.Fatalf("run = %v, want nil", err)
	}
	snapshot, err := started.sys.store.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got := processingtest.TotalCalls(snapshot); got != drainedExports {
		t.Errorf("calls after drain = %d, want %d", got, drainedExports)
	}
}

func TestPipelineExitIsAFailure(t *testing.T) {
	cfg := e2eConfig(t)
	started := launchSystem(t, &cfg, gatedSystem(t, &cfg, broken))
	select {
	case err := <-started.result:
		if !errors.Is(err, errFeedBroken) || !errors.Is(err, errUnexpectedExit) {
			t.Errorf("run = %v, want errFeedBroken and errUnexpectedExit", err)
		}
	case <-time.After(shutdownDeadline):
		t.Fatalf("run did not return within %v", shutdownDeadline)
	}
}

func TestHealthIsOKWhileServing(t *testing.T) {
	cfg := e2eConfig(t)
	sys, err := newSystem(t.Context(), &cfg, testLogger(&logBuffer{}))
	if err != nil {
		t.Fatalf("newSystem: %v", err)
	}
	started := launchSystem(t, &cfg, sys)
	eventually(t, "serving", sys.lifecycle.Ready)
	got := map[string]int{
		"http1": healthStatus(t, started.http1, started.open),
		"h2c":   healthStatus(t, started.h2c, started.open),
	}
	want := map[string]int{
		"http1": http.StatusOK,
		"h2c":   http.StatusOK,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("healthz (-want +got):\n%s", diff)
	}
	if err := started.stop(t); err != nil {
		t.Errorf("run = %v, want nil", err)
	}
}

func TestSecondOTLPConnectionWaitsForTheFirst(t *testing.T) {
	cfg := e2eConfig(t)
	open, err := listen(t.Context(), &cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := errors.Join(open.OTLP.Close(), open.API.Close()); err != nil {
			t.Errorf("close listeners: %v", err)
		}
	})
	first := dialRaw(t, open)
	accepted, err := open.OTLP.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	dialRaw(t, open)
	second := make(chan net.Conn, 1)
	go func() {
		conn, err := open.OTLP.Accept()
		if err != nil {
			close(second)
			return
		}
		second <- conn
	}()
	select {
	case <-second:
		t.Fatal("second connection accepted while the first is open")
	case <-time.After(acceptWindow):
	}
	closeAll(t, accepted, first)
	select {
	case conn := <-second:
		closeAll(t, conn)
	case <-time.After(visibleDeadline):
		t.Fatal("second connection not accepted after the first closed")
	}
}

func dialRaw(t *testing.T, open listeners) net.Conn {
	t.Helper()
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), network, open.OTLP.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close conn: %v", err)
		}
	})
	return conn
}

func closeAll(t *testing.T, conns ...net.Conn) {
	t.Helper()
	for _, conn := range conns {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	}
}
