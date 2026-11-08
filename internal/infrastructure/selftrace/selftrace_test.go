package selftrace_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
)

const (
	loopback      = "127.0.0.1:0"
	network       = "tcp"
	flushBudget   = 200 * time.Millisecond
	returnBudget  = time.Second
	spanName      = "faultline.v1.GraphService/GetGraph"
	tracerName    = "selftrace_test"
	serviceKey    = "service.name"
	exportFailure = "self_tracing.export_failed"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type logBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buffer.Write(p)
}

func (b *logBuffer) count(t *testing.T, name string) int {
	t.Helper()
	b.lock.Lock()
	defer b.lock.Unlock()
	matched := 0
	scanner := bufio.NewScanner(bytes.NewReader(b.buffer.Bytes()))
	for scanner.Scan() {
		record := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if record["msg"] == name {
			matched++
		}
	}
	return matched
}

type collector struct {
	ptraceotlp.UnimplementedGRPCServer
	lock     sync.Mutex
	received []exported
}

type exported struct {
	Service string
	Span    string
}

func (c *collector) Export(_ context.Context, request ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	resources := request.Traces().ResourceSpans()
	for index := range resources.Len() {
		resource := resources.At(index)
		service, _ := resource.Resource().Attributes().Get(serviceKey)
		spans := resource.ScopeSpans().At(0).Spans()
		for spanIndex := range spans.Len() {
			c.received = append(c.received, exported{
				Service: service.Str(),
				Span:    spans.At(spanIndex).Name(),
			})
		}
	}
	return ptraceotlp.NewExportResponse(), nil
}

func (c *collector) spans() []exported {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.received
}

func serveCollector(t *testing.T) (*collector, string) {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), network, loopback)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	server := grpc.NewServer()
	received := &collector{}
	ptraceotlp.RegisterGRPCServer(server, received)
	var serving sync.WaitGroup
	serving.Go(func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Serve: %v", err)
		}
	})
	t.Cleanup(func() {
		server.Stop()
		serving.Wait()
	})
	return received, listener.Addr().String()
}

func closedAddress(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	probe, err := config.Listen(t.Context(), network, loopback)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	return address
}

func emitSpan(ctx context.Context, tracing *selftrace.Tracing) {
	_, span := tracing.TracerProvider().Tracer(tracerName).Start(ctx, spanName)
	span.End()
}

func shutdownWithin(t *testing.T, tracing *selftrace.Tracing) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), flushBudget)
	defer cancel()
	started := time.Now()
	if err := tracing.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown = %v, want nil or a deadline", err)
	}
	return time.Since(started)
}

func TestDisabledTracingRecordsNothing(t *testing.T) {
	tracing := selftrace.Disabled()
	_, span := tracing.TracerProvider().Tracer(tracerName).Start(t.Context(), spanName)
	defer span.End()
	if span.IsRecording() {
		t.Error("disabled tracing records spans")
	}
	if err := tracing.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown = %v, want nil", err)
	}
}

func TestShutdownFlushesSpansToTheCollector(t *testing.T) {
	received, address := serveCollector(t)
	tracing, err := selftrace.New(t.Context(), address, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	emitSpan(t.Context(), tracing)
	if err := tracing.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	want := []exported{
		{
			Service: "faultline",
			Span:    spanName,
		},
	}
	if diff := cmp.Diff(want, received.spans()); diff != "" {
		t.Errorf("exported spans (-want +got):\n%s", diff)
	}
}

func TestUnreachableCollectorIsLoggedAndDoesNotHoldShutdown(t *testing.T) {
	logs := &logBuffer{}
	tracing, err := selftrace.New(t.Context(), closedAddress(t), slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	emitSpan(t.Context(), tracing)
	if took := shutdownWithin(t, tracing); took > returnBudget {
		t.Errorf("Shutdown took %v, want at most %v", took, returnBudget)
	}
	if got := logs.count(t, exportFailure); got == 0 {
		t.Errorf("no %s record for an unreachable collector", exportFailure)
	}
	goleak.VerifyNone(t)
}
