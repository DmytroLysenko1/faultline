package otlp_test

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

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/test/bufconn"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	exportMethod = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	bufferBytes  = 256 << 10
	bufconnAddr  = "passthrough:///bufconn"
)

var errNotRaw = errors.New("raw codec carries only *rawMessage")

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fakeClock struct {
	lock sync.Mutex
	now  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.now
}

func (c *fakeClock) Advance(step time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.now = c.now.Add(step)
}

type logBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.buffer.Write(p)
}

func (l *logBuffer) Records(t *testing.T) []map[string]any {
	t.Helper()
	l.lock.Lock()
	defer l.lock.Unlock()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(l.buffer.Bytes()))
	for scanner.Scan() {
		record := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, scanner.Text())
		}
		records = append(records, record)
	}
	return records
}

func (l *logBuffer) Events(t *testing.T, name string) []map[string]any {
	t.Helper()
	var matched []map[string]any
	for _, record := range l.Records(t) {
		if record["msg"] == name {
			matched = append(matched, record)
		}
	}
	return matched
}

type acceptorFunc func(ctx context.Context, spans ingest.SpanSource) (ingest.AcceptResult, error)

func (f acceptorFunc) Execute(ctx context.Context, spans ingest.SpanSource) (ingest.AcceptResult, error) {
	return f(ctx, spans)
}

type harness struct {
	Client ptraceotlp.GRPCClient
	Conn   *grpc.ClientConn
	Logs   *logBuffer
	Clock  *fakeClock
}

func options(logs *logBuffer, clock *fakeClock) otlp.Options {
	return otlp.DefaultOptions(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})), clock)
}

func serve(t *testing.T, listener net.Listener, acceptor acceptorFunc, settings otlp.Options) {
	t.Helper()
	server := otlp.NewServer(acceptor, settings)
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
}

func start(t *testing.T, acceptor acceptorFunc) harness {
	t.Helper()
	logs := &logBuffer{}
	clock := &fakeClock{
		now: time.Unix(0, 0),
	}
	listener := bufconn.Listen(bufferBytes)
	serve(t, listener, acceptor, options(logs, clock))
	conn := dial(t, func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	})
	return harness{
		Client: ptraceotlp.NewGRPCClient(conn),
		Conn:   conn,
		Logs:   logs,
		Clock:  clock,
	}
}

func dial(t *testing.T, dialer func(context.Context, string) (net.Conn, error)) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(bufconnAddr,
		grpc.WithContextDialer(dialer),
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
	return conn
}

func returning(result ingest.AcceptResult, err error) acceptorFunc {
	return func(context.Context, ingest.SpanSource) (ingest.AcceptResult, error) {
		return result, err
	}
}

func requestOf(spanCount int) ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "checkout")
	spans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	for index := range spanCount {
		otlptest.AppendNumberedSpan(spans, index)
	}
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

type rawMessage struct {
	Wire []byte
}

type rawCodec struct{}

func (rawCodec) Name() string {
	return "proto"
}

func (rawCodec) Marshal(value any) (mem.BufferSlice, error) {
	message, ok := value.(*rawMessage)
	if !ok {
		return nil, errNotRaw
	}
	return mem.BufferSlice{
		mem.SliceBuffer(message.Wire),
	}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, value any) error {
	message, ok := value.(*rawMessage)
	if !ok {
		return errNotRaw
	}
	message.Wire = data.Materialize()
	return nil
}

func sendRaw(ctx context.Context, conn *grpc.ClientConn, wire []byte) error {
	return conn.Invoke(ctx, exportMethod, &rawMessage{
		Wire: wire,
	}, &rawMessage{}, grpc.ForceCodecV2(rawCodec{}))
}
