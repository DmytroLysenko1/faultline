package ingest_test

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/journaltest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	bufferBytes        = 256 << 10
	journalIdleAfter   = 35 * time.Second
	roomyBytes         = 1 << 30
	roomyBatches       = 64
	journalLowerBound  = 48 << 20
	retentionSlack     = 1 << 20
	smallRequestSpans  = 3
	largeRequestSpans  = 10
	tinyJournalBytes   = 1 << 10
	journalBatchesUsed = 1
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fixedClock struct{}

func (fixedClock) Now() time.Time {
	return time.Unix(0, 0)
}

type pipeline struct {
	Journal *journaltest.Memory
	Client  ptraceotlp.GRPCClient
}

func startPipeline(t *testing.T, limits journaltest.Limits) pipeline {
	t.Helper()
	limits.IdleAfter = cmp.Or(limits.IdleAfter, journalIdleAfter)
	journal, err := journaltest.NewMemory(limits, fixedClock{})
	if err != nil {
		t.Fatalf("journaltest.NewMemory: %v", err)
	}
	settings := otlp.DefaultOptions(slog.New(slog.DiscardHandler), fixedClock{})
	accept := ingest.NewAcceptBatch(journal, operation.NewNormalizer(nil))
	server := otlp.NewServer(accept, otlp.Guards{
		Tap:  otlp.NewTap(otlptest.Admitting(), settings),
		Gate: otlptest.Gate(t, otlp.AdmissionLimits(otlp.MaxRequestsPerSecond)),
	}, settings)
	listener := bufconn.Listen(bufferBytes)
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
	return pipeline{
		Journal: journal,
		Client:  ptraceotlp.NewGRPCClient(dial(t, listener)),
	}
}

func dial(t *testing.T, listener *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		otlptest.WithTestToken(),
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

func requestOf(spanCount int, firstSpan int) ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "checkout")
	spans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	for index := range spanCount {
		otlptest.AppendNumberedSpan(spans, firstSpan+index)
	}
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func ptraceotlpRequest(traces ptrace.Traces) ptraceotlp.ExportRequest {
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func footprintOf(t *testing.T, request ptraceotlp.ExportRequest) int64 {
	t.Helper()
	probe := startPipeline(t, journaltest.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
	})
	if _, err := probe.Client.Export(t.Context(), request); err != nil {
		t.Fatalf("probe Export: %v", err)
	}
	return probe.Journal.Stats().Bytes
}

func hasRetryInfo(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if _, ok := detail.(*errdetails.RetryInfo); ok {
			return true
		}
	}
	return false
}

type rejection struct {
	Code      codes.Code
	RetryInfo bool
}

func rejectionOf(err error) rejection {
	return rejection{
		Code:      status.Code(err),
		RetryInfo: hasRetryInfo(err),
	}
}
