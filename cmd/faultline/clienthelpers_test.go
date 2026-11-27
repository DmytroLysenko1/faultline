package main

import (
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	loopback        = "127.0.0.1:0"
	attrServiceName = "service.name"
	callerSpan      = 1
	calleeSpan      = 2
	pollInterval    = 10 * time.Millisecond
	visibleDeadline = 20 * time.Second
	healthPath      = "/healthz"
)

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
	conn, err := grpc.NewClient("passthrough:///"+address,
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
	created.SetSpanID(pcommon.SpanID{spanID})
	if parentID != 0 {
		created.SetParentSpanID(pcommon.SpanID{parentID})
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
