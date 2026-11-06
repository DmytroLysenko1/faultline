package recovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/recovery"
)

const (
	method      = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
	panicSecret = "SECRET-PANIC-VALUE"
)

func call(t *testing.T, logs *bytes.Buffer, handler grpc.UnaryHandler) (any, error) {
	t.Helper()
	interceptor := recovery.UnaryInterceptor(slog.New(slog.NewJSONHandler(logs, nil)))
	return interceptor(t.Context(), "request", &grpc.UnaryServerInfo{
		FullMethod: method,
	}, handler)
}

func TestPanicBecomesInternalWithoutThePanicValue(t *testing.T) {
	var logs bytes.Buffer
	response, err := call(t, &logs, func(context.Context, any) (any, error) {
		panic(panicSecret)
	})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v, want %v", got, codes.Internal)
	}
	if strings.Contains(err.Error(), panicSecret) {
		t.Errorf("client message %q leaks the panic value", err.Error())
	}
	if response != nil {
		t.Errorf("response = %v, want nil after a panic", response)
	}
	var record struct {
		Message string `json:"msg"`
		Level   string `json:"level"`
		Method  string `json:"method"`
		Panic   string `json:"panic"`
		Stack   string `json:"stack"`
	}
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatalf("log is not one JSON record: %v\n%s", err, logs.String())
	}
	if record.Message != "rpc.panic" || record.Level != "ERROR" || record.Method != method || record.Panic != panicSecret {
		t.Errorf("log record = %+v, want an ERROR rpc.panic with method and panic value", record)
	}
	if !strings.Contains(record.Stack, "recovery_test.go") {
		t.Errorf("stack %q does not point at the panicking handler", record.Stack)
	}
}

func TestHandlerResultPassesThroughUnchanged(t *testing.T) {
	var logs bytes.Buffer
	response, err := call(t, &logs, func(context.Context, any) (any, error) {
		return "served", nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if response != "served" {
		t.Errorf("response = %v, want the handler result", response)
	}
	if logs.Len() != 0 {
		t.Errorf("a call without panic logged %s", logs.String())
	}
}
