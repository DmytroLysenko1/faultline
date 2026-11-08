package tracing_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/tracing"
)

const (
	traceparentHeader = "traceparent"
	remoteTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	remoteParent      = "00-" + remoteTraceID + "-00f067aa0ba902b7-01"
	tracerName        = "tracing_test"
)

var (
	errMissing = errors.New("graph missing")
	errBroken  = errors.New("graph broken")
	errPanic   = errors.New("handler panicked")
)

type graphHandler struct {
	faultlinev1connect.UnimplementedGraphServiceHandler
	answer func() (*connect.Response[faultlinev1.GetGraphResponse], error)
}

func (h graphHandler) GetGraph(
	context.Context,
	*connect.Request[faultlinev1.GetGraphRequest],
) (*connect.Response[faultlinev1.GetGraphResponse], error) {
	return h.answer()
}

func recoverPanic(context.Context, connect.Spec, http.Header, any) error {
	return connect.NewError(connect.CodeInternal, errPanic)
}

type span struct {
	Name        string
	Kind        trace.SpanKind
	HasParent   bool
	RemoteTrace bool
	Status      codes.Code
	Attributes  map[string]string
}

func serve(t *testing.T, recorder *tracetest.SpanRecorder, handler graphHandler) faultlinev1connect.GraphServiceClient {
	t.Helper()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("provider shutdown: %v", err)
		}
	})
	mux := http.NewServeMux()
	mux.Handle(faultlinev1connect.NewGraphServiceHandler(handler,
		connect.WithInterceptors(tracing.Interceptor(provider.Tracer(tracerName))),
		connect.WithRecover(recoverPanic),
	))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return faultlinev1connect.NewGraphServiceClient(server.Client(), server.URL)
}

func spanOf(t *testing.T, recorder *tracetest.SpanRecorder) span {
	t.Helper()
	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	attributes := map[string]string{}
	for _, attribute := range ended[0].Attributes() {
		attributes[string(attribute.Key)] = attribute.Value.String()
	}
	return span{
		Name:        ended[0].Name(),
		Kind:        ended[0].SpanKind(),
		HasParent:   ended[0].Parent().IsValid(),
		RemoteTrace: ended[0].SpanContext().TraceID().String() == remoteTraceID,
		Status:      ended[0].Status().Code,
		Attributes:  attributes,
	}
}

func procedureAttributes(errorCode string) map[string]string {
	attributes := map[string]string{
		"rpc.system":  "connect_rpc",
		"rpc.service": "faultline.v1.GraphService",
		"rpc.method":  "GetGraph",
	}
	if errorCode != "" {
		attributes["rpc.connect_rpc.error_code"] = errorCode
	}
	return attributes
}

func answering(response *connect.Response[faultlinev1.GetGraphResponse], err error) graphHandler {
	return graphHandler{
		answer: func() (*connect.Response[faultlinev1.GetGraphResponse], error) {
			return response, err
		},
	}
}

func TestInterceptorRecordsOneRootServerSpanPerCall(t *testing.T) {
	tests := []struct {
		Name string
		Args graphHandler
		Want span
	}{
		{
			Name: "success leaves the status unset",
			Args: answering(connect.NewResponse(&faultlinev1.GetGraphResponse{}), nil),
			Want: span{
				Attributes: procedureAttributes(""),
			},
		},
		{
			Name: "client error keeps the status unset and names the code",
			Args: answering(nil, connect.NewError(connect.CodeNotFound, errMissing)),
			Want: span{
				Attributes: procedureAttributes("not_found"),
			},
		},
		{
			Name: "invalid argument is the client's fault",
			Args: answering(nil, connect.NewError(connect.CodeInvalidArgument, errMissing)),
			Want: span{
				Attributes: procedureAttributes("invalid_argument"),
			},
		},
		{
			Name: "deadline exceeded is the server's fault",
			Args: answering(nil, connect.NewError(connect.CodeDeadlineExceeded, errBroken)),
			Want: span{
				Status:     codes.Error,
				Attributes: procedureAttributes("deadline_exceeded"),
			},
		},
		{
			Name: "unavailable is the server's fault",
			Args: answering(nil, connect.NewError(connect.CodeUnavailable, errBroken)),
			Want: span{
				Status:     codes.Error,
				Attributes: procedureAttributes("unavailable"),
			},
		},
		{
			Name: "server error marks the span failed",
			Args: answering(nil, connect.NewError(connect.CodeInternal, errBroken)),
			Want: span{
				Status:     codes.Error,
				Attributes: procedureAttributes("internal"),
			},
		},
		{
			Name: "panic recovered inside the handler marks the span failed",
			Args: graphHandler{
				answer: func() (*connect.Response[faultlinev1.GetGraphResponse], error) {
					panic(errBroken)
				},
			},
			Want: span{
				Status:     codes.Error,
				Attributes: procedureAttributes("internal"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			client := serve(t, recorder, tt.Args)
			request := connect.NewRequest(&faultlinev1.GetGraphRequest{})
			request.Header().Set(traceparentHeader, remoteParent)
			if _, err := client.GetGraph(t.Context(), request); err != nil && connect.CodeOf(err) == connect.CodeUnknown {
				t.Fatalf("GetGraph transport failure: %v", err)
			}
			want := tt.Want
			want.Name = "faultline.v1.GraphService/GetGraph"
			want.Kind = trace.SpanKindServer
			if diff := cmp.Diff(want, spanOf(t, recorder)); diff != "" {
				t.Errorf("span (-want +got):\n%s", diff)
			}
		})
	}
}
