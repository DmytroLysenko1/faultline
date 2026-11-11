package connectapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/limit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/recovery"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/timeout"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/tracing"
)

const (
	RequestTimeout          = 30 * time.Second
	SendTimeout             = 30 * time.Second
	SendMaxBytes            = 192 << 20
	ReadMaxBytes            = 1 << 20
	MaxConcurrentGetGraph   = 4
	MaxConcurrentReflection = 2
	DefaultResponseBudget   = 256 << 20

	tracerName = "github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

type Options struct {
	Timeout                 time.Duration
	SendTimeout             time.Duration
	SendMaxBytes            int
	ReadMaxBytes            int
	MaxConcurrent           int64
	MaxConcurrentReflection int64
	ResponseBudget          int64
	Logger                  *slog.Logger
	TracerProvider          trace.TracerProvider
}

type reflectionHandler func(*grpcreflect.Reflector, ...connect.HandlerOption) (string, http.Handler)

func DefaultOptions(logger *slog.Logger) Options {
	return Options{
		Timeout:                 RequestTimeout,
		SendTimeout:             SendTimeout,
		SendMaxBytes:            SendMaxBytes,
		ReadMaxBytes:            ReadMaxBytes,
		MaxConcurrent:           MaxConcurrentGetGraph,
		MaxConcurrentReflection: MaxConcurrentReflection,
		ResponseBudget:          DefaultResponseBudget,
		Logger:                  logger,
		TracerProvider:          noop.NewTracerProvider(),
	}
}

func NewMux(graphs faultlinev1connect.GraphServiceHandler, options *Options) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	path, service := faultlinev1connect.NewGraphServiceHandler(graphs, graphOptions(options)...)
	mux.Handle(path, service)
	admitted, err := graphservice.NewAdmission(options.ResponseBudget, service)
	if err != nil {
		return nil, fmt.Errorf("admit %s: %w", faultlinev1connect.GraphServiceGetGraphProcedure, err)
	}
	err = mountLimited(mux, faultlinev1connect.GraphServiceGetGraphProcedure, admitted, options.MaxConcurrent, options)
	if err != nil {
		return nil, err
	}
	if err := mountReflection(mux, options); err != nil {
		return nil, err
	}
	return mux, nil
}

func mountReflection(mux *http.ServeMux, options *Options) error {
	reflector := grpcreflect.NewStaticReflector(faultlinev1connect.GraphServiceName)
	versions := []reflectionHandler{
		grpcreflect.NewHandlerV1,
		grpcreflect.NewHandlerV1Alpha,
	}
	for _, version := range versions {
		path, handler := version(reflector, reflectionOptions(options)...)
		if err := mountLimited(mux, path, handler, options.MaxConcurrentReflection, options); err != nil {
			return err
		}
	}
	return nil
}

func mountLimited(mux *http.ServeMux, pattern string, next http.Handler, maxConcurrent int64, options *Options) error {
	limited, err := limit.New(next, maxConcurrent, options.Timeout+options.SendTimeout, options.Logger)
	if err != nil {
		return fmt.Errorf("limit %s: %w", pattern, err)
	}
	mux.Handle(pattern, limited)
	return nil
}

func graphOptions(options *Options) []connect.HandlerOption {
	return []connect.HandlerOption{
		connect.WithInterceptors(
			tracing.Interceptor(options.TracerProvider.Tracer(tracerName)),
			timeout.Interceptor(options.Timeout),
		),
		connect.WithSendMaxBytes(options.SendMaxBytes),
		connect.WithReadMaxBytes(options.ReadMaxBytes),
		connect.WithRecover(recovery.Handler(options.Logger)),
	}
}

func reflectionOptions(options *Options) []connect.HandlerOption {
	return []connect.HandlerOption{
		connect.WithReadMaxBytes(options.ReadMaxBytes),
		connect.WithRecover(recovery.Handler(options.Logger)),
	}
}
