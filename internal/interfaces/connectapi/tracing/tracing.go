package tracing

import (
	"context"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	systemKey    = "rpc.system"
	serviceKey   = "rpc.service"
	methodKey    = "rpc.method"
	errorCodeKey = "rpc.connect_rpc.error_code"
	systemName   = "connect_rpc"
	separator    = "/"
)

func Interceptor(tracer trace.Tracer) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			name := strings.TrimPrefix(req.Spec().Procedure, separator)
			ctx, span := tracer.Start(ctx, name,
				trace.WithNewRoot(),
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(procedureAttributes(name)...),
			)
			defer span.End()
			response, err := next(ctx, req)
			recordFailure(span, err)
			return response, err
		}
	}
}

func procedureAttributes(name string) []attribute.KeyValue {
	service, method, _ := strings.Cut(name, separator)
	return []attribute.KeyValue{
		attribute.String(systemKey, systemName),
		attribute.String(serviceKey, service),
		attribute.String(methodKey, method),
	}
}

func recordFailure(span trace.Span, err error) {
	if err == nil {
		return
	}
	code := connect.CodeOf(err)
	span.SetAttributes(attribute.String(errorCodeKey, code.String()))
	if slices.Contains(serverFaults(), code) {
		span.SetStatus(codes.Error, code.String())
	}
}

func serverFaults() []connect.Code {
	return []connect.Code{
		connect.CodeUnknown,
		connect.CodeDeadlineExceeded,
		connect.CodeUnimplemented,
		connect.CodeInternal,
		connect.CodeUnavailable,
		connect.CodeDataLoss,
	}
}
