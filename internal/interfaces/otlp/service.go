package otlp

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
)

const (
	traceServiceName  = "opentelemetry.proto.collector.trace.v1.TraceService"
	exportMethodName  = "Export"
	exportFullMethod  = "/" + traceServiceName + "/" + exportMethodName
	traceServiceProto = "opentelemetry/proto/collector/trace/v1/trace_service.proto"
)

type traceExporter interface {
	Export(ctx context.Context, request ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error)
}

type guardedExport struct {
	gate     *Tap
	receiver traceExporter
}

func traceServiceDesc(export guardedExport) *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: traceServiceName,
		HandlerType: (*traceExporter)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: exportMethodName,
				Handler:    export.handle,
			},
		},
		Metadata: traceServiceProto,
	}
}

func (g guardedExport) handle(
	_ any,
	ctx context.Context,
	decode func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	if err := g.gate.enforce(ctx); err != nil {
		return nil, err
	}
	request := ptraceotlp.NewExportRequest()
	if err := decode(request); err != nil {
		return nil, err
	}
	info := &grpc.UnaryServerInfo{
		Server:     g.receiver,
		FullMethod: exportFullMethod,
	}
	return interceptor(ctx, request, info, func(ctx context.Context, _ any) (any, error) {
		return g.receiver.Export(ctx, request)
	})
}
