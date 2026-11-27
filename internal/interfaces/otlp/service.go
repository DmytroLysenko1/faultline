package otlp

import (
	"context"
	"net"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
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
	gate      *Tap
	admission *admission.Gate
	receiver  traceExporter
	events    *events
	holdLimit time.Duration
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
	verdict, err := g.gate.enforce(ctx)
	defer verdict.stop()
	if err != nil {
		return nil, err
	}
	release, err := g.admission.Enter(verdict.principal.TokenID, remoteOf(ctx))
	if err != nil {
		return nil, g.refuse(ctx, verdict.principal, err)
	}
	defer release()
	request, err := g.receive(ctx, verdict, decode)
	if err != nil {
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

func (g guardedExport) receive(ctx context.Context, held verdict, decode func(any) error) (ptraceotlp.ExportRequest, error) {
	watchdog := time.AfterFunc(g.holdLimit, func() {
		held.cancel(admission.ErrSlotHeldTooLong)
	})
	defer watchdog.Stop()
	request := ptraceotlp.NewExportRequest()
	err := decode(request)
	if !watchdog.Stop() {
		g.admission.Admission.SlotHeldTooLong()
		return request, g.refuse(ctx, held.principal, admission.ErrSlotHeldTooLong)
	}
	return request, err
}

func (g guardedExport) refuse(ctx context.Context, principal ingestauth.Principal, err error) error {
	outcome := admissionFailureOf(err)
	outcome.report(g.events, ctx, principal.TokenID, err)
	return outcome.status.Err()
}

func remoteOf(ctx context.Context) net.Addr {
	remote, found := peer.FromContext(ctx)
	if !found {
		return nil
	}
	return remote.Addr
}
