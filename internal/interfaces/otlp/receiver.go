package otlp

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/spanmap"
)

type spanAcceptor interface {
	Execute(ctx context.Context, spans ingest.SpanSource) (ingest.AcceptResult, error)
}

type traceReceiver struct {
	ptraceotlp.UnimplementedGRPCServer
	acceptor spanAcceptor
	events   *events
}

func (r *traceReceiver) Export(
	ctx context.Context,
	request ptraceotlp.ExportRequest,
) (ptraceotlp.ExportResponse, error) {
	result, err := r.acceptor.Execute(ctx, spanmap.NewSource(request.Traces()))
	if err != nil {
		return ptraceotlp.NewExportResponse(), r.fail(ctx, err)
	}
	r.events.accepted(ctx, result)
	return responseOf(result), nil
}

func (r *traceReceiver) fail(ctx context.Context, err error) error {
	outcome := failureOf(err)
	outcome.report(r.events, ctx, err)
	return outcome.status.Err()
}
