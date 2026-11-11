package graphservice

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphmap"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/limit"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

var ErrInternal = errors.New("internal error")

type graphQuery interface {
	Execute(ctx context.Context) (*topology.Snapshot, error)
}

type Handler struct {
	Query  graphQuery
	Logger *slog.Logger
}

func NewHandler(query graphQuery, logger *slog.Logger) *Handler {
	return &Handler{
		Query:  query,
		Logger: logger,
	}
}

func (h *Handler) GetGraph(
	ctx context.Context,
	_ *connect.Request[faultlinev1.GetGraphRequest],
) (*connect.Response[faultlinev1.GetGraphResponse], error) {
	snapshot, err := h.Query.Execute(ctx)
	if err != nil {
		return nil, h.connectError(ctx, err)
	}
	if snapshot == nil {
		return nil, h.connectError(ctx, graphmap.ErrNilSnapshot)
	}
	if err := reserve(ctx, len(snapshot.Nodes), len(snapshot.Edges)); err != nil {
		return nil, h.connectError(ctx, err)
	}
	graph, err := graphmap.Graph(snapshot)
	if err != nil {
		return nil, h.connectError(ctx, err)
	}
	return connect.NewResponse(&faultlinev1.GetGraphResponse{
		Graph: graph,
	}), nil
}

func (h *Handler) connectError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrBusy):
		return busy()
	case errors.Is(err, ErrTooLarge):
		return connect.NewError(connect.CodeFailedPrecondition, ErrTooLarge)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, context.Canceled)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
	default:
		h.Logger.ErrorContext(ctx, "api.get_graph_failed", slog.String("error", err.Error()))
		return connect.NewError(connect.CodeInternal, ErrInternal)
	}
}

func busy() error {
	failure := connect.NewError(connect.CodeResourceExhausted, ErrBusy)
	retry, err := connect.NewErrorDetail(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(limit.RetryDelay),
	})
	if err != nil {
		return connect.NewError(connect.CodeInternal, ErrInternal)
	}
	failure.AddDetail(retry)
	return failure
}
