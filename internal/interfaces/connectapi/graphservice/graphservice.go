package graphservice

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphmap"
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
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, context.Canceled)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
	default:
		h.Logger.ErrorContext(ctx, "api.get_graph_failed", slog.String("error", err.Error()))
		return connect.NewError(connect.CodeInternal, ErrInternal)
	}
}
