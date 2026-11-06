package query

import (
	"context"
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

type graphReader interface {
	Latest(ctx context.Context) (*topology.Snapshot, error)
}

type GetGraph struct {
	Graphs graphReader
}

func NewGetGraph(graphs graphReader) *GetGraph {
	return &GetGraph{
		Graphs: graphs,
	}
}

func (g *GetGraph) Execute(ctx context.Context) (*topology.Snapshot, error) {
	snapshot, err := g.Graphs.Latest(ctx)
	if err != nil {
		return nil, fmt.Errorf("get graph: %w", err)
	}
	return snapshot, nil
}
