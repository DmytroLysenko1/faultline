package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"connectrpc.com/connect"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

var (
	ErrFetch         = errors.New("get graph")
	ErrWrite         = errors.New("write output")
	ErrMissingGraph  = errors.New("response carries no graph")
	ErrMalformedNode = errors.New("malformed node")
	ErrDanglingEdge  = errors.New("edge refers to a missing node")
)

type Renderer func(w io.Writer, graph *faultlinev1.Graph) error

type graphClient interface {
	GetGraph(
		ctx context.Context,
		req *connect.Request[faultlinev1.GetGraphRequest],
	) (*connect.Response[faultlinev1.GetGraphResponse], error)
}

func Export(ctx context.Context, graphs graphClient, renderer Renderer, out io.Writer) error {
	response, err := graphs.GetGraph(ctx, connect.NewRequest(&faultlinev1.GetGraphRequest{}))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFetch, err)
	}
	graph, err := validate(response.Msg)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFetch, err)
	}
	if err := renderer(out, graph); err != nil {
		return fmt.Errorf("%w: %w", ErrWrite, err)
	}
	return nil
}

func validate(response *faultlinev1.GetGraphResponse) (*faultlinev1.Graph, error) {
	graph := response.GetGraph()
	if graph == nil {
		return nil, ErrMissingGraph
	}
	if err := validateNodes(graph.GetNodes()); err != nil {
		return nil, err
	}
	if err := validateEdges(graph.GetEdges(), len(graph.GetNodes())); err != nil {
		return nil, err
	}
	return graph, nil
}

func validateNodes(nodes []*faultlinev1.Node) error {
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		if _, duplicated := seen[node.GetId()]; duplicated || node.GetId() == "" ||
			node.GetService() == nil {
			return fmt.Errorf("node %d %q: %w", index, node.GetId(), ErrMalformedNode)
		}
		seen[node.GetId()] = struct{}{}
	}
	return nil
}

func validateEdges(edges []*faultlinev1.Edge, nodes int) error {
	for index, edge := range edges {
		if int(edge.GetSourceIndex()) >= nodes || int(edge.GetTargetIndex()) >= nodes {
			return fmt.Errorf("edge %d (%d -> %d) of %d nodes: %w",
				index, edge.GetSourceIndex(), edge.GetTargetIndex(), nodes, ErrDanglingEdge)
		}
	}
	return nil
}
