package graphmap

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

var (
	ErrDanglingEdge = errors.New("edge points to a missing node")
	ErrNilSnapshot  = errors.New("nil snapshot")
)

func Graph(snapshot *topology.Snapshot) (*faultlinev1.Graph, error) {
	if snapshot == nil {
		return nil, ErrNilSnapshot
	}
	mapped, err := edges(snapshot.Edges, indexByID(snapshot.Nodes))
	if err != nil {
		return nil, err
	}
	return &faultlinev1.Graph{
		Revision:    snapshot.Revision,
		GeneratedAt: timestamppb.New(snapshot.GeneratedAt),
		Nodes:       nodes(snapshot.Nodes),
		Edges:       mapped,
		WindowSize:  durationpb.New(snapshot.WindowSize),
		Watermark:   watermark(snapshot),
	}, nil
}

func indexByID(nodes []topology.NodeView) map[string]uint32 {
	positions := make(map[string]uint32, len(nodes))
	var position uint32
	for i := range nodes {
		positions[nodes[i].ID] = position
		position++
	}
	return positions
}

func nodes(source []topology.NodeView) []*faultlinev1.Node {
	mapped := make([]*faultlinev1.Node, 0, len(source))
	for i := range source {
		mapped = append(mapped, node(&source[i]))
	}
	return mapped
}

func node(source *topology.NodeView) *faultlinev1.Node {
	return &faultlinev1.Node{
		Id: source.ID,
		Service: &faultlinev1.ServiceRef{
			Namespace: source.Service.Namespace,
			Name:      source.Service.Name,
		},
		UnansweredCalls:       source.UnansweredCalls,
		UnansweredFailedCalls: source.UnansweredFailedCalls,
		ServedRequests:        source.Served,
		Live:                  windowStats(&source.Live),
		Final:                 windowStats(&source.Final),
	}
}

func edges(source []topology.EdgeView, positions map[string]uint32) ([]*faultlinev1.Edge, error) {
	mapped := make([]*faultlinev1.Edge, 0, len(source))
	for i := range source {
		edge := &source[i]
		caller, callerFound := positions[edge.Source]
		callee, calleeFound := positions[edge.Target]
		if !callerFound || !calleeFound {
			return nil, fmt.Errorf("%w: %s → %s", ErrDanglingEdge, edge.Source, edge.Target)
		}
		mapped = append(mapped, &faultlinev1.Edge{
			SourceIndex: caller,
			TargetIndex: callee,
			Calls:       edge.Calls,
			FailedCalls: edge.FailedCalls,
			Live:        windowStats(&edge.Live),
			Final:       windowStats(&edge.Final),
		})
	}
	return mapped, nil
}
