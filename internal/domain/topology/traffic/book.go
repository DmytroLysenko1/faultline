package traffic

import (
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

type edgeEnds struct {
	Caller     resource.ServiceRef
	Callee     resource.ServiceRef
	CalleeNode handle.Node
}

type subjectBook struct {
	nodes []resource.ServiceRef
	edges []edgeEnds
}

func newSubjectBook(maxNodes, maxEdges int) subjectBook {
	return subjectBook{
		nodes: make([]resource.ServiceRef, 0, maxNodes),
		edges: make([]edgeEnds, 0, maxEdges),
	}
}

func (b *subjectBook) noteNode(node handle.Node, service resource.ServiceRef) {
	b.nodes = extended(b.nodes, int(node))
	b.nodes[node] = service
}

func (b *subjectBook) noteEdge(edge handle.Edge, ends edgeEnds) {
	b.edges = extended(b.edges, int(edge))
	b.edges[edge] = ends
}

func (b *subjectBook) owner(key operationKey) handle.Node {
	if key.Kind == SubjectNode {
		return handle.Node(key.Subject)
	}
	return b.edges[key.Subject].CalleeNode
}

func (b *subjectBook) series(kind SeriesKind, subject subjectKey) SeriesKey {
	key := SeriesKey{
		Kind:    kind,
		Subject: subject.Kind,
	}
	if subject.Kind == SubjectNode {
		key.Callee = b.nodes[subject.Subject]
		return key
	}
	ends := b.edges[subject.Subject]
	key.Caller, key.Callee = ends.Caller, ends.Callee
	return key
}

func (b *subjectBook) operationSeries(key operationKey, op operation.Operation) SeriesKey {
	series := b.series(SeriesOperation, key.subject())
	series.Protocol, series.Operation = op.Protocol, op.Name
	return series
}

func extended[T any](items []T, index int) []T {
	if index < len(items) {
		return items
	}
	if index >= cap(items) {
		panic(fmt.Sprintf("traffic: handle %d is past the limit of %d subjects", index, cap(items)))
	}
	return items[:index+1]
}
