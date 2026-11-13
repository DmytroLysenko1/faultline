package graph

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func (g *Graph) WriteCheckpoint(w *checkpoint.Writer) {
	w.Uint(g.Stats.GraphOverflow)
	w.Uint(g.Stats.UnansweredOverflow)
	w.Uint(g.Stats.NodeOverflow)
	w.Bool(g.Dirty)
	w.Time(g.ChangedAt)
	g.Clock.WriteCheckpoint(w)
	w.Uint(g.View.Revision)
	w.Time(g.View.GeneratedAt)
	g.writeNodes(w)
	g.writeEdges(w)
}

func (g *Graph) writeNodes(w *checkpoint.Writer) {
	byHandle := make([]*Node, len(g.Nodes))
	for _, node := range g.Nodes {
		byHandle[node.Handle] = node
	}
	w.Count(len(byHandle))
	for _, node := range byHandle {
		node.Service.WriteCheckpoint(w)
		w.Uint(node.UnansweredCalls)
		w.Uint(node.UnansweredFailedCalls)
		w.Uint(node.Served)
	}
}

func (g *Graph) writeEdges(w *checkpoint.Writer) {
	byHandle := make([]edgeKey, len(g.Edges))
	for key, edge := range g.Edges {
		byHandle[edge.Handle] = key
	}
	w.Count(len(byHandle))
	for _, key := range byHandle {
		edge := g.Edges[key]
		w.Count(int(g.Nodes[key.Caller].Handle))
		w.Count(int(g.Nodes[key.Callee].Handle))
		w.Uint(edge.Calls)
		w.Uint(edge.FailedCalls)
	}
}

func Restore(limits Limits, r *checkpoint.Reader) (*Graph, error) {
	restored, err := New(limits, time.Time{})
	if err != nil {
		return nil, err
	}
	restored.Stats = Stats{
		GraphOverflow:      r.Uint(),
		UnansweredOverflow: r.Uint(),
		NodeOverflow:       r.Uint(),
	}
	restored.Dirty = r.Bool()
	restored.ChangedAt = r.Time()
	restored.Clock = clock.Restore(r)
	restored.View.Revision = r.Uint()
	restored.View.GeneratedAt = r.Time()
	byHandle := restored.readNodes(r)
	restored.readEdges(r, byHandle)
	restored.restoreView()
	return restored, nil
}

func (g *Graph) readNodes(r *checkpoint.Reader) []*Node {
	byHandle := make([]*Node, r.Count(g.Limits.MaxNodes))
	for i := range byHandle {
		service := resource.ReadServiceRef(r)
		if _, repeated := g.Nodes[service]; repeated {
			r.Fail("a service node is repeated")
			return nil
		}
		node := NewNode(service)
		node.Handle = g.nextNode
		node.UnansweredCalls, node.UnansweredFailedCalls, node.Served = r.Uint(), r.Uint(), r.Uint()
		g.nextNode++
		g.Nodes[service], byHandle[i] = &node, &node
	}
	return byHandle
}

func (g *Graph) readEdges(r *checkpoint.Reader, byHandle []*Node) {
	count := r.Count(g.Limits.MaxEdges)
	for range count {
		g.readEdge(r, byHandle)
	}
}

func (g *Graph) readEdge(r *checkpoint.Reader, byHandle []*Node) {
	caller, callee := nodeAt(r, byHandle), nodeAt(r, byHandle)
	if r.Err() != nil {
		return
	}
	key := edgeKey{
		Caller: caller.Service,
		Callee: callee.Service,
	}
	if _, repeated := g.Edges[key]; repeated {
		r.Fail("an edge is repeated")
		return
	}
	g.Edges[key] = &Edge{
		Source:      caller.ID,
		Target:      callee.ID,
		Handle:      g.nextEdge,
		Calls:       r.Uint(),
		FailedCalls: r.Uint(),
	}
	g.nextEdge++
}

func nodeAt(r *checkpoint.Reader, byHandle []*Node) *Node {
	index := r.Index(len(byHandle))
	if r.Err() != nil {
		return nil
	}
	return byHandle[index]
}

func (g *Graph) restoreView() {
	if g.Dirty || g.View.Revision == 0 {
		return
	}
	g.View.Nodes = sortedCopies(g.Nodes, CompareNodes)
	g.View.Edges = sortedCopies(g.Edges, CompareEdges)
}
