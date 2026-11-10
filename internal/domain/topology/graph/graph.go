package graph

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

var ErrInvalidLimits = errors.New("invalid graph limits")

type Limits struct {
	MaxNodes int
	MaxEdges int
}

type edgeKey struct {
	Caller resource.ServiceRef
	Callee resource.ServiceRef
}

type View struct {
	Revision    uint64
	GeneratedAt time.Time
	Nodes       []Node
	Edges       []Edge
}

type Change struct {
	ReceivedAt time.Time
	Services   []resource.ServiceRef
	Calls      []call.Call
	Unanswered []call.Unanswered
	Served     []resource.ServiceRef
}

type Applied struct {
	CallEdges   []handle.Edge
	CallCallees []handle.Node
	ServedNodes []handle.Node
}

type Graph struct {
	Limits    Limits
	Nodes     map[resource.ServiceRef]*Node
	Edges     map[edgeKey]*Edge
	Stats     Stats
	Dirty     bool
	ChangedAt time.Time
	Clock     clock.Monotonic
	View      View
	applied   Applied
	nextNode  handle.Node
	nextEdge  handle.Edge
}

func New(limits Limits, startedAt time.Time) (*Graph, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	return &Graph{
		Limits: limits,
		Nodes:  make(map[resource.ServiceRef]*Node),
		Edges:  make(map[edgeKey]*Edge),
		View: View{
			GeneratedAt: startedAt,
		},
	}, nil
}

func (l Limits) validate() error {
	switch {
	case l.MaxNodes <= 0 || l.MaxNodes > math.MaxInt32:
		return fmt.Errorf("%w: max nodes %d must be in 1 … %d", ErrInvalidLimits, l.MaxNodes, math.MaxInt32)
	case l.MaxEdges <= 0 || l.MaxEdges > math.MaxInt32:
		return fmt.Errorf("%w: max edges %d must be in 1 … %d", ErrInvalidLimits, l.MaxEdges, math.MaxInt32)
	}
	return nil
}

func (g *Graph) Apply(change *Change) Applied {
	g.Clock.Advance(change.ReceivedAt)
	g.resetApplied()
	changed := g.addNodes(change.Services)
	changed = g.addCalls(change.Calls) || changed
	changed = g.addServed(change.Served) || changed
	changed = g.addUnanswered(change.Unanswered) || changed
	if changed {
		g.Dirty = true
		g.ChangedAt = g.Clock.Now()
	}
	return g.applied
}

func (g *Graph) resetApplied() {
	g.applied.CallEdges = g.applied.CallEdges[:0]
	g.applied.CallCallees = g.applied.CallCallees[:0]
	g.applied.ServedNodes = g.applied.ServedNodes[:0]
}

func (g *Graph) CurrentView() View {
	if !g.Dirty {
		return g.View
	}
	g.View = View{
		Revision:    g.View.Revision + 1,
		GeneratedAt: g.ChangedAt,
		Nodes:       sortedCopies(g.Nodes, CompareNodes),
		Edges:       sortedCopies(g.Edges, CompareEdges),
	}
	g.Dirty = false
	return g.View
}

func (g *Graph) addNodes(services []resource.ServiceRef) (changed bool) {
	for _, service := range services {
		changed = g.addNode(service) || changed
	}
	return changed
}

func (g *Graph) addNode(service resource.ServiceRef) (added bool) {
	if _, exists := g.Nodes[service]; exists {
		return false
	}
	if len(g.Nodes) >= g.Limits.MaxNodes {
		g.Stats.NodeOverflow++
		return false
	}
	created := NewNode(service)
	created.Handle = g.nextNode
	g.nextNode++
	g.Nodes[service] = &created
	return true
}

func (g *Graph) addCalls(calls []call.Call) (changed bool) {
	for i := range calls {
		edge, callee := g.recordCall(&calls[i])
		g.applied.CallEdges = append(g.applied.CallEdges, edge)
		g.applied.CallCallees = append(g.applied.CallCallees, callee)
		changed = changed || edge != handle.None
	}
	return changed
}

func (g *Graph) recordCall(matched *call.Call) (handle.Edge, handle.Node) {
	edge := g.edgeFor(matched)
	if edge == nil {
		g.Stats.GraphOverflow++
		return handle.None, handle.None
	}
	edge.Record(matched)
	return edge.Handle, g.Nodes[matched.Callee].Handle
}

func (g *Graph) addServed(served []resource.ServiceRef) (changed bool) {
	for _, service := range served {
		node, exists := g.Nodes[service]
		if !exists {
			g.applied.ServedNodes = append(g.applied.ServedNodes, handle.None)
			continue
		}
		node.RecordServed()
		g.applied.ServedNodes = append(g.applied.ServedNodes, node.Handle)
		changed = true
	}
	return changed
}

func (g *Graph) edgeFor(matched *call.Call) *Edge {
	key := edgeKey{
		Caller: matched.Caller,
		Callee: matched.Callee,
	}
	if edge, exists := g.Edges[key]; exists {
		return edge
	}
	caller, callerExists := g.Nodes[matched.Caller]
	callee, calleeExists := g.Nodes[matched.Callee]
	if !callerExists || !calleeExists || len(g.Edges) >= g.Limits.MaxEdges {
		return nil
	}
	edge := &Edge{
		Source: caller.ID,
		Target: callee.ID,
		Handle: g.nextEdge,
	}
	g.nextEdge++
	g.Edges[key] = edge
	return edge
}

func (g *Graph) addUnanswered(unanswered []call.Unanswered) (changed bool) {
	for _, expired := range unanswered {
		node, exists := g.Nodes[expired.Service]
		if !exists {
			g.Stats.UnansweredOverflow++
			continue
		}
		node.RecordUnanswered(expired)
		changed = true
	}
	return changed
}

func sortedCopies[K comparable, V any](items map[K]*V, compare func(a, b *V) int) []V {
	sorted := slices.SortedFunc(maps.Values(items), compare)
	return lo.Map(sorted, func(item *V, _ int) V {
		return *item
	})
}
