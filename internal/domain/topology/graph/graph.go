package graph

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
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
	case l.MaxNodes <= 0:
		return fmt.Errorf("%w: max nodes %d must be positive", ErrInvalidLimits, l.MaxNodes)
	case l.MaxEdges <= 0:
		return fmt.Errorf("%w: max edges %d must be positive", ErrInvalidLimits, l.MaxEdges)
	}
	return nil
}

func (g *Graph) Apply(change *Change) {
	g.Clock.Advance(change.ReceivedAt)
	changed := g.addNodes(change.Services)
	changed = g.addCalls(change.Calls) || changed
	changed = g.addUnanswered(change.Unanswered) || changed
	if changed {
		g.Dirty = true
		g.ChangedAt = g.Clock.Now()
	}
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
	node := NewNode(service)
	g.Nodes[service] = &node
	return true
}

func (g *Graph) addCalls(calls []call.Call) (changed bool) {
	for _, matched := range calls {
		edge := g.edgeFor(matched)
		if edge == nil {
			g.Stats.GraphOverflow++
			continue
		}
		edge.Record(matched)
		changed = true
	}
	return changed
}

func (g *Graph) edgeFor(matched call.Call) *Edge {
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
	}
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

func sortedCopies[K comparable, V any](items map[K]*V, compare func(a, b V) int) []V {
	copies := lo.MapToSlice(items, func(_ K, item *V) V {
		return *item
	})
	slices.SortFunc(copies, compare)
	return copies
}
