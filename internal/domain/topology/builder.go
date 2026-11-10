package topology

import (
	"fmt"
	"slices"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
)

type Limits struct {
	PairingTTL    time.Duration
	MaxPending    int
	TombstoneTTL  time.Duration
	MaxTombstones int
	MaxNodes      int
	MaxEdges      int
}

type Builder struct {
	Matcher *matcher.Matcher
	Graph   *graph.Graph
}

func NewBuilder(limits Limits, startedAt time.Time) (*Builder, error) {
	pairing, err := matcher.New(limits.matcherLimits())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	services, err := graph.New(limits.graphLimits(), startedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	return &Builder{
		Matcher: pairing,
		Graph:   services,
	}, nil
}

func (l Limits) matcherLimits() matcher.Limits {
	return matcher.Limits{
		TTL:           l.PairingTTL,
		MaxPending:    l.MaxPending,
		TombstoneTTL:  l.TombstoneTTL,
		MaxTombstones: l.MaxTombstones,
	}
}

func (l Limits) graphLimits() graph.Limits {
	return graph.Limits{
		MaxNodes: l.MaxNodes,
		MaxEdges: l.MaxEdges,
	}
}

func (b *Builder) Apply(received batch.Batch) {
	receivedAt, spans := received.ReceivedAt(), received.Spans()
	unanswered := b.Matcher.Expire(receivedAt)
	calls := make([]call.Call, 0, len(spans))
	for i := range spans {
		if matched, ok := b.Matcher.Match(&spans[i], receivedAt); ok {
			calls = append(calls, matched)
		}
	}
	b.Graph.Apply(&graph.Change{
		ReceivedAt: receivedAt,
		Services:   participants(spans),
		Calls:      calls,
		Unanswered: unanswered,
	})
}

func participants(spans []span.Span) []resource.ServiceRef {
	services := make([]resource.ServiceRef, 0, len(spans))
	for i := range spans {
		if _, side := role.Of(&spans[i]); side.Participates() {
			services = append(services, spans[i].Service)
		}
	}
	return services
}

func (b *Builder) Snapshot() *Snapshot {
	view := b.Graph.CurrentView()
	return &Snapshot{
		Revision:     view.Revision,
		GeneratedAt:  view.GeneratedAt,
		Nodes:        slices.Clone(view.Nodes),
		Edges:        slices.Clone(view.Edges),
		PairingStats: b.Matcher.Stats,
		GraphStats:   b.Graph.Stats,
	}
}
