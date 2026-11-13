package topology

import (
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type Limits struct {
	PairingTTL              time.Duration
	MaxPending              int
	TombstoneTTL            time.Duration
	MaxTombstones           int
	MaxNodes                int
	MaxEdges                int
	Windows                 stream.Config
	EarlyFirePeriod         time.Duration
	MaxSeries               int
	MaxOperationsPerService int
	SketchEntries           int
}

type Builder struct {
	limits    Limits
	Matcher   *matcher.Matcher
	Graph     *graph.Graph
	Traffic   *traffic.Traffic
	sink      traffic.FinalSink
	change    graph.Change
	served    []int
	published published
}

func NewBuilder(limits *Limits, startedAt time.Time, options ...Option) (*Builder, error) {
	pairing, err := matcher.New(limits.matcherLimits())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	services, err := graph.New(limits.graphLimits(), startedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	built := &Builder{
		limits:  *limits,
		Matcher: pairing,
		Graph:   services,
		published: published{
			generatedAt: startedAt,
		},
	}
	for _, option := range options {
		option(built)
	}
	if built.Traffic, err = traffic.New(limits.trafficLimits(), built.sink); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	return built, nil
}

func (l *Limits) matcherLimits() matcher.Limits {
	return matcher.Limits{
		TTL:           l.PairingTTL,
		MaxPending:    l.MaxPending,
		TombstoneTTL:  l.TombstoneTTL,
		MaxTombstones: l.MaxTombstones,
	}
}

func (l *Limits) graphLimits() graph.Limits {
	return graph.Limits{
		MaxNodes: l.MaxNodes,
		MaxEdges: l.MaxEdges,
	}
}

func (l *Limits) trafficLimits() *traffic.Limits {
	return &traffic.Limits{
		Windows:         l.Windows,
		EarlyFirePeriod: l.EarlyFirePeriod,
		MaxNodes:        l.MaxNodes,
		MaxEdges:        l.MaxEdges,
		MaxSeries:       l.MaxSeries,
		MaxPerService:   l.MaxOperationsPerService,
		SketchEntries:   l.SketchEntries,
	}
}

func (b *Builder) Apply(received batch.Batch) {
	defer b.releaseChange()
	b.resetChange(received.ReceivedAt)
	for i := range received.Spans {
		b.observe(i, &received.Spans[i])
	}
	applied := b.Graph.Apply(&b.change)
	b.Traffic.Begin(received)
	for i, edge := range applied.CallEdges {
		b.Traffic.RecordCall(edge, applied.CallCallees[i], &b.change.Calls[i])
	}
	for i, node := range applied.ServedNodes {
		b.Traffic.RecordServed(node, &received.Spans[b.served[i]])
	}
	b.Traffic.End()
}

func (b *Builder) releaseChange() {
	clear(b.change.Services)
	clear(b.change.Calls)
	clear(b.change.Served)
	b.change.Unanswered = nil
}

func (b *Builder) resetChange(receivedAt time.Time) {
	b.change.ReceivedAt = receivedAt
	b.change.Unanswered = b.Matcher.Expire(receivedAt)
	b.change.Services = b.change.Services[:0]
	b.change.Calls = b.change.Calls[:0]
	b.change.Served = b.change.Served[:0]
	b.served = b.served[:0]
}

func (b *Builder) observe(index int, observed *span.Span) {
	_, side := role.Of(observed)
	if side.Participates() {
		b.change.Services = append(b.change.Services, observed.Service)
	}
	matched, outcome := b.Matcher.Match(observed, b.change.ReceivedAt)
	if outcome == matcher.OutcomeMatched {
		b.change.Calls = append(b.change.Calls, matched)
	}
	if outcome.Served(side) {
		b.change.Served = append(b.change.Served, observed.Service)
		b.served = append(b.served, index)
	}
}

func (b *Builder) Fire() {
	b.Traffic.Fire()
}

func (b *Builder) StructureChanged() bool {
	return len(b.Graph.Nodes) !=
		b.published.nodes ||
		len(b.Graph.Edges) !=
			b.published.edges
}
