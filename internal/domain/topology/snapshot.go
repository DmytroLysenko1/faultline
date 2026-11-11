package topology

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

type Snapshot struct {
	Revision       uint64
	GeneratedAt    time.Time
	Nodes          []NodeView
	Edges          []EdgeView
	WindowSize     time.Duration
	Watermark      time.Time
	WatermarkKnown bool
	PairingStats   matcher.Stats
	GraphStats     graph.Stats
	TrafficStats   traffic.Stats
}

type published struct {
	revision    uint64
	generatedAt time.Time
	nodes       int
	edges       int
}

func (b *Builder) Snapshot() *Snapshot {
	graphChanged, summariesChanged := b.Graph.Dirty, b.Traffic.Changed()
	view, summaries := b.Graph.CurrentView(), b.Traffic.View()
	b.advance(&view, graphChanged, summariesChanged)
	return &Snapshot{
		Revision:       b.published.revision,
		GeneratedAt:    b.published.generatedAt,
		Nodes:          nodeViews(view.Nodes, summaries.Nodes),
		Edges:          edgeViews(view.Edges, summaries.Edges),
		WindowSize:     summaries.WindowSize,
		Watermark:      summaries.Watermark.Time,
		WatermarkKnown: summaries.Watermark.Known,
		PairingStats:   b.Matcher.Stats,
		GraphStats:     b.Graph.Stats,
		TrafficStats:   b.Traffic.Stats(),
	}
}

func (b *Builder) advance(view *graph.View, graphChanged, summariesChanged bool) {
	if !graphChanged && !summariesChanged {
		return
	}
	b.published = published{
		revision:    b.published.revision + 1,
		generatedAt: view.GeneratedAt,
		nodes:       len(view.Nodes),
		edges:       len(view.Edges),
	}
	if summariesChanged {
		b.published.generatedAt = later(view.GeneratedAt, b.Graph.Clock.Now())
	}
}

func later(first, second time.Time) time.Time {
	if second.After(first) {
		return second
	}
	return first
}
