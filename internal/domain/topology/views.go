package topology

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

type NodeView struct {
	graph.Node
	Live  red.WindowStats
	Final red.WindowStats
}

type EdgeView struct {
	graph.Edge
	Live  red.WindowStats
	Final red.WindowStats
}

func nodeViews(nodes []graph.Node, summaries []traffic.SubjectRED) []NodeView {
	views := make([]NodeView, len(nodes))
	for i := range nodes {
		summary := summaryOf(summaries, int(nodes[i].Handle))
		views[i] = NodeView{
			Node:  nodes[i],
			Live:  summary.Live,
			Final: summary.Final,
		}
	}
	return views
}

func edgeViews(edges []graph.Edge, summaries []traffic.SubjectRED) []EdgeView {
	views := make([]EdgeView, len(edges))
	for i := range edges {
		summary := summaryOf(summaries, int(edges[i].Handle))
		views[i] = EdgeView{
			Edge:  edges[i],
			Live:  summary.Live,
			Final: summary.Final,
		}
	}
	return views
}

func summaryOf(summaries []traffic.SubjectRED, handle int) traffic.SubjectRED {
	if handle >= len(summaries) {
		return traffic.SubjectRED{}
	}
	return summaries[handle]
}
