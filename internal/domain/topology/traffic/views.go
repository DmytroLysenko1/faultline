package traffic

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type SubjectRED struct {
	Live  red.WindowStats
	Final red.WindowStats
}

type View struct {
	Nodes      []SubjectRED
	Edges      []SubjectRED
	WindowSize time.Duration
	Watermark  stream.Mark
}

type views struct {
	nodes   []SubjectRED
	edges   []SubjectRED
	changed bool
}

func newViews(maxNodes, maxEdges int) views {
	return views{
		nodes: make([]SubjectRED, 0, maxNodes),
		edges: make([]SubjectRED, 0, maxEdges),
	}
}

func (v *views) of(key subjectKey) *SubjectRED {
	if key.Kind == SubjectNode {
		v.nodes = extended(v.nodes, int(key.Subject))
		return &v.nodes[key.Subject]
	}
	v.edges = extended(v.edges, int(key.Subject))
	return &v.edges[key.Subject]
}

func (v *views) offerLive(target *SubjectRED, stats red.WindowStats) {
	if !target.Live.IsZero() && stats.StartUnixNano < target.Live.StartUnixNano {
		return
	}
	v.replace(&target.Live, stats)
}

func (v *views) offerFinal(target *SubjectRED, stats red.WindowStats) {
	if newer(stats, target.Final) {
		v.replace(&target.Final, stats)
	}
	if stats.Kind == red.PaneOnTime && target.Live.StartUnixNano == stats.StartUnixNano {
		v.replace(&target.Live, red.WindowStats{})
	}
}

func (v *views) replace(slot *red.WindowStats, stats red.WindowStats) {
	if *slot == stats {
		return
	}
	*slot = stats
	v.changed = true
}

func newer(candidate, current red.WindowStats) bool {
	if candidate.StartUnixNano != current.StartUnixNano {
		return candidate.StartUnixNano > current.StartUnixNano
	}
	return candidate.Revision > current.Revision
}

func (t *Traffic) Changed() bool {
	return t.views.changed
}

func (t *Traffic) View() View {
	t.views.changed = false
	return View{
		Nodes:      t.views.nodes,
		Edges:      t.views.edges,
		WindowSize: t.limits.Windows.Size,
		Watermark:  t.watermark.Current,
	}
}
