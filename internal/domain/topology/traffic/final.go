package traffic

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type Final struct {
	Series SeriesKey
	Pane   stream.Pane
	Stats  red.WindowStats
}

type FinalSink interface {
	Final(final *Final)
}

type discard struct{}

func (discard) Final(*Final) {}

func (t *Traffic) emitSubject(key subjectKey, pane stream.Pane, cell *red.Cell) {
	stats := red.Summarize(t.arena, pane, cell, t.observed(pane))
	target := t.views.of(key)
	if pane.Kind == stream.PaneEarly {
		t.views.offerLive(target, stats)
		return
	}
	t.views.offerFinal(target, stats)
	t.final = Final{
		Series: t.book.series(SeriesSubject, key),
		Pane:   pane,
		Stats:  stats,
	}
	t.emitFinal()
}

func (t *Traffic) emitFinal() {
	t.counters.Finals++
	t.sink.Final(&t.final)
}

func (t *Traffic) observed(pane stream.Pane) time.Duration {
	if pane.Kind == stream.PaneEarly {
		reached := t.watermark.MaxReceivedAt.Time.Sub(pane.Window.Start)
		return min(max(reached, t.limits.EarlyFirePeriod), t.limits.Windows.Size)
	}
	from := pane.Window.Start
	if coverage := t.watermark.CoverageStart.Time; coverage.After(from) {
		from = coverage
	}
	return pane.Window.End.Sub(from)
}
