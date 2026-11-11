package traffic

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type subjectSink struct {
	traffic *Traffic
}

func (s subjectSink) Emit(key subjectKey, pane stream.Pane, cell *red.Cell) {
	s.traffic.emitSubject(key, pane, cell)
}

func (s subjectSink) Retire(_ subjectKey, _ stream.Window, cell *red.Cell) {
	cell.Release(s.traffic.arena)
}

func (subjectSink) Closed(subjectKey) {}

type operationSink struct {
	traffic *Traffic
}

func (s operationSink) Emit(key operationKey, pane stream.Pane, counts *red.Counts) {
	t := s.traffic
	series := t.book.operationSeries(key, t.registry.Operation(key.Operation))
	t.final = Final{
		Series: series,
		Pane:   pane,
		Stats:  red.SummarizeCounts(pane, counts, t.observed(pane)),
	}
	t.emitFinal()
}

func (operationSink) Retire(operationKey, stream.Window, *red.Counts) {}

func (s operationSink) Closed(key operationKey) {
	s.traffic.registry.Release(s.traffic.book.owner(key), key.Operation)
}

type overflowSink struct {
	traffic *Traffic
}

func (s overflowSink) Emit(key subjectKey, pane stream.Pane, counts *red.Counts) {
	t := s.traffic
	t.final = Final{
		Series: t.book.series(SeriesOverflow, key),
		Pane:   pane,
		Stats:  red.SummarizeCounts(pane, counts, t.observed(pane)),
	}
	t.emitFinal()
}

func (overflowSink) Retire(subjectKey, stream.Window, *red.Counts) {}

func (overflowSink) Closed(subjectKey) {}
