package stream

func (w *Windows[K, C]) emitOnTime(position int) {
	b := &w.buckets[position]
	b.final = true
	window := w.cfg.window(b.windowID)
	for _, series := range b.members {
		s := w.slotAt(series, position)
		revision := s.finalize()
		w.stats.PanesOnTime++
		w.sink.Emit(w.series[series].key, Pane{
			Window:   window,
			Kind:     PaneOnTime,
			Revision: revision,
		}, &s.cell)
	}
}

func (w *Windows[K, C]) emitLate(ref slotRef) {
	s := w.slotAt(ref.series, int(ref.position))
	revision := s.finalize()
	w.stats.PanesLate++
	pane := Pane{
		Window:   w.cfg.window(w.buckets[ref.position].windowID),
		Kind:     PaneLate,
		Revision: revision,
	}
	w.sink.Emit(w.series[ref.series].key, pane, &s.cell)
}

func (w *Windows[K, C]) emitEarly(position int) {
	b := &w.buckets[position]
	window := w.cfg.window(b.windowID)
	for _, series := range b.members {
		s := w.slotAt(series, position)
		if !s.isEarlyDirty() {
			continue
		}
		s.clearEarly()
		w.stats.PanesEarly++
		w.sink.Emit(w.series[series].key, Pane{
			Window: window,
			Kind:   PaneEarly,
		}, &s.cell)
	}
}

func (w *Windows[K, C]) retire(position int) {
	b := &w.buckets[position]
	window := w.cfg.window(b.windowID)
	for _, series := range b.members {
		s := w.slotAt(series, position)
		w.sink.Retire(w.series[series].key, window, &s.cell)
		*s = slot[C]{}
		w.stats.Retired++
		w.releaseSlot(series)
	}
	b.vacate()
}

func (w *Windows[K, C]) releaseSlot(series int32) {
	state := &w.series[series]
	state.live--
	if state.live > 0 {
		return
	}
	key := state.key
	delete(w.index, key)
	*state = seriesState[K]{}
	w.free = append(w.free, series)
	w.sink.Closed(key)
}

func (w *Windows[K, C]) slotAt(series int32, position int) *slot[C] {
	return &w.slots[int(series)*int(w.slotsPer)+position]
}
