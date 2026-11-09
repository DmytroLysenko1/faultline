package stream

import "time"

func (w *Windows[K, C]) Begin(mark Mark, anchor time.Time) {
	if w.batch.open {
		panic("stream: Begin while a batch is open")
	}
	if !mark.Known && !w.batch.known {
		w.batch.open = true
		return
	}
	nanos := w.monotonicMark(mark)
	size := int64(w.cfg.Size)
	w.batch = batchState{open: true, known: true, markNanos: nanos}
	w.batch.dropBelow = nanos - int64(w.cfg.AllowedLateness)
	w.batch.openFrom = floorDiv(nanos, size)
	w.batch.firstLive = floorDiv(w.batch.dropBelow, size)
	w.batch.anchorID = w.clampToHorizon(w.cfg.windowID(anchor))
	w.closeWindows()
}

func (w *Windows[K, C]) monotonicMark(mark Mark) int64 {
	if !mark.Known {
		return w.batch.markNanos
	}
	nanos := w.saturatedNanos(mark.Time)
	if w.batch.known {
		return max(nanos, w.batch.markNanos)
	}
	return nanos
}

func (w *Windows[K, C]) End() {
	w.mustBeInBatch()
	for _, ref := range w.late {
		w.emitLate(ref)
	}
	w.late = w.late[:0]
	w.batch.open = false
}

func (w *Windows[K, C]) FireEarly() {
	if !w.early {
		return
	}
	for _, position := range w.liveBucketsInOrder() {
		if !w.buckets[position].final {
			w.emitEarly(position)
		}
	}
}

func (w *Windows[K, C]) closeWindows() {
	ordered := w.liveBucketsInOrder()
	for _, position := range ordered {
		w.finalizeIfDue(position)
	}
	for _, position := range ordered {
		if w.buckets[position].windowID < w.batch.firstLive {
			w.retire(position)
		}
	}
}

func (w *Windows[K, C]) finalizeIfDue(position int) {
	if b := &w.buckets[position]; !b.final && b.windowID < w.batch.openFrom {
		w.emitOnTime(position)
	}
}

func (w *Windows[K, C]) clampToHorizon(id int64) int64 {
	return min(max(id, w.batch.firstLive), w.batch.firstLive+w.slotsPer-1)
}

func (w *Windows[K, C]) liveBucketsInOrder() []int {
	w.order = w.order[:0]
	for position := range w.buckets {
		if w.buckets[position].live {
			w.order = insertByWindow(w.order, w.buckets, position)
		}
	}
	return w.order
}

func insertByWindow(order []int, buckets []bucket, position int) []int {
	order = append(order, position)
	for i := len(order) - 1; i > 0 && buckets[order[i-1]].windowID > buckets[position].windowID; i-- {
		order[i-1], order[i] = order[i], order[i-1]
	}
	return order
}

func (w *Windows[K, C]) mustBeInBatch() {
	if !w.batch.open {
		panic("stream: placement outside Begin/End")
	}
}
