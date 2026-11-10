package stream

import (
	"math"
	"time"
)

func (w *Windows[K, C]) Classify(t time.Time) Placement {
	placement, _, _ := w.classify(t)
	return placement
}

func (w *Windows[K, C]) Drop(placement Placement) {
	w.mustBeInBatch()
	w.stats.Offered++
	switch placement {
	case PlacementDroppedLate:
		w.stats.LateDropped++
	case PlacementDroppedNoWatermark:
		w.stats.NoWatermark++
	default:
		panic("stream: Drop of an event that is placed, not dropped")
	}
}

func (w *Windows[K, C]) Locate(key K, t time.Time) (*C, Placement, bool) {
	placement, id, reassigned := w.classify(t)
	if placement.Dropped() {
		w.Drop(placement)
		return nil, placement, false
	}
	if reassigned {
		w.stats.Reassigned++
	}
	series, created := w.seriesFor(key)
	s, position := w.openSlot(series, id)
	w.markDirty(s, slotRef{series: series, position: position}, placement)
	return &s.cell, placement, created
}

func (w *Windows[K, C]) classify(t time.Time) (Placement, int64, bool) {
	w.mustBeInBatch()
	if !w.batch.known {
		return PlacementDroppedNoWatermark, 0, false
	}
	nanos, beyond := w.saturatedNanos(t), t.After(w.latest)
	if nanos < w.batch.dropBelow {
		return PlacementDroppedLate, 0, false
	}
	id, reassigned := w.horizonWindow(nanos, beyond)
	if id < w.batch.openFrom {
		return PlacementLate, id, reassigned
	}
	return PlacementOnTime, id, reassigned
}

func (w *Windows[K, C]) horizonWindow(nanos int64, beyond bool) (int64, bool) {
	id := floorDiv(nanos, int64(w.cfg.Size))
	if beyond || id >= w.batch.firstLive+w.slotsPer {
		return w.batch.anchorID, true
	}
	return id, false
}

func (w *Windows[K, C]) saturatedNanos(t time.Time) int64 {
	switch {
	case t.Before(w.earliest):
		return math.MinInt64
	case t.After(w.latest):
		return math.MaxInt64
	}
	return t.UnixNano()
}

func (w *Windows[K, C]) seriesFor(key K) (int32, bool) {
	if series, ok := w.index[key]; ok {
		return series, false
	}
	if len(w.free) == 0 {
		panic("stream: no capacity for a new series; the caller must check Len() < Capacity()")
	}
	series := w.free[len(w.free)-1]
	w.free = w.free[:len(w.free)-1]
	w.index[key] = series
	w.series[series] = seriesState[K]{key: key}
	return series, true
}

func (w *Windows[K, C]) openSlot(series int32, id int64) (*slot[C], int32) {
	position := seriesIndex(int(floorMod(id, w.slotsPer)))
	b := &w.buckets[position]
	if !b.live {
		b.occupy(id, id < w.batch.openFrom)
	}
	if b.windowID != id {
		panic("stream: window ring collision; the horizon invariant is broken")
	}
	s := w.slotAt(series, int(position))
	if !s.occupied() {
		s.occupy()
		b.members = append(b.members, series)
		w.series[series].live++
	}
	return s, position
}

func (w *Windows[K, C]) markDirty(s *slot[C], ref slotRef, placement Placement) {
	w.stats.Offered++
	if placement == PlacementOnTime {
		w.stats.OnTime++
		s.markEarly()
		return
	}
	w.stats.LateAccepted++
	if !s.isLateDirty() {
		s.markLate()
		w.late = append(w.late, ref)
	}
}
