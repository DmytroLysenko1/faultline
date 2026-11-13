package stream

import (
	"fmt"
	"math"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

type Codec[K comparable, C any] interface {
	WriteKey(w *checkpoint.Writer, key K)
	ReadKey(r *checkpoint.Reader) K
	WriteCell(w *checkpoint.Writer, cell *C)
	ReadCell(r *checkpoint.Reader, cell *C)
}

func (w *Windows[K, C]) WriteCheckpoint(out *checkpoint.Writer, codec Codec[K, C]) {
	if w.batch.open {
		panic("stream: WriteCheckpoint while a batch is open")
	}
	out.Count(len(w.series))
	w.writeBatch(out)
	w.writeStats(out)
	w.writeSeries(out, codec)
	out.Count(len(w.free))
	for _, series := range w.free {
		out.Count(int(series))
	}
	for position := range w.buckets {
		w.writeBucket(out, codec, position)
	}
}

func (w *Windows[K, C]) writeBatch(out *checkpoint.Writer) {
	out.Bool(w.batch.known)
	out.Int(w.batch.markNanos)
}

func (w *Windows[K, C]) writeStats(out *checkpoint.Writer) {
	for _, counter := range w.counters() {
		out.Uint(*counter)
	}
}

func (w *Windows[K, C]) counters() []*uint64 {
	return []*uint64{
		&w.stats.Offered, &w.stats.OnTime,
		&w.stats.LateAccepted, &w.stats.LateDropped,
		&w.stats.NoWatermark, &w.stats.Reassigned,
		&w.stats.PanesEarly, &w.stats.PanesOnTime,
		&w.stats.PanesLate, &w.stats.Retired,
	}
}

func (w *Windows[K, C]) onHorizon(b *bucket, position int) bool {
	return w.batch.known &&
		b.windowID >= w.batch.firstLive && b.windowID-w.batch.firstLive < w.slotsPer &&
		floorMod(b.windowID, w.slotsPer) == int64(position) &&
		b.final == (b.windowID < w.batch.openFrom)
}

func (w *Windows[K, C]) writeSeries(out *checkpoint.Writer, codec Codec[K, C]) {
	out.Count(len(w.index))
	for series := range w.series {
		if w.series[series].live > 0 {
			out.Count(series)
			codec.WriteKey(out, w.series[series].key)
		}
	}
}

func (w *Windows[K, C]) writeBucket(out *checkpoint.Writer, codec Codec[K, C], position int) {
	b := &w.buckets[position]
	out.Bool(b.live)
	if !b.live {
		return
	}
	out.Int(b.windowID)
	out.Bool(b.final)
	out.Count(len(b.members))
	for _, series := range b.members {
		s := w.slotAt(series, position)
		out.Count(int(series))
		out.Uint(uint64(s.flags))
		codec.WriteCell(out, &s.cell)
	}
}

func Restore[K comparable, C any](
	cfg Config,
	options Options,
	sink Sink[K, C],
	r *checkpoint.Reader,
	codec Codec[K, C],
) (*Windows[K, C], error) {
	w, err := New(cfg, options, sink)
	if err != nil {
		return nil, err
	}
	restore := windowsRestore[K, C]{
		windows: w,
		codec:   codec,
		saved:   r.Count(options.Capacity),
	}
	restore.read(r)
	return w, nil
}

type windowsRestore[K comparable, C any] struct {
	windows *Windows[K, C]
	codec   Codec[K, C]
	saved   int
}

func (x *windowsRestore[K, C]) read(r *checkpoint.Reader) {
	x.readBatch(r)
	for _, counter := range x.windows.counters() {
		*counter = r.Uint()
	}
	x.readSeries(r)
	x.readFree(r)
	for position := range x.windows.buckets {
		x.readBucket(r, position)
	}
	x.checkSeriesLive(r)
}

func (x *windowsRestore[K, C]) readBatch(r *checkpoint.Reader) {
	known, markNanos := r.Bool(), r.Int()
	if known {
		x.windows.batch = x.windows.batchAt(markNanos)
	}
}

func (x *windowsRestore[K, C]) readSeries(r *checkpoint.Reader) {
	w := x.windows
	count, previous := r.Count(x.saved), int32(-1)
	for range count {
		series := x.seriesIndex(r)
		key := x.codec.ReadKey(r)
		if _, repeated := w.index[key]; repeated || series <= previous {
			r.Fail("window series are repeated or out of order")
			return
		}
		w.index[key] = series
		w.series[series].key = key
		previous = series
	}
}

func (x *windowsRestore[K, C]) seriesIndex(r *checkpoint.Reader) int32 {
	return r.Int32(int32(min(max(x.saved-1, 0), math.MaxInt32)))
}

func (x *windowsRestore[K, C]) readFree(r *checkpoint.Reader) {
	w := x.windows
	count := r.Count(x.saved)
	if count != x.saved-len(w.index) {
		r.Fail(fmt.Sprintf("%d free series for %d open of %d", count, len(w.index), x.saved))
		return
	}
	grown := len(w.series) - x.saved
	w.free = w.free[:grown]
	seen := make([]bool, x.saved)
	for range count {
		x.readFreeSeries(r, seen)
	}
}

func (x *windowsRestore[K, C]) readFreeSeries(r *checkpoint.Reader, seen []bool) {
	w := x.windows
	series := x.seriesIndex(r)
	if r.Err() != nil || seen[series] || w.isOpen(series) {
		r.Fail("a free series is repeated or open")
		return
	}
	seen[series] = true
	w.free = append(w.free, series)
}

func (w *Windows[K, C]) isOpen(series int32) bool {
	key := w.series[series].key
	open, found := w.index[key]
	return found && open == series
}

func (x *windowsRestore[K, C]) readBucket(r *checkpoint.Reader, position int) {
	b := &x.windows.buckets[position]
	if !r.Bool() {
		return
	}
	b.occupy(r.Int(), r.Bool())
	if !x.windows.onHorizon(b, position) {
		r.Fail("a window lies off the horizon, in another window's slot or with a wrong final mark")
		return
	}
	members := r.Count(x.saved)
	for range members {
		x.readMember(r, position)
	}
}

func (x *windowsRestore[K, C]) readMember(r *checkpoint.Reader, position int) {
	w := x.windows
	series := x.seriesIndex(r)
	s := w.slotAt(series, position)
	if r.Err() != nil || !w.isOpen(series) || s.occupied() {
		r.Fail("a window member is not an open series or is repeated")
		return
	}
	s.flags = readSlotFlags(r)
	x.codec.ReadCell(r, &s.cell)
	w.buckets[position].members = append(w.buckets[position].members, series)
	w.series[series].live++
}

func (x *windowsRestore[K, C]) checkSeriesLive(r *checkpoint.Reader) {
	for _, series := range x.windows.index {
		if x.windows.series[series].live == 0 {
			r.Fail("an open series has no window")
			return
		}
	}
}

func readSlotFlags(r *checkpoint.Reader) uint32 {
	flags := r.Uint32(math.MaxUint32)
	if flags&occupiedFlag == 0 || flags&lateFlag != 0 {
		r.Fail("a window slot is unoccupied or still marked late")
	}
	return flags
}

func (w *Watermark) WriteCheckpoint(out *checkpoint.Writer) {
	for _, mark := range []Mark{
		w.Current,
		w.MaxReceivedAt,
		w.CoverageStart,
		w.maxCandidate,
	} {
		out.Bool(mark.Known)
		out.Time(mark.Time)
	}
}

func RestoreWatermark(cfg Config, r *checkpoint.Reader) (*Watermark, error) {
	w, err := NewWatermark(cfg)
	if err != nil {
		return nil, err
	}
	for _, mark := range []*Mark{
		&w.Current,
		&w.MaxReceivedAt,
		&w.CoverageStart,
		&w.maxCandidate,
	} {
		mark.Known = r.Bool()
		mark.Time = r.Time()
	}
	return w, nil
}
