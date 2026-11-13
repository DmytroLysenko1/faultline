package sketch

import (
	"fmt"
	"math"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

type ArenaRestore struct {
	Arena   *Arena
	budget  int
	claimed []uint64
	inUse   int
}

func (a *Arena) WriteCheckpoint(w *checkpoint.Writer) {
	w.Count(a.Stats.Budget)
	w.Count(a.Stats.InUse)
	w.Uint(a.Stats.CollapsedAdds)
	w.Uint(a.Stats.CollapsedSketches)
	w.Uint(a.Stats.DroppedSamples)
	for order := uint8(minOrder); order <= a.blocks.topOrder; order++ {
		a.writeFreeList(w, order)
	}
}

func (a *Arena) writeFreeList(w *checkpoint.Writer, order uint8) {
	length := 0
	for offset := a.blocks.heads[order]; offset != noBlock; offset = a.blocks.next(offset) {
		length++
	}
	w.Count(length)
	for offset := a.blocks.heads[order]; offset != noBlock; offset = a.blocks.next(offset) {
		w.Uint(uint64(offset))
	}
}

func (b *buddy) next(offset uint32) uint32 {
	return b.slab[offset+linkNext].link()
}

func (a *Arena) WriteSketch(w *checkpoint.Writer, s *Sketch) {
	w.Uint(s.Count)
	w.Uint(uint64(s.zero))
	w.Bool(s.collapsed)
	w.Uint(uint64(s.order))
	w.Uint(uint64(s.offset))
	entries := a.entries(s)
	w.Count(len(entries))
	previous := uint16(0)
	for _, e := range entries {
		w.Uint(uint64(e.index() - previous))
		w.Uint(e.count())
		previous = e.index()
	}
}

func RestoreArena(maxEntries int, r *checkpoint.Reader) (*ArenaRestore, error) {
	arena, err := NewArena(maxEntries)
	if err != nil {
		return nil, err
	}
	arena.blocks.remove(arena.blocks.topOrder, firstFree)
	restore := &ArenaRestore{
		Arena:  arena,
		budget: r.Count(maxEntries),
	}
	if !restore.budgetFits(r) {
		return restore, nil
	}
	restore.claimed = make([]uint64, bitmapWords(restore.budget, minOrder))
	restore.readStats(r)
	restore.readFreeLists(r)
	return restore, nil
}

func (x *ArenaRestore) budgetFits(r *checkpoint.Reader) bool {
	if err := validateBudget(x.budget); err != nil {
		r.Fail(fmt.Sprintf("saved sketch budget: %v", err))
		return false
	}
	return r.Err() == nil
}

func (x *ArenaRestore) readStats(r *checkpoint.Reader) {
	stats := &x.Arena.Stats
	stats.InUse = r.Count(x.budget)
	stats.CollapsedAdds = r.Uint()
	stats.CollapsedSketches = r.Uint()
	stats.DroppedSamples = r.Uint()
}

func (x *ArenaRestore) readFreeLists(r *checkpoint.Reader) {
	for order := uint8(minOrder); 1<<order <= x.budget; order++ {
		x.readFreeList(r, order)
	}
}

func (x *ArenaRestore) readFreeList(r *checkpoint.Reader, order uint8) {
	offsets := make([]uint32, r.Count(x.budget>>order))
	for i := range offsets {
		offsets[i] = x.claimBlock(r, order)
	}
	for i := len(offsets) - 1; i >= 0 && r.Err() == nil; i-- {
		x.Arena.blocks.push(order, offsets[i])
	}
}

func (x *ArenaRestore) ReadSketch(r *checkpoint.Reader, s *Sketch) {
	*s = Sketch{
		Count:     r.Uint(),
		zero:      r.Uint32(maxZero),
		collapsed: r.Bool(),
		order:     r.Uint8(maxOrder),
	}
	if s.order == noOrder {
		x.readNoBlock(r, s)
		return
	}
	if s.order < minOrder {
		r.Fail(fmt.Sprintf("sketch order %d is below %d", s.order, minOrder))
		return
	}
	s.offset = x.claimBlock(r, s.order)
	x.readEntries(r, s)
}

func (x *ArenaRestore) readNoBlock(r *checkpoint.Reader, s *Sketch) {
	offset, length := r.Uint(), r.Uint()
	if offset != 0 || length != 0 || s.Count != uint64(s.zero) {
		r.Fail("a sketch without a block has an offset, entries or uncounted samples")
	}
}

func (x *ArenaRestore) readEntries(r *checkpoint.Reader, s *Sketch) {
	length := r.Uint16(uint16(1) << s.order)
	if r.Err() != nil {
		return
	}
	block := x.Arena.blocks.block(s.offset, s.order)
	total := uint64(s.zero)
	previous := -1
	for i := range length {
		block[i] = x.readEntry(r, previous)
		total += block[i].count()
		previous = int(block[i].index())
	}
	s.length = length
	s.generation = x.Arena.generations.current(s.offset)
	x.inUse += s.capacity()
	if total != s.Count {
		r.Fail(fmt.Sprintf("sketch counts sum to %d, not %d", total, s.Count))
	}
}

func (x *ArenaRestore) readEntry(r *checkpoint.Reader, previous int) entry {
	index := max(previous, 0) + r.Index(len(x.Arena.mapping.values))
	count := r.Uint()
	if index <= previous || index >= len(x.Arena.mapping.values) || count == 0 || count > countMask {
		r.Fail("sketch entries are not strictly ordered known bins with positive counts")
		return 0
	}
	return entryKey(binOf(index)) | entry(count)
}

func (x *ArenaRestore) claimBlock(r *checkpoint.Reader, order uint8) uint32 {
	size := uint32(1) << order
	offset := r.Uint32(entriesUint32(x.budget) - size)
	if r.Err() != nil || offset%size != 0 {
		r.Fail(fmt.Sprintf("block %d of order %d is misaligned or outside the arena", offset, order))
		return 0
	}
	for unit := offset / MinEntriesPerSketch; unit < (offset+size)/MinEntriesPerSketch; unit++ {
		x.claimUnit(r, unit)
	}
	return offset
}

func (x *ArenaRestore) claimUnit(r *checkpoint.Reader, unit uint32) {
	word, mask := unit/wordBits, uint64(1)<<(unit%wordBits)
	if x.claimed[word]&mask != 0 {
		r.Fail("two blocks overlap")
		return
	}
	x.claimed[word] |= mask
}

func (x *ArenaRestore) Finish(r *checkpoint.Reader) {
	if r.Err() != nil {
		return
	}
	if x.inUse != x.Arena.Stats.InUse || !x.coversArena() {
		r.Fail("sketch blocks and free blocks do not tile the arena")
		return
	}
	x.Arena.blocks.grow(x.budget)
}

func (x *ArenaRestore) coversArena() bool {
	units := x.budget / MinEntriesPerSketch
	for unit := range units {
		if x.claimed[unit/wordBits]&(uint64(1)<<(unit%wordBits)) == 0 {
			return false
		}
	}
	return true
}

func (b *buddy) grow(from int) {
	for order := orderOf(from); order < b.topOrder; order++ {
		b.release(uint32(1)<<order, order)
	}
}

func entriesUint32(entries int) uint32 {
	if entries < 0 || entries > math.MaxUint32 {
		return 0
	}
	return uint32(entries)
}

func binOf(index int) uint16 {
	if index < 0 || index > math.MaxUint16 {
		return 0
	}
	return uint16(index)
}
