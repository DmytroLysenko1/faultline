package sketch

const (
	noBlock   = ^uint32(0)
	wordBits  = 64
	wordBytes = 8
	linkNext  = 0
	linkPrev  = 1
	firstFree = 0
)

type buddy struct {
	slab     []entry
	topOrder uint8
	heads    []uint32
	free     [][]uint64
}

func newBuddy(entries int) buddy {
	topOrder := orderOf(entries)
	allocator := buddy{
		slab:     make([]entry, entries),
		topOrder: topOrder,
		heads:    make([]uint32, topOrder+1),
		free:     make([][]uint64, topOrder+1),
	}
	for order := range allocator.heads {
		allocator.heads[order] = noBlock
	}
	for order := minOrder; order <= int(topOrder); order++ {
		allocator.free[order] = make([]uint64, bitmapWords(entries, order))
	}
	allocator.push(topOrder, firstFree)
	return allocator
}

func (b *buddy) alloc(order uint8) (uint32, bool) {
	available := order
	for available <= b.topOrder && b.heads[available] == noBlock {
		available++
	}
	if available > b.topOrder {
		return 0, false
	}
	offset := b.heads[available]
	b.remove(available, offset)
	for available > order {
		available--
		b.push(available, offset+uint32(1)<<available)
	}
	return offset, true
}

func (b *buddy) release(offset uint32, order uint8) {
	for order < b.topOrder {
		sibling := offset ^ uint32(1)<<order
		if !b.isFree(order, sibling) {
			break
		}
		b.remove(order, sibling)
		offset = min(offset, sibling)
		order++
	}
	b.push(order, offset)
}

func (b *buddy) absorbSibling(offset uint32, order uint8) (uint32, bool) {
	if order >= b.topOrder {
		return 0, false
	}
	sibling := offset ^ uint32(1)<<order
	if !b.isFree(order, sibling) {
		return 0, false
	}
	b.remove(order, sibling)
	return min(offset, sibling), true
}

func (b *buddy) block(offset uint32, order uint8) []entry {
	return b.slab[offset : offset+uint32(1)<<order]
}

func (b *buddy) push(order uint8, offset uint32) {
	head := b.heads[order]
	b.slab[offset+linkNext] = entry(head)
	b.slab[offset+linkPrev] = entry(noBlock)
	if head != noBlock {
		b.slab[head+linkPrev] = entry(offset)
	}
	b.heads[order] = offset
	b.setFree(order, offset, true)
}

func (b *buddy) remove(order uint8, offset uint32) {
	next := b.slab[offset+linkNext].link()
	prev := b.slab[offset+linkPrev].link()
	if prev == noBlock {
		b.heads[order] = next
	} else {
		b.slab[prev+linkNext] = entry(next)
	}
	if next != noBlock {
		b.slab[next+linkPrev] = entry(prev)
	}
	b.setFree(order, offset, false)
}

func (b *buddy) isFree(order uint8, offset uint32) bool {
	position := offset >> order
	return b.free[order][position/wordBits]&(1<<(position%wordBits)) != 0
}

func (b *buddy) setFree(order uint8, offset uint32, isFree bool) {
	position := offset >> order
	mask := uint64(1) << (position % wordBits)
	if isFree {
		b.free[order][position/wordBits] |= mask
		return
	}
	b.free[order][position/wordBits] &^= mask
}

func bitmapBytes(entries int) int64 {
	var words int64
	for order := minOrder; entries>>order > 0; order++ {
		words += int64(bitmapWords(entries, order))
	}
	return words * wordBytes
}

func bitmapWords(entries, order int) int {
	return (entries>>order + wordBits - 1) / wordBits
}

func orderOf(entries int) uint8 {
	var order uint8
	for size := entries; size > 1; size >>= 1 {
		order++
	}
	return order
}
