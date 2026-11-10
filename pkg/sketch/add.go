package sketch

func (a *Arena) addIndex(s *Sketch, index uint16) {
	entries := a.entries(s)
	position := lowerBound(entries, entryKey(index))
	if position < len(entries) && entries[position].index() == index {
		a.incrementEntry(s, entries, position)
		return
	}
	if a.makeRoom(s) {
		a.insert(s, position, index)
		return
	}
	a.collapseInto(s, position, index)
}

func lowerBound(entries []entry, key entry) int {
	base, size := 0, len(entries)
	for size > 1 {
		half := size >> 1
		if entries[base+half-1] < key {
			base += half
		}
		size -= half
	}
	if size == 1 && entries[base] < key {
		base++
	}
	return base
}

func (a *Arena) incrementEntry(s *Sketch, entries []entry, position int) {
	if entries[position].count() == countMask {
		a.Stats.DroppedSamples++
		return
	}
	entries[position] = entries[position].plus(1)
	s.Count++
	a.generations.touch(s)
}

func (a *Arena) makeRoom(s *Sketch) bool {
	if int(s.length) < s.capacity() {
		return true
	}
	if !s.hasBlock() {
		return a.allocFirst(s)
	}
	return a.growInPlace(s) || a.growByCopy(s)
}

func (a *Arena) allocFirst(s *Sketch) bool {
	offset, ok := a.blocks.alloc(minOrder)
	if !ok {
		return false
	}
	a.adopt(s, offset, minOrder)
	return true
}

func (a *Arena) growInPlace(s *Sketch) bool {
	merged, ok := a.blocks.absorbSibling(s.offset, s.order)
	if !ok {
		return false
	}
	copy(a.blocks.block(merged, s.order+1), a.entries(s))
	a.disown(s)
	a.adopt(s, merged, s.order+1)
	return true
}

func (a *Arena) growByCopy(s *Sketch) bool {
	order := s.order + 1
	offset, ok := a.blocks.alloc(order)
	if !ok {
		return false
	}
	copy(a.blocks.block(offset, order), a.entries(s))
	a.releaseBlock(s)
	a.adopt(s, offset, order)
	return true
}

func (a *Arena) adopt(s *Sketch, offset uint32, order uint8) {
	s.offset, s.order = offset, order
	s.generation = a.generations.current(offset)
	a.Stats.InUse += s.capacity()
}

func (a *Arena) disown(s *Sketch) {
	a.Stats.InUse -= s.capacity()
	a.generations.retire(s.offset)
}

func (a *Arena) releaseBlock(s *Sketch) {
	if !s.hasBlock() {
		return
	}
	a.disown(s)
	a.blocks.release(s.offset, s.order)
}

func (a *Arena) insert(s *Sketch, position int, index uint16) {
	entries := a.blocks.block(s.offset, s.order)[:s.length+1]
	copy(entries[position+1:], entries[position:s.length])
	entries[position] = newEntry(index)
	s.length++
	s.Count++
	a.generations.touch(s)
}

func (a *Arena) collapseInto(s *Sketch, position int, index uint16) {
	entries := a.entries(s)
	if s.hasBlock() && position == 0 {
		a.markCollapsed(s)
		a.incrementEntry(s, entries, 0)
		return
	}
	if !s.hasBlock() || !foldFits(entries) {
		a.Stats.DroppedSamples++
		return
	}
	a.markCollapsed(s)
	entries[1] = entries[1].plus(entries[0].count())
	copy(entries, entries[1:])
	s.length--
	a.insert(s, position-1, index)
}

func foldFits(entries []entry) bool {
	return len(entries) < 2 ||
		entries[1].count() <= countMask-entries[0].count()
}

func (a *Arena) markCollapsed(s *Sketch) {
	a.Stats.CollapsedAdds++
	if s.collapsed {
		return
	}
	s.collapsed = true
	a.Stats.CollapsedSketches++
}
