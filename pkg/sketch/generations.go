//go:build faultline_debug

package sketch

const generationBytes = 4

type generations struct {
	byBlock []uint32
}

func newGenerations(entries int) generations {
	return generations{
		byBlock: make([]uint32, entries/MinEntriesPerSketch),
	}
}

func (g generations) current(offset uint32) uint32 {
	return g.byBlock[offset/MinEntriesPerSketch]
}

func (g generations) retire(offset uint32) {
	g.byBlock[offset/MinEntriesPerSketch]++
}

func (g generations) touch(s *Sketch) {
	g.retire(s.offset)
	s.generation = g.current(s.offset)
}

func generationsBytes(entries int) int64 {
	return int64(entries/MinEntriesPerSketch) * generationBytes
}

func (g generations) check(s *Sketch) {
	if s.hasBlock() && g.current(s.offset) != s.generation {
		panic("sketch: stale Sketch copy: block released or sketch changed since the copy")
	}
}
