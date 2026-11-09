package stream

const (
	occupiedFlag  = 1
	earlyFlag     = 2
	lateFlag      = 4
	revisionShift = 3
	flagsBits     = 32
	maxRevision   = 1<<(flagsBits-revisionShift) - 1
)

type slot[C any] struct {
	cell  C
	flags uint32
}

func (s *slot[C]) occupied() bool {
	return s.flags&occupiedFlag != 0
}

func (s *slot[C]) occupy() {
	s.flags = occupiedFlag
}

func (s *slot[C]) revision() uint32 {
	return s.flags >> revisionShift
}

func (s *slot[C]) isEarlyDirty() bool {
	return s.flags&earlyFlag != 0
}

func (s *slot[C]) isLateDirty() bool {
	return s.flags&lateFlag != 0
}

func (s *slot[C]) markEarly() {
	s.flags |= earlyFlag
}

func (s *slot[C]) markLate() {
	s.flags |= lateFlag
}

func (s *slot[C]) clearEarly() {
	s.flags &^= earlyFlag
}

func (s *slot[C]) finalize() uint32 {
	revision := min(s.revision()+1, maxRevision)
	s.flags = revision<<revisionShift | occupiedFlag
	return revision
}

type slotRef struct {
	series   int32
	position int32
}

type bucket struct {
	windowID int64
	live     bool
	final    bool
	members  []int32
}

func (b *bucket) occupy(windowID int64, final bool) {
	b.windowID = windowID
	b.live = true
	b.final = final
	b.members = b.members[:0]
}

func (b *bucket) vacate() {
	b.live = false
	b.members = b.members[:0]
}
