package sketch

const (
	minOrder = 1
	maxOrder = 11

	MinEntriesPerSketch = 1 << minOrder
	MaxEntriesPerSketch = 1 << maxOrder

	noOrder = 0

	maxZero = ^uint32(0)
)

type Sketch struct {
	count      uint64
	offset     uint32
	zero       uint32
	generation uint32
	length     uint16
	order      uint8
	collapsed  bool
}

func (s *Sketch) Count() uint64 {
	return s.count
}

func (s *Sketch) capacity() int {
	if s.order == noOrder {
		return 0
	}
	return 1 << s.order
}

func (s *Sketch) hasBlock() bool {
	return s.order != noOrder
}
