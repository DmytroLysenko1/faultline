package sketch

const (
	countBits = 48
	countMask = 1<<countBits - 1
	linkMask  = 1<<32 - 1
)

type entry uint64

func newEntry(index uint16) entry {
	return entryKey(index) | 1
}

func entryKey(index uint16) entry {
	return entry(uint64(index) << countBits)
}

func (e entry) index() uint16 {
	return uint16(e >> countBits)
}

func (e entry) count() uint64 {
	return uint64(e) & countMask
}

func (e entry) plus(count uint64) entry {
	return e + entry(count)
}

func (e entry) link() uint32 {
	return uint32(e & linkMask)
}
