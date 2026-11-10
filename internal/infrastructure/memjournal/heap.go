package memjournal

import "unsafe"

const (
	queueSlotBytes     = int64(unsafe.Sizeof(entry{}))
	queueOverheadBytes = 64 << 10
)

func (l Limits) WorstCaseHeapBytes() int64 {
	return l.MaxBytes + int64(l.MaxBatches)*queueSlotBytes + queueOverheadBytes
}
