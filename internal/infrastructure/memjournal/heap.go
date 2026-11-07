package memjournal

const queueSlotBytes = 56

func (l Limits) WorstCaseHeapBytes() int64 {
	return l.MaxBytes + int64(l.MaxBatches)*queueSlotBytes
}
