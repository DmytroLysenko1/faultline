package tombstone

const (
	entryHeapBytes      = 104
	ringGrowthSlotBytes = 16
)

func WorstCaseHeapBytes(limit int) int64 {
	return int64(limit) * (entryHeapBytes + ringGrowthSlotBytes)
}
