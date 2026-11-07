package matcher

import "github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"

const (
	pendingHalfHeapBytes  = 240
	serviceRefStringBytes = 512
)

func (l Limits) WorstCaseHeapBytes() int64 {
	pending := int64(l.MaxPending) * (pendingHalfHeapBytes + serviceRefStringBytes)
	return pending + tombstone.WorstCaseHeapBytes(l.MaxTombstones)
}
