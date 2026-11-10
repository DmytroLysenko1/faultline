package matcher

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
)

const (
	pendingHalfHeapBytes  = 288
	serviceRefStringBytes = 512
	operationNameBytes    = operation.MaxNameBytes
)

func (l Limits) WorstCaseHeapBytes() int64 {
	pending := int64(l.MaxPending) * (pendingHalfHeapBytes + serviceRefStringBytes + operationNameBytes)
	return pending + tombstone.WorstCaseHeapBytes(l.MaxTombstones)
}
