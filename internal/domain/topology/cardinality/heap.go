package cardinality

import (
	"unsafe"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

const (
	operationOverheadBytes = 160
	registryFixedBytes     = 64 << 10
)

func WorstHeapBytes(maxOperations, maxOwners int) int64 {
	perOperation := int64(operation.MaxNameBytes + operationOverheadBytes)
	perOwner := int64(unsafe.Sizeof(0))
	return int64(maxOperations)*perOperation + int64(maxOwners)*perOwner + registryFixedBytes
}
