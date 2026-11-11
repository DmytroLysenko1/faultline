package graphservice

import (
	"unsafe"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	nodeResponseBytes      = 3_392
	edgeResponseBytes      = 816
	responseEnvelopeBytes  = 1 << 10
	nodeTreeBytes          = 1_072
	edgeTreeBytes          = 960
	marshalPeakNumerator   = 5
	marshalPeakDenominator = 2
	nodeViewBytes          = int64(unsafe.Sizeof(topology.NodeView{}))
	edgeViewBytes          = int64(unsafe.Sizeof(topology.EdgeView{}))
)

func ResponseBytes(nodes, edges int) int64 {
	return int64(nodes)*nodeResponseBytes + int64(edges)*edgeResponseBytes + responseEnvelopeBytes
}

func ResponseTreeBytes(nodes, edges int) int64 {
	return int64(nodes)*nodeTreeBytes + int64(edges)*edgeTreeBytes
}

func ResponseWeight(nodes, edges int) int64 {
	view := int64(nodes)*nodeViewBytes + int64(edges)*edgeViewBytes
	return view + ResponseTreeBytes(nodes, edges) + ResponseBytes(nodes, edges)*marshalPeakNumerator/marshalPeakDenominator
}
