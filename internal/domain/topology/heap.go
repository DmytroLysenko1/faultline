package topology

import (
	"unsafe"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

const (
	participantBytes   = int64(unsafe.Sizeof(resource.ServiceRef{}))
	matchedCallBytes   = int64(unsafe.Sizeof(call.Call{}))
	servedBytes        = int64(unsafe.Sizeof(resource.ServiceRef{}))
	appliedBytes       = 2*int64(unsafe.Sizeof(handle.Node(0))) + int64(unsafe.Sizeof(handle.Edge(0)))
	servedIndexBytes   = int64(unsafe.Sizeof(0))
	rebuiltViewCopies  = 1
	nodeViewBytes      = int64(unsafe.Sizeof(NodeView{}))
	edgeViewBytes      = int64(unsafe.Sizeof(EdgeView{}))
	snapshotFixedBytes = 64 << 10
)

func (l *Limits) WorstCaseHeapBytes() int64 {
	graphLimits := l.graphLimits()
	rebuilt := rebuiltViewCopies * graphLimits.ViewHeapBytes()
	return l.matcherLimits().WorstCaseHeapBytes() + graphLimits.WorstCaseHeapBytes() + rebuilt +
		l.trafficLimits().WorstCaseHeapBytes()
}

func (l *Limits) ViewHeapBytes() int64 {
	return int64(l.MaxNodes)*nodeViewBytes + int64(l.MaxEdges)*edgeViewBytes + snapshotFixedBytes
}

func ApplyHeapBytes(maxSpans int) int64 {
	return int64(maxSpans) * (participantBytes + matchedCallBytes + servedBytes + appliedBytes + servedIndexBytes)
}
