package graph

import "unsafe"

const (
	edgeHeapBytes   = 264
	nodeHeapBytes   = 240
	nodeStringBytes = 2048
	graphFixedBytes = 64 << 10
	viewNodeBytes   = int64(unsafe.Sizeof(Node{}))
	viewEdgeBytes   = int64(unsafe.Sizeof(Edge{}))
)

func (l Limits) WorstCaseHeapBytes() int64 {
	return int64(l.MaxEdges)*edgeHeapBytes + int64(l.MaxNodes)*(nodeHeapBytes+nodeStringBytes) + graphFixedBytes
}

func (l Limits) ViewHeapBytes() int64 {
	return int64(l.MaxNodes)*viewNodeBytes + int64(l.MaxEdges)*viewEdgeBytes
}
