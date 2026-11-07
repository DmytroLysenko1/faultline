package graph

const (
	edgeHeapBytes   = 248
	nodeHeapBytes   = 216
	nodeStringBytes = 2048
	viewNodeBytes   = 64
	viewEdgeBytes   = 48
)

func (l Limits) WorstCaseHeapBytes() int64 {
	return int64(l.MaxEdges)*edgeHeapBytes + int64(l.MaxNodes)*(nodeHeapBytes+nodeStringBytes)
}

func (l Limits) ViewHeapBytes() int64 {
	return int64(l.MaxNodes)*viewNodeBytes + int64(l.MaxEdges)*viewEdgeBytes
}
