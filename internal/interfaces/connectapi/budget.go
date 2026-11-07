package connectapi

const (
	worstNodeResponseBytes = 2_688
	worstEdgeResponseBytes = 112
	responseEnvelopeBytes  = 1 << 10
	responseTreeNodeBytes  = 176
	responseTreeEdgeBytes  = 80
	marshalPeakNumerator   = 5
	marshalPeakDenominator = 2
)

func WorstResponseBytes(maxNodes, maxEdges int) int64 {
	return int64(maxNodes)*worstNodeResponseBytes + int64(maxEdges)*worstEdgeResponseBytes + responseEnvelopeBytes
}

func WorstHeapBytes(maxNodes, maxEdges int, viewBytes int64) int64 {
	tree := int64(maxNodes)*responseTreeNodeBytes + int64(maxEdges)*responseTreeEdgeBytes
	marshal := WorstResponseBytes(maxNodes, maxEdges) * marshalPeakNumerator / marshalPeakDenominator
	return MaxConcurrentGetGraph * (viewBytes + tree + marshal)
}
