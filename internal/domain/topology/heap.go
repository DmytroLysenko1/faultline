package topology

const (
	participantBytes  = 32
	matchedCallBytes  = 72
	rebuiltViewCopies = 1
)

func (l Limits) WorstCaseHeapBytes() int64 {
	graphLimits := l.graphLimits()
	rebuilt := rebuiltViewCopies * graphLimits.ViewHeapBytes()
	return l.matcherLimits().WorstCaseHeapBytes() + graphLimits.WorstCaseHeapBytes() + rebuilt
}

func (l Limits) ViewHeapBytes() int64 {
	return l.graphLimits().ViewHeapBytes()
}

func ApplyHeapBytes(maxSpans int) int64 {
	return int64(maxSpans) * (participantBytes + matchedCallBytes)
}
