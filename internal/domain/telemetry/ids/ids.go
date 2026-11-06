package ids

type TraceID [16]byte

func (id TraceID) IsZero() bool {
	return id == TraceID{}
}

type SpanID [8]byte

func (id SpanID) IsZero() bool {
	return id == SpanID{}
}

type CallKey struct {
	TraceID TraceID
	ID      SpanID
}

func NewCallKey(traceID TraceID, id SpanID) CallKey {
	return CallKey{
		TraceID: traceID,
		ID:      id,
	}
}
