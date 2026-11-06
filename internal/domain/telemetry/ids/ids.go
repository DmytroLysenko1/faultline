package ids

const (
	TraceIDBytes = 16
	SpanIDBytes  = 8
)

type TraceID [TraceIDBytes]byte

func (id TraceID) IsZero() bool {
	return id == TraceID{}
}

type SpanID [SpanIDBytes]byte

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
