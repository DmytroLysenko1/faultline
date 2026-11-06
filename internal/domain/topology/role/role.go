package role

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

type Role uint8

const (
	Ignored Role = iota
	Unpairable
	Caller
	Callee
)

func (r Role) Participates() bool {
	return r != Ignored
}

func (r Role) Pairable() bool {
	return r == Caller || r == Callee
}

func Of(observed span.Span) (ids.CallKey, Role) {
	switch observed.Kind {
	case kind.Client:
		if observed.CallsExternalSystem {
			return ids.CallKey{}, Unpairable
		}
		return ids.NewCallKey(observed.TraceID, observed.SpanID), Caller
	case kind.Server:
		if observed.ParentSpanID.IsZero() {
			return ids.CallKey{}, Unpairable
		}
		return ids.NewCallKey(observed.TraceID, observed.ParentSpanID), Callee
	default:
		return ids.CallKey{}, Ignored
	}
}
