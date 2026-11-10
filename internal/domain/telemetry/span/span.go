package span

import (
	"errors"
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/response"
)

var ErrInvalid = errors.New("invalid span")

const IdentityStringCount = 3

type Fields struct {
	TraceID      ids.TraceID
	SpanID       ids.SpanID
	ParentSpanID ids.SpanID
	Kind         kind.Kind
	StatusError  bool
	Start        uint64
	End          uint64
	Operation    operation.Operation
	Attributes   response.Attributes
}

type Span struct {
	Service             resource.ServiceRef
	TraceID             ids.TraceID
	SpanID              ids.SpanID
	ParentSpanID        ids.SpanID
	Interval            eventtime.Interval
	Operation           operation.Operation
	Kind                kind.Kind
	Failed              bool
	CallsExternalSystem bool
}

func New(source resource.Resource, fields *Fields) (Span, error) {
	if err := fields.validate(); err != nil {
		return Span{}, err
	}
	if err := source.Validate(); err != nil {
		return Span{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return Span{
		Service:             source.ServiceRef(),
		TraceID:             fields.TraceID,
		SpanID:              fields.SpanID,
		ParentSpanID:        fields.ParentSpanID,
		Interval:            eventtime.FromUnixNanos(fields.Start, fields.End),
		Operation:           fields.Operation,
		Kind:                fields.Kind.Normalized(),
		Failed:              fields.Attributes.Failed(fields.StatusError),
		CallsExternalSystem: fields.Attributes.CallsExternalSystem(),
	}, nil
}

func (s *Span) IdentityStrings() [IdentityStringCount]string {
	return [IdentityStringCount]string{
		s.Service.Namespace,
		s.Service.Name,
		s.Operation.Name,
	}
}

func (f *Fields) validate() error {
	switch {
	case f.TraceID.IsZero():
		return fmt.Errorf("%w: trace_id: zero", ErrInvalid)
	case f.SpanID.IsZero():
		return fmt.Errorf("%w: span_id: zero", ErrInvalid)
	}
	if err := f.Operation.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}
