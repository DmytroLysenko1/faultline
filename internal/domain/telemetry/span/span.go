package span

import (
	"errors"
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/response"
)

var ErrInvalid = errors.New("invalid span")

type Fields struct {
	TraceID      ids.TraceID
	SpanID       ids.SpanID
	ParentSpanID ids.SpanID
	Kind         kind.Kind
	StatusError  bool
	Attributes   response.Attributes
}

type Span struct {
	Service             resource.ServiceRef
	TraceID             ids.TraceID
	SpanID              ids.SpanID
	ParentSpanID        ids.SpanID
	Kind                kind.Kind
	Failed              bool
	CallsExternalSystem bool
}

func New(source resource.Resource, fields Fields) (Span, error) {
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
		Kind:                fields.Kind.Normalized(),
		Failed:              fields.Attributes.Failed(fields.StatusError),
		CallsExternalSystem: fields.Attributes.CallsExternalSystem(),
	}, nil
}

func (s Span) IdentityStrings() [2]string {
	return [2]string{
		s.Service.Namespace,
		s.Service.Name,
	}
}

func (f Fields) validate() error {
	switch {
	case f.TraceID.IsZero():
		return fmt.Errorf("%w: trace_id: zero", ErrInvalid)
	case f.SpanID.IsZero():
		return fmt.Errorf("%w: span_id: zero", ErrInvalid)
	}
	return nil
}
