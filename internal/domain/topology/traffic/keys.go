package traffic

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
)

type SubjectKind uint8

const (
	SubjectNode SubjectKind = iota + 1
	SubjectEdge
)

type SeriesKind uint8

const (
	SeriesSubject SeriesKind = iota + 1
	SeriesOperation
	SeriesOverflow
)

type SeriesKey struct {
	Kind      SeriesKind
	Subject   SubjectKind
	Caller    resource.ServiceRef
	Callee    resource.ServiceRef
	Protocol  operation.Protocol
	Operation string
}

type subjectKey struct {
	Subject int32
	Kind    SubjectKind
}

type operationKey struct {
	Subject   int32
	Kind      SubjectKind
	Operation cardinality.ID
}

func (k subjectKey) withOperation(id cardinality.ID) operationKey {
	return operationKey{
		Subject:   k.Subject,
		Kind:      k.Kind,
		Operation: id,
	}
}

func (k operationKey) subject() subjectKey {
	return subjectKey{
		Subject: k.Subject,
		Kind:    k.Kind,
	}
}
