package call

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

type Party struct {
	Service   resource.ServiceRef
	Failed    bool
	Interval  eventtime.Interval
	Operation operation.Operation
}

type Call struct {
	Caller    resource.ServiceRef
	Callee    resource.ServiceRef
	Failed    bool
	Interval  eventtime.Interval
	Operation operation.Operation
}

type Unanswered struct {
	Service resource.ServiceRef
	Failed  bool
}

func Between(caller, callee *Party) Call {
	return Call{
		Caller:    caller.Service,
		Callee:    callee.Service,
		Failed:    caller.Failed || callee.Failed,
		Interval:  caller.Interval,
		Operation: callee.namedOr(caller),
	}
}

func (p *Party) namedOr(fallback *Party) operation.Operation {
	if p.Operation.IsZero() {
		return fallback.Operation
	}
	return p.Operation
}
