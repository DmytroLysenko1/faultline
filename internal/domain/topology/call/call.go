package call

import "github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"

type Party struct {
	Service resource.ServiceRef
	Failed  bool
}

type Call struct {
	Caller resource.ServiceRef
	Callee resource.ServiceRef
	Failed bool
}

type Unanswered struct {
	Service resource.ServiceRef
	Failed  bool
}

func Between(caller, callee Party) Call {
	return Call{
		Caller: caller.Service,
		Callee: callee.Service,
		Failed: caller.Failed || callee.Failed,
	}
}
