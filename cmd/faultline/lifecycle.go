package main

import (
	"log/slog"
	"sync/atomic"
)

type lifecycleState string

const eventState = "lifecycle.state"

const (
	stateStarting   lifecycleState = "starting"
	stateConnecting lifecycleState = "connecting"
	stateServing    lifecycleState = "serving"
	stateDraining   lifecycleState = "draining"
	stateAborting   lifecycleState = "aborting"
	stateStopped    lifecycleState = "stopped"
)

const (
	reasonStarted       = "started"
	reasonSignal        = "signal"
	reasonComponentExit = "component_exit"
	reasonShutdown      = "shutdown_complete"
	reasonLaunched      = "launched"
	reasonStartFailed   = "start_failed"
	reasonUnexplained   = "unexplained"
	reasonDialing       = "dialing"
)

type lifecycle struct {
	ready  atomic.Bool
	served atomic.Bool
	logger *slog.Logger
}

func newLifecycle(logger *slog.Logger) *lifecycle {
	return &lifecycle{
		logger: logger,
	}
}

func (l *lifecycle) transition(next lifecycleState, reason string) {
	l.ready.Store(next == stateServing)
	l.logger.Info(eventState, "state", string(next), "reason", reason)
}

func (l *lifecycle) Ready() bool {
	return l.ready.Load()
}

func (l *lifecycle) serve() {
	l.transition(stateServing, reasonStarted)
	l.served.Store(true)
}

func (l *lifecycle) finish(code int) {
	l.ready.Store(false)
	l.logger.Info(eventState, "state", string(stateStopped), "reason", reasonShutdown, "code", code)
}

func (l *lifecycle) Served() bool {
	return l.served.Load()
}
