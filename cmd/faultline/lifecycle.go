package main

import (
	"log/slog"
	"sync/atomic"
)

type lifecycleState string

const (
	stateServing  lifecycleState = "serving"
	stateDraining lifecycleState = "draining"
	stateAborting lifecycleState = "aborting"
	stateStopped  lifecycleState = "stopped"
)

const (
	reasonStarted       = "started"
	reasonSignal        = "signal"
	reasonComponentExit = "component_exit"
	reasonShutdown      = "shutdown_complete"
)

type lifecycle struct {
	ready  atomic.Bool
	logger *slog.Logger
}

func newLifecycle(logger *slog.Logger) *lifecycle {
	return &lifecycle{
		logger: logger,
	}
}

func (l *lifecycle) transition(next lifecycleState, reason string) {
	l.ready.Store(next == stateServing)
	l.logger.Info("lifecycle.state", "state", string(next), "reason", reason)
}

func (l *lifecycle) Ready() bool {
	return l.ready.Load()
}
