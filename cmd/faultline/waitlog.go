package main

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

const (
	lineBreak = "\n"
	lineJoin  = "; "
)

type streamWait struct {
	reminder
	warned    map[string]bool
	current   string
	lastError string
}

func (w *streamWait) details() []any {
	return []any{
		"reason", w.current,
		"last_error", w.lastError,
	}
}

type waitLog struct {
	state  lifecycleState
	prefix string
	phases phaseLog
	logger *slog.Logger
}

func (l waitLog) keep(ctx context.Context, wait *streamWait, reason string, err error) {
	wait.current = reason
	wait.lastError = strings.ReplaceAll(err.Error(), lineBreak, lineJoin)
	if wait.began.IsZero() {
		l.enter(wait, reason)
	}
	if !wait.warned[reason] {
		wait.warned[reason] = true
		l.logger.WarnContext(ctx, l.prefix+reason, "error", wait.lastError)
	}
	wait.remind(ctx, l.logger)
}

func (l waitLog) enter(wait *streamWait, reason string) {
	now := time.Now()
	wait.reminder = reminder{
		state:  l.state,
		level:  slog.LevelWarn,
		began:  now,
		next:   now.Add(waitReportPeriod),
		detail: wait.details,
	}
	l.phases.transition(l.state, reason)
}
