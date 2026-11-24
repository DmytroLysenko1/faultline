package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

var (
	errUnexpectedExit        = errors.New("component exited unexpectedly")
	errDuplicateComponent    = errors.New("component name is already taken")
	errComponentOutlivedStop = errors.New("component outlived its layer stop")
)

const eventComponentExit = "lifecycle.component_exit"

type unitExitError struct {
	name string
	err  error
}

func (e *unitExitError) Error() string {
	prefix := e.name + ": " + errUnexpectedExit.Error()
	if e.err == nil {
		return prefix
	}
	return prefix + ": " + e.err.Error()
}

func (e *unitExitError) Unwrap() []error {
	if e.err == nil {
		return []error{errUnexpectedExit}
	}
	return []error{errUnexpectedExit, e.err}
}

type supervised struct {
	stopping   bool
	unexpected bool
	claimed    bool
	err        error
	done       chan struct{}
}

func newSupervised() *supervised {
	return &supervised{
		done: make(chan struct{}),
	}
}

type supervisor struct {
	mu             sync.Mutex
	fail           context.CancelCauseFunc
	logger         *slog.Logger
	units          map[string]*supervised
	incidentLogged bool
	snapshotted    bool
	first          error
	lateExit       error
}

func newSupervisor(fail context.CancelCauseFunc, logger *slog.Logger) *supervisor {
	return &supervisor{
		fail:   fail,
		logger: logger,
		units:  make(map[string]*supervised),
	}
}

func (s *supervisor) spawn(name string, run func() error) {
	current, fresh := s.register(name)
	if !fresh {
		s.refuse(name, errDuplicateComponent)
		return
	}
	go func() {
		s.exited(name, current, run())
	}()
}

func (s *supervisor) refuse(name string, err error) {
	s.exited(name, newSupervised(), err)
}

func (s *supervisor) register(name string) (*supervised, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, taken := s.units[name]; taken {
		return nil, false
	}
	current := newSupervised()
	s.units[name] = current
	return current, true
}

func (s *supervisor) exited(name string, current *supervised, err error) {
	defer close(current.done)
	exit, opens := s.record(name, current, err)
	if exit == nil {
		return
	}
	s.logExit(exit, opens)
	s.fail(exit)
}

func (s *supervisor) record(name string, current *supervised, err error) (*unitExitError, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current.err = err
	if current.stopping {
		return nil, false
	}
	current.unexpected = true
	exit := &unitExitError{
		name: name,
		err:  err,
	}
	opens := !s.incidentLogged
	s.incidentLogged = true
	s.keep(exit)
	return exit, opens
}

func (s *supervisor) keep(exit *unitExitError) {
	if s.snapshotted && s.lateExit == nil {
		s.lateExit = exit
	}
	if !s.snapshotted && s.first == nil {
		s.first = exit
	}
}

func (s *supervisor) logExit(exit *unitExitError, opens bool) {
	if opens &&
		!errors.Is(exit, processing.ErrLeaseLost) {
		s.logger.Error(eventComponentExit,
			"component", exit.name,
			"error", exit,
		)
		return
	}
	s.logger.Warn(eventComponentExit,
		"component", exit.name,
		"error", exit,
		"followup", true,
	)
}

func (s *supervisor) stopping(names ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		if current, known := s.units[name]; known {
			current.stopping = true
		}
	}
}

func (s *supervisor) wait(ctx context.Context, name string) (bool, error) {
	current, known := s.lookup(name)
	if !known {
		return true, nil
	}
	awaitExit(ctx, current)
	return s.claim(current)
}

func (s *supervisor) outlived(ctx context.Context, names ...string) error {
	var failures []error
	for _, name := range names {
		if err := s.unclaimed(ctx, name); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", name, err))
		}
	}
	return errors.Join(failures...)
}

func (s *supervisor) unclaimed(ctx context.Context, name string) error {
	current, known := s.lookup(name)
	if !known {
		return nil
	}
	awaitExit(ctx, current)
	if !hasExited(current) {
		return errComponentOutlivedStop
	}
	return s.leftover(current)
}

func (s *supervisor) lookup(name string) (*supervised, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, known := s.units[name]
	return current, known
}

func (s *supervisor) claim(current *supervised) (bool, error) {
	if !hasExited(current) {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current.claimed = true
	return true, current.stopError()
}

func (s *supervisor) leftover(current *supervised) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current.claimed {
		return nil
	}
	current.claimed = true
	return current.stopError()
}

func (u *supervised) stopError() error {
	if u.unexpected {
		return nil
	}
	return u.err
}

func (s *supervisor) snapshot(decide func(unit error) exitOutcome) exitOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotted = true
	outcome := decide(s.first)
	if outcome.level == slog.LevelError {
		s.incidentLogged = true
	}
	return outcome
}

func (s *supervisor) late() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lateExit
}

func (s *supervisor) incidentOpened() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.incidentLogged
}

func hasExited(current *supervised) bool {
	select {
	case <-current.done:
		return true
	default:
		return false
	}
}

func awaitExit(ctx context.Context, current *supervised) {
	select {
	case <-current.done:
	case <-ctx.Done():
	}
}
