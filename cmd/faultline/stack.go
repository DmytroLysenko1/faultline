package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
)

const (
	stageServing  = "serving"
	stageStarting = "starting"

	eventLayerStopped    = "lifecycle.layer_stopped"
	eventLayerStopFailed = "lifecycle.layer_stop_failed"

	exitGrace = 100 * time.Millisecond
)

var (
	errStepAbandoned       = errors.New("stop step abandoned at its budget")
	errStopAbandoned       = errors.New("layer stop abandoned past its budget")
	errUndeclaredComponent = errors.New("component is not declared by its layer")
)

type unitSupervisor interface {
	spawn(name string, run func() error)
	wait(ctx context.Context, name string) (bool, error)
}

type unit interface {
	start(ctx context.Context, sup unitSupervisor) error
	stop(ctx context.Context) error
}

type layer struct {
	name          string
	unit          unit
	components    []string
	stopBudget    time.Duration
	contributions []contribution
	statsGroups   func(ctx context.Context) []slog.Attr
	sample        func(ctx context.Context, sample *selfalert.Sample)
	ready         func() bool
}

func (l *layer) supervisedBy(sup *supervisor) layerSupervisor {
	return layerSupervisor{
		sup:        sup,
		layer:      l.name,
		components: l.components,
	}
}

func (l *layer) stopLogged(base context.Context, sup *supervisor, logger *slog.Logger) error {
	began := time.Now()
	err := l.stopWithin(base, sup)
	elapsed := time.Since(began)
	if err != nil {
		logger.WarnContext(base, eventLayerStopFailed,
			"layer", l.name,
			slog.Float64("elapsed_seconds", elapsed.Seconds()),
			"error", err,
		)
		return err
	}
	logger.InfoContext(base, eventLayerStopped,
		"layer", l.name,
		slog.Float64("elapsed_seconds", elapsed.Seconds()),
	)
	return nil
}

func (l *layer) stopAndAwait(ctx context.Context, sup *supervisor) error {
	err := l.unit.stop(ctx)
	return errors.Join(err, sup.outlived(ctx, l.components...))
}

func (l *layer) stopWithin(base context.Context, sup *supervisor) error {
	ctx, cancel := context.WithTimeout(base, l.stopBudget)
	defer cancel()
	stopped := make(chan error, 1)
	go func() {
		stopped <- l.stopAndAwait(ctx, sup)
	}()
	watchdog := time.NewTimer(l.stopBudget + exitGrace)
	defer watchdog.Stop()
	select {
	case err := <-stopped:
		return err
	case <-watchdog.C:
		return errStopAbandoned
	}
}

type stack struct {
	layers  []layer
	entered int
}

func (s *stack) declaredOnce() error {
	duplicates := lo.FindDuplicates(s.components())
	if len(duplicates) == 0 {
		return nil
	}
	return &stageError{
		stage: stageStarting,
		err:   fmt.Errorf("%s: %w", strings.Join(duplicates, ", "), errDuplicateComponent),
	}
}

func (s *stack) hooks() []layer {
	return s.layers
}

func (s *stack) start(ctx context.Context, sup *supervisor) error {
	if err := s.declaredOnce(); err != nil {
		return err
	}
	for index := range s.layers {
		if err := s.enter(ctx, sup, index); err != nil {
			return err
		}
	}
	return nil
}

func (s *stack) components() []string {
	var names []string
	for index := range s.layers {
		names = append(names, s.layers[index].components...)
	}
	return names
}

func (s *stack) contributions() []contribution {
	return lo.FlatMap(s.layers, func(current layer, _ int) []contribution {
		return current.contributions
	})
}

func (s *stack) enter(ctx context.Context, sup *supervisor, index int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.entered = index + 1
	current := &s.layers[index]
	return current.unit.start(
		ctx,
		current.supervisedBy(sup),
	)
}

func (s *stack) stop(base context.Context, sup *supervisor, logger *slog.Logger) []stopFailure {
	var failures []stopFailure
	for index := range slices.Backward(s.layers[:s.entered]) {
		current := &s.layers[index]
		sup.stopping(current.components...)
		if err := current.stopLogged(base, sup, logger); err != nil {
			failures = append(failures, stopFailure{
				layer: current.name,
				err:   err,
			},
			)
		}
	}
	return failures
}

func (s *stack) stageAt(served bool) string {
	if served {
		return stageServing
	}
	if s.entered == 0 {
		return stageStarting
	}
	return s.layers[s.entered-1].name
}

type stageError struct {
	stage string
	err   error
}

func (e *stageError) Error() string {
	return e.stage + ": " + e.err.Error()
}

func (e *stageError) Unwrap() error {
	return e.err
}

type stopFailure struct {
	layer string
	err   error
}

type layerSupervisor struct {
	sup        *supervisor
	layer      string
	components []string
}

func (v layerSupervisor) spawn(name string, run func() error) {
	if slices.Contains(v.components, name) {
		v.sup.spawn(name, run)
		return
	}
	v.sup.refuse(name, fmt.Errorf("layer %s: %w", v.layer, errUndeclaredComponent))
}

func (v layerSupervisor) wait(ctx context.Context, name string) (bool, error) {
	if slices.Contains(v.components, name) {
		return v.sup.wait(ctx, name)
	}
	return true, fmt.Errorf("wait %s: %w", name, errUndeclaredComponent)
}

func withoutSentinel(err, normal error) error {
	if errors.Is(err, normal) {
		return nil
	}
	return err
}
