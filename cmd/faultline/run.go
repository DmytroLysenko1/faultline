package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

const (
	otlpGrace   = 4500 * time.Millisecond
	drainGrace  = 2500 * time.Millisecond
	cancelGrace = 500 * time.Millisecond
	httpGrace   = 2 * time.Second
	exitGrace   = 100 * time.Millisecond
)

var (
	errUnexpectedExit  = errors.New("component exited unexpectedly")
	errDrainIncomplete = errors.New("journal not drained before exit")
	errNotStopped      = errors.New("component did not stop in time")
)

type shutdownBudget struct {
	OTLPGrace   time.Duration
	Drain       time.Duration
	CancelGrace time.Duration
	HTTPGrace   time.Duration
	ExitGrace   time.Duration
}

func defaultShutdownBudget() shutdownBudget {
	return shutdownBudget{
		OTLPGrace:   otlpGrace,
		Drain:       drainGrace,
		CancelGrace: cancelGrace,
		HTTPGrace:   httpGrace,
		ExitGrace:   exitGrace,
	}
}

type shutdown struct {
	sys            *system
	parts          *components
	budget         shutdownBudget
	cancelPipeline context.CancelFunc
	failures       []error
}

func run(
	ctx context.Context,
	stop context.CancelFunc,
	sys *system,
	open listeners,
	budget shutdownBudget,
) error {
	pipelineCtx, cancelPipeline := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelPipeline()
	down := &shutdown{
		sys:            sys,
		parts:          startComponents(pipelineCtx, sys, open),
		budget:         budget,
		cancelPipeline: cancelPipeline,
	}
	sys.lifecycle.transition(stateServing, reasonStarted)
	return down.await(ctx, stop)
}

func startComponents(pipelineCtx context.Context, sys *system, open listeners) *components {
	parts := newComponents(componentOTLP, componentAPI, componentPipeline)
	parts.start(componentOTLP, func() error {
		return withoutSentinel(sys.ingest.Serve(open.OTLP), grpc.ErrServerStopped)
	})
	parts.start(componentAPI, func() error {
		return withoutSentinel(sys.api.Serve(open.API), http.ErrServerClosed)
	})
	parts.start(componentPipeline, func() error {
		return sys.pipeline.Run(pipelineCtx)
	})
	return parts
}

func withoutSentinel(err, normal error) error {
	if errors.Is(err, normal) {
		return nil
	}
	return err
}

func (d *shutdown) await(ctx context.Context, stop context.CancelFunc) error {
	base := context.WithoutCancel(ctx)
	select {
	case <-ctx.Done():
		return d.afterSignal(base, stop)
	case exit := <-d.parts.firstExit():
		defer stop()
		d.record(exit)
		if exit.name == componentPipeline {
			return d.abort(base)
		}
		return d.drain(base, reasonComponentExit)
	}
}

func (d *shutdown) afterSignal(ctx context.Context, stop context.CancelFunc) error {
	if d.parts.hasExited(componentPipeline) {
		defer stop()
		return d.abort(ctx)
	}
	stop()
	return d.drain(ctx, reasonSignal)
}

func (d *shutdown) drain(ctx context.Context, reason string) error {
	d.sys.lifecycle.transition(stateDraining, reason)
	if pipelineDied := d.stopIngest(ctx, d.parts.exited(componentPipeline)); pipelineDied {
		d.reportRemainder(true)
	} else {
		d.drainJournal(ctx)
	}
	return d.finish(ctx)
}

func (d *shutdown) abort(ctx context.Context) error {
	d.sys.lifecycle.transition(stateAborting, reasonComponentExit)
	d.sys.journal.Close()
	d.stopIngest(ctx, nil)
	d.reportRemainder(true)
	return d.finish(ctx)
}

func (d *shutdown) finish(ctx context.Context) error {
	d.stopAPI(ctx)
	d.awaitServers(ctx)
	for _, exit := range d.parts.remainingExits() {
		d.record(exit)
	}
	d.sys.lifecycle.transition(stateStopped, reasonShutdown)
	return errors.Join(d.failures...)
}

func (d *shutdown) record(exit componentExit) {
	if !exit.unexpected && exit.err == nil {
		return
	}
	failure := exit.err
	if exit.unexpected {
		failure = errors.Join(errUnexpectedExit, exit.err)
	}
	d.failures = append(d.failures, fmt.Errorf("%s: %w", exit.name, failure))
	d.sys.logger.Error("lifecycle.component_exit", "component", exit.name, "error", failure)
}

func (d *shutdown) forced(component string, err error) {
	d.sys.logger.Warn("lifecycle.forced_stop", "component", component, "error", err)
}
