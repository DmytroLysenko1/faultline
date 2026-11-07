package main

import (
	"context"
	"fmt"
	"time"
)

func (d *shutdown) stopIngest(ctx context.Context, pipelineDone <-chan struct{}) bool {
	d.parts.stopping(componentOTLP)
	stopped := make(chan struct{})
	go func() {
		d.sys.ingest.GracefulStop()
		close(stopped)
	}()
	stageCtx, cancel := context.WithTimeout(ctx, d.budget.OTLPGrace)
	defer cancel()
	return d.awaitIngest(stageCtx, stopped, pipelineDone)
}

func (d *shutdown) awaitIngest(ctx context.Context, stopped, pipelineDone <-chan struct{}) bool {
	pipelineDied := false
	for {
		select {
		case <-stopped:
			return pipelineDied
		case <-pipelineDone:
			pipelineDone, pipelineDied = nil, true
			d.sys.journal.Close()
		case <-ctx.Done():
			d.forced(componentOTLP, ctx.Err())
			d.sys.ingest.Stop()
			d.awaitForcedStop(stopped)
			return pipelineDied
		}
	}
}

func (d *shutdown) awaitForcedStop(stopped <-chan struct{}) {
	timer := time.NewTimer(d.budget.ExitGrace)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		d.forced(componentOTLP, errNotStopped)
		d.failures = append(d.failures, fmt.Errorf("%s: %w", componentOTLP, errNotStopped))
	}
}

func (d *shutdown) drainJournal(ctx context.Context) {
	d.parts.stopping(componentPipeline)
	d.sys.journal.Close()
	d.reportRemainder(d.awaitPipeline(ctx))
}

func (d *shutdown) awaitPipeline(ctx context.Context) bool {
	drainCtx, cancelDrain := context.WithTimeout(ctx, d.budget.Drain)
	defer cancelDrain()
	if d.parts.wait(drainCtx, componentPipeline) {
		return true
	}
	d.cancelPipeline()
	graceCtx, cancelGrace := context.WithTimeout(ctx, d.budget.CancelGrace)
	defer cancelGrace()
	return d.parts.wait(graceCtx, componentPipeline)
}

func (d *shutdown) reportRemainder(exact bool) {
	left := d.sys.journal.Stats()
	if left.Batches == 0 {
		return
	}
	d.failures = append(d.failures, errDrainIncomplete)
	d.sys.logger.Error("lifecycle.drain_incomplete",
		"spans", left.Spans,
		"batches", left.Batches,
		"bytes", left.Bytes,
		"exact", exact,
	)
}

func (d *shutdown) stopAPI(ctx context.Context) {
	d.parts.stopping(componentAPI)
	stageCtx, cancel := context.WithTimeout(ctx, d.budget.HTTPGrace)
	defer cancel()
	err := d.sys.api.Shutdown(stageCtx)
	if err == nil {
		return
	}
	d.forced(componentAPI, err)
	if err := d.sys.api.Close(); err != nil {
		d.forced(componentAPI, err)
	}
}

func (d *shutdown) awaitServers(ctx context.Context) {
	stageCtx, cancel := context.WithTimeout(ctx, d.budget.ExitGrace)
	defer cancel()
	for _, name := range []string{componentOTLP, componentAPI} {
		if !d.parts.wait(stageCtx, name) {
			d.forced(name, stageCtx.Err())
			d.failures = append(d.failures, fmt.Errorf("%s: %w", name, errNotStopped))
		}
	}
}
