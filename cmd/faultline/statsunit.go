package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
)

const (
	eventPipelineStats = "pipeline.stats"
	finalReportBudget  = 100 * time.Millisecond
	componentStats     = "stats"
	statsInterval      = 30 * time.Second
)

type statsUnit struct {
	layers   func() []layer
	served   func() bool
	alerts   *selfalert.Evaluator
	clock    wallClock
	logger   *slog.Logger
	interval time.Duration
	sup      unitSupervisor
	cancel   context.CancelFunc
}

func statsLayer(
	layers func() []layer,
	served func() bool,
	thresholds selfalert.Thresholds,
	logger *slog.Logger,
) layer {
	return layer{
		name: layerStats,
		unit: &statsUnit{
			layers:   layers,
			served:   served,
			alerts:   selfalert.NewEvaluator(thresholds),
			clock:    systemClock{},
			logger:   logger,
			interval: statsInterval,
		},
		components: []string{componentStats},
		stopBudget: exitGrace + finalReportBudget,
	}
}

func (u *statsUnit) start(ctx context.Context, sup unitSupervisor) error {
	reportCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	u.sup = sup
	u.cancel = cancel
	sup.spawn(componentStats, func() error {
		return u.run(reportCtx)
	})
	return nil
}

func (u *statsUnit) run(ctx context.Context) error {
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			u.tick(ctx)
		}
	}
}

func (u *statsUnit) tick(ctx context.Context) {
	if !u.served() {
		return
	}
	u.evaluate(ctx)
	if ctx.Err() != nil {
		return
	}
	u.report(ctx, false)
}

func (u *statsUnit) stop(ctx context.Context) error {
	if u.cancel == nil {
		return nil
	}
	u.cancel()
	exited, err := u.awaitReporter(ctx)
	if !exited {
		return errStepAbandoned
	}
	if u.served() {
		u.report(ctx, true)
	}
	return err
}

func (u *statsUnit) awaitReporter(ctx context.Context) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, exitGrace)
	defer cancel()
	return u.sup.wait(waitCtx, componentStats)
}

func (u *statsUnit) evaluate(ctx context.Context) {
	sample := selfalert.Sample{
		Now: u.clock.Now(),
	}
	layers := u.layers()
	for index := range layers {
		if ctx.Err() != nil {
			return
		}
		fill := layers[index].sample
		if fill == nil {
			continue
		}
		fill(ctx, &sample)
	}
	logTransitions(ctx, u.logger, u.alerts.Evaluate(&sample))
}

func (u *statsUnit) report(ctx context.Context, final bool) {
	attrs := []slog.Attr{slog.Bool("final", final)}
	layers := u.layers()
	for index := range layers {
		if ctx.Err() != nil {
			return
		}
		groups := layers[index].statsGroups
		if groups == nil {
			continue
		}
		attrs = append(attrs, groups(ctx)...)
	}
	attrs = append(attrs, alertsGroup(u.alerts.Alerts()))
	u.logger.LogAttrs(ctx, slog.LevelInfo, eventPipelineStats, attrs...)
}
