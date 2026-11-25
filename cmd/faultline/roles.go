package main

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
)

const commandIngester = "ingester"

type ingesterSetup struct {
	Config  Config
	NATS    natsConfig
	Journal journal.Options
}

func runIngester(current process) int {
	role, life, err := prepareIngester(current)
	if err != nil {
		return fail(current.Stderr, err, exitConfig)
	}
	return launchStack(context.Background(), signalNotify, role, life)
}

func prepareIngester(current process) (*stack, *lifecycle, error) {
	setup, err := loadIngester(current)
	if err != nil {
		return nil, nil, err
	}
	logger := roleLogger(current.Stderr, setup.Config.LogLevel)
	life := newLifecycle(logger)
	role := ingesterStack(&setup, life, logger)
	if err := enforceRoleBudget(current, role, logger); err != nil {
		return nil, nil, err
	}
	return role, life, nil
}

func loadIngester(current process) (ingesterSetup, error) {
	cfg, rules, cfgErr := loadRoleConfig(current.Lookup)
	nats, natsErr := loadNATS(current.Lookup, current.Hostname)
	topology, streamsErr := loadStreams(current.Lookup)
	publish, publishErr := loadPublish(current.Lookup)
	if err := errors.Join(cfgErr, natsErr, streamsErr, publishErr); err != nil {
		return ingesterSetup{}, err
	}
	cfg.RouteRules = rules.Rules
	return ingesterSetup{
		Config: cfg,
		NATS:   nats,
		Journal: journal.Options{
			Tenant:         publish.Tenant,
			Topology:       topology,
			RulesVersion:   rules.Hash,
			PublishTimeout: publish.PublishTimeout,
			Clock:          systemClock{},
		},
	}, nil
}

func loadRoleConfig(lookup lookupFunc) (Config, routeRules, error) {
	cfg, err := Load(lookup)
	if err != nil {
		return Config{}, routeRules{}, err
	}
	rules, _ := loadRules(cfg.RouteRulesFile)
	return cfg, rules, nil
}

func ingesterStack(setup *ingesterSetup, life *lifecycle, logger *slog.Logger) *stack {
	role := &stack{}
	tracing, tracer := tracingLayer(&setup.Config, logger)
	conn, conns := connLayer(&setup.NATS, life, logger)
	publishing, journals := journalLayer(conns, &setup.Journal, life, logger)
	routes := roleRoutes(readiness{
		life:   life,
		layers: role.hooks,
	})
	role.layers = []layer{
		tracing,
		httpLayer(&setup.Config, tracer, routes, setup.Config.OTLPAddr, logger),
		conn,
		statsLayer(role.hooks, life.Served, selfalert.Thresholds{}, logger),
		publishing,
		otlpLayer(&setup.Config, journals, logger),
	}
	return role
}

func enforceRoleBudget(current process, role *stack, logger *slog.Logger) error {
	memory, err := enforceMemoryBudget(current, role.contributions())
	if err != nil {
		return err
	}
	logger.Info("config.memory_limit", "bytes", memory.Bytes, "source", memory.Source)
	return nil
}

func roleLogger(stderr io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{
		Level: level,
	}))
}
