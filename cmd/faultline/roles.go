package main

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
)

const commandIngester = "ingester"

type ingesterSetup struct {
	Config  Config
	NATS    natsConfig
	Journal journal.Options
}

type roleBase struct {
	Config  Config
	Rules   routeRules
	NATS    natsConfig
	Streams natsjs.Topology
}

func ingesterRole() roleRecipe[ingesterSetup] {
	return roleRecipe[ingesterSetup]{
		Load: loadIngester,
		Level: func(setup *ingesterSetup) slog.Level {
			return setup.Config.LogLevel
		},
		Build: ingesterStack,
	}
}

func loadIngester(current process) (ingesterSetup, error) {
	base, baseErr := loadBase(current)
	publish, publishErr := loadPublish(current.Lookup)
	if err := errors.Join(baseErr, publishErr); err != nil {
		return ingesterSetup{}, err
	}
	return ingesterSetup{
		Config:  base.Config,
		NATS:    base.NATS,
		Journal: journalOptions(&base, &publish),
	}, nil
}

func loadBase(current process) (roleBase, error) {
	cfg, rules, cfgErr := loadRoleConfig(current.Lookup)
	nats, natsErr := loadNATS(current.Lookup, current.Hostname)
	streams, streamsErr := loadStreams(current.Lookup)
	cfg.RouteRules = rules.Rules
	return roleBase{
		Config:  cfg,
		Rules:   rules,
		NATS:    nats,
		Streams: streams,
	}, errors.Join(cfgErr, natsErr, streamsErr)
}

func journalOptions(base *roleBase, publish *publishConfig) journal.Options {
	return journal.Options{
		Tenant:         publish.Tenant,
		Topology:       base.Streams,
		RulesVersion:   base.Rules.Hash,
		PublishTimeout: publish.PublishTimeout,
		Clock:          systemClock{},
	}
}

func loadRoleConfig(lookup lookupFunc) (Config, routeRules, error) {
	cfg, err := Load(lookup)
	if err != nil {
		return Config{}, routeRules{}, err
	}
	rules, err := loadRules(cfg.RouteRulesFile)
	if err != nil {
		return Config{}, routeRules{}, err
	}
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

type roleRecipe[S any] struct {
	Load  func(current process) (S, error)
	Level func(setup *S) slog.Level
	Build func(setup *S, life *lifecycle, logger *slog.Logger) *stack
}

func (r roleRecipe[S]) run(current process) int {
	role, life, err := prepareRole(current, r)
	if err != nil {
		return fail(current.Stderr, err)
	}
	return launchStack(context.Background(), signalNotify, role, life)
}

func prepareRole[S any](current process, recipe roleRecipe[S]) (*stack, *lifecycle, error) {
	setup, err := recipe.Load(current)
	if err != nil {
		return nil, nil, err
	}
	logger := roleLogger(current.Stderr, recipe.Level(&setup))
	life := newLifecycle(logger)
	role := recipe.Build(&setup, life, logger)
	if err := enforceRoleBudget(current, role, logger); err != nil {
		return nil, nil, err
	}
	return role, life, nil
}

func withDefaults(lookup lookupFunc, defaults map[string]string) lookupFunc {
	return func(name string) (string, bool) {
		value, set := lookup(name)
		if set {
			return value, set
		}
		fallback, known := defaults[name]
		return fallback, known
	}
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
