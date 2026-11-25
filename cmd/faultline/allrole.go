package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
)

type allSetup struct {
	Config    Config
	NATS      natsConfig
	Graph     graphConfig
	Journal   journal.Options
	Partition partitionConfig
}

func runAll(current process) int {
	role, life, err := prepareAll(current)
	if err != nil {
		return fail(current.Stderr, err)
	}
	return launchStack(context.Background(), signalNotify, role, life)
}

func prepareAll(current process) (*stack, *lifecycle, error) {
	setup, err := loadAll(current)
	if err != nil {
		return nil, nil, err
	}
	logger := roleLogger(current.Stderr, setup.Config.LogLevel)
	life := newLifecycle(logger)
	role := allStack(&setup, life, logger)
	if err := enforceRoleBudget(current, role, logger); err != nil {
		return nil, nil, err
	}
	return role, life, nil
}

func loadAll(current process) (allSetup, error) {
	base, baseErr := loadBase(current)
	publish, publishErr := loadPublish(current.Lookup)
	graph, partition, processingErr := loadProcessing(current.Lookup, current.Lookup, &base)
	if err := errors.Join(baseErr, publishErr, processingErr); err != nil {
		return allSetup{}, err
	}
	return allSetup{
		Config:    base.Config,
		NATS:      base.NATS,
		Graph:     graph,
		Journal:   journalOptions(&base, &publish),
		Partition: partition,
	}, nil
}

func allStack(setup *allSetup, life *lifecycle, logger *slog.Logger) *stack {
	role := &stack{}
	graph := &memgraph.Store{}
	tracing, tracer := tracingLayer(&setup.Config, logger)
	conn, conns := connLayer(&setup.NATS, life, logger)
	publishing, journals := journalLayer(conns, &setup.Journal, life, logger)
	ready := readiness{
		life:   life,
		layers: role.hooks,
	}
	role.layers = []layer{
		tracing,
		graphAPILayer(&setup.Config, setup.Graph.APIResponseBudget, tracer, graph, ready, setup.Config.OTLPAddr, logger),
		conn,
		statsLayer(role.hooks, life.Served, processorThresholds(&setup.Partition), logger),
		observedPartition(&setup.Partition, conns, graph, life, logger),
		publishing,
		otlpLayer(&setup.Config, journals, logger),
	}
	return role
}
