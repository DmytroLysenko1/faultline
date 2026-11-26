package main

import (
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	allSketchEntries = 2 << 20
	allTombstoneTTL  = 15 * time.Second
)

type allSetup struct {
	Config    Config
	NATS      natsConfig
	Graph     graphConfig
	Journal   journal.Options
	Partition partitionConfig
}

func allRole() roleRecipe[allSetup] {
	return roleRecipe[allSetup]{
		Load: loadAll,
		Level: func(setup *allSetup) slog.Level {
			return setup.Config.LogLevel
		},
		Build: allStack,
	}
}

func allDefaults(lookup lookupFunc) lookupFunc {
	return withDefaults(lookup, map[string]string{
		envSketchEntries: strconv.Itoa(allSketchEntries),
		envTombstoneTTL:  allTombstoneTTL.String(),
	})
}

func loadAll(current process) (allSetup, error) {
	base, baseErr := loadBase(current)
	publish, publishErr := loadPublish(current.Lookup)
	graph, partition, processingErr := loadProcessing(current.Lookup, allDefaults(current.Lookup), &base)
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
	conn.contributions = connContributions(record.MaxRecordBytes)
	publishing, journals := journalLayer(conns, &setup.Journal, life, logger)
	guard, taps := directoryLayer(conns, setup.Journal.Tenant, life, logger)
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
		guard,
		publishing,
		otlpLayer(&setup.Config, journals, taps, logger),
	}
	return role
}
