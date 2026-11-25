package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/query"
	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	commandProcessor = "processor"

	envFeedMaxBytes       = "FEED_MAX_BYTES"
	envCheckpointInterval = "CHECKPOINT_INTERVAL"
	envCheckpointDir      = "CHECKPOINT_DIR"

	defaultFeedMaxBytes       = 64 * mib
	minFeedMaxBytes           = 48 * mib
	maxFeedMaxBytes           = 512 * mib
	defaultCheckpointInterval = 10 * time.Second
	minCheckpointInterval     = time.Second
	maxCheckpointInterval     = time.Minute
	processorMaxPending       = 200_000
)

type processorSetup struct {
	Config    Config
	NATS      natsConfig
	Partition partitionConfig
}

func runProcessor(current process) int {
	role, life, err := prepareProcessor(current)
	if err != nil {
		return fail(current.Stderr, err)
	}
	return launchStack(context.Background(), signalNotify, role, life)
}

func prepareProcessor(current process) (*stack, *lifecycle, error) {
	setup, err := loadProcessor(current)
	if err != nil {
		return nil, nil, err
	}
	logger := roleLogger(current.Stderr, setup.Config.LogLevel)
	life := newLifecycle(logger)
	role := processorStack(&setup, life, logger)
	if err := enforceRoleBudget(current, role, logger); err != nil {
		return nil, nil, err
	}
	return role, life, nil
}

func loadProcessor(current process) (processorSetup, error) {
	cfg, rules, cfgErr := loadRoleConfig(processorDefaults(current.Lookup))
	nats, natsErr := loadNATS(current.Lookup, current.Hostname)
	streams, streamsErr := loadStreams(current.Lookup)
	partition, partitionErr := loadPartition(current.Lookup)
	if err := errors.Join(cfgErr, natsErr, streamsErr, partitionErr); err != nil {
		return processorSetup{}, err
	}
	partition.Instance = nats.Instance
	partition.Streams = streams
	partition.Limits = cfg.Topology
	partition.RulesHash = rules.Hash
	partition.IdleAfter = cfg.Journal.IdleAfter
	partition.RestoreBudget = restoreBudget(cfg.Topology.WorstCheckpointBytes())
	return processorSetup{
		Config:    cfg,
		NATS:      nats,
		Partition: partition,
	}, nil
}

func processorDefaults(lookup lookupFunc) lookupFunc {
	return func(name string) (string, bool) {
		value, set := lookup(name)
		if set || name != envPairingMaxPending {
			return value, set
		}
		return strconv.Itoa(processorMaxPending), true
	}
}

func loadPartition(lookup lookupFunc) (partitionConfig, error) {
	reader := newEnvReader(lookup)
	cfg := partitionConfig{
		FeedMaxBytes: reader.size(envFeedMaxBytes, defaultFeedMaxBytes, minFeedMaxBytes, maxFeedMaxBytes),
		CheckpointInterval: reader.duration(
			envCheckpointInterval,
			defaultCheckpointInterval,
			minCheckpointInterval,
			maxCheckpointInterval,
		),
		CheckpointDir: reader.path(envCheckpointDir),
		Recovery:      reader.recovery(envCheckpointRecovery),
	}
	reader.writableDir(envCheckpointDir, cfg.CheckpointDir)
	return cfg, errors.Join(reader.errs...)
}

func (r *envReader) writableDir(name, dir string) {
	if dir == "" {
		r.require(name)
		return
	}
	if err := probeWritable(dir); err != nil {
		r.fail(name, err)
	}
}

func probeWritable(dir string) error {
	probe, err := os.CreateTemp(dir, ".writable-*")
	if err != nil {
		return fmt.Errorf("probe write: %w", err)
	}
	return errors.Join(probe.Close(), os.Remove(probe.Name()))
}

func (r *envReader) recovery(name string) *processing.DiscardTarget {
	raw, set := r.value(name)
	if !set {
		return nil
	}
	target, err := parseRecovery(raw, 0)
	if err != nil {
		r.fail(name, err)
		return nil
	}
	return target
}

func processorStack(setup *processorSetup, life *lifecycle, logger *slog.Logger) *stack {
	role := &stack{}
	graph := &memgraph.Store{}
	tracing, tracer := tracingLayer(&setup.Config, logger)
	conn, conns := connLayer(&setup.NATS, life, logger)
	ready := readiness{
		life:   life,
		layers: role.hooks,
	}
	api := httpLayer(
		&setup.Config, tracer,
		graphRoutes(graph, ready, setup.Config.APIResponseBudget, logger),
		"", logger,
	)
	api.contributions = apiContributions(&setup.Config)
	role.layers = []layer{
		tracing,
		api,
		conn,
		statsLayer(role.hooks, life.Served, processorThresholds(&setup.Partition), logger),
		observedPartition(&setup.Partition, conns, graph, life, logger),
	}
	return role
}

func observedPartition(
	cfg *partitionConfig,
	source severableConns,
	graph *memgraph.Store,
	phases phaseLog,
	logger *slog.Logger,
) layer {
	observed, part := partitionLayer(cfg, source, graph, phases, logger)
	observed.contributions = partitionContributions(cfg)
	observed.statsGroups = part.statsGroups
	observed.sample = part.sample
	return observed
}

func graphRoutes(graph *memgraph.Store, ready readiness, responseBudget int64, logger *slog.Logger) apiRoutes {
	return func(tracer trace.TracerProvider) (http.Handler, error) {
		options := connectapi.DefaultOptions(logger)
		options.TracerProvider = tracer
		options.ResponseBudget = responseBudget
		mux, err := connectapi.NewMux(graphservice.NewHandler(query.NewGetGraph(graph), logger), &options)
		if err != nil {
			return nil, fmt.Errorf("build api: %w", err)
		}
		mux.Handle(healthPattern, connectapi.Health(alive{}))
		mux.Handle(readyPattern, connectapi.Health(ready))
		return mux, nil
	}
}

func apiContributions(cfg *Config) []contribution {
	return []contribution{
		{
			name:  "api responses",
			bytes: cfg.APIResponseBudget,
		},
	}
}

func processorThresholds(cfg *partitionConfig) selfalert.Thresholds {
	return selfalert.Thresholds{
		WatermarkLag: cfg.Limits.Windows.Delay + cfg.IdleAfter + cfg.Limits.EarlyFirePeriod + lagSlack,
	}
}

func partitionContributions(cfg *partitionConfig) []contribution {
	view := cfg.Limits.ViewHeapBytes()
	return []contribution{
		{
			name:  "feed credit and pull",
			bytes: cfg.FeedMaxBytes + feedPullMaxBytes,
		},
		{
			name:  "pairing, graph and traffic",
			bytes: cfg.Limits.WorstCaseHeapBytes(),
		},
		{
			name:  "snapshots",
			bytes: (memgraph.HeldSnapshots + processing.HeldSnapshots) * view,
		},
		{
			name:  "batch apply",
			bytes: topology.ApplyHeapBytes(otlp.MaxSpansPerRequest),
		},
	}
}
