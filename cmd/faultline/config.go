package main

import (
	"errors"
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	envOTLPAddr          = "FAULTLINE_OTLP_GRPC_ADDR"
	envAPIAddr           = "FAULTLINE_API_ADDR"
	envJournalMaxBytes   = "FAULTLINE_JOURNAL_MAX_BYTES"
	envJournalMaxBatches = "FAULTLINE_JOURNAL_MAX_BATCHES"
	envIdleAdvanceAfter  = "FAULTLINE_IDLE_ADVANCE_AFTER"
	envPairingTTL        = "FAULTLINE_PAIRING_TTL"
	envPairingMaxPending = "FAULTLINE_PAIRING_MAX_PENDING"
	envTombstoneTTL      = "FAULTLINE_TOMBSTONE_TTL"
	envTombstoneMax      = "FAULTLINE_TOMBSTONE_MAX"
	envGraphMaxNodes     = "FAULTLINE_GRAPH_MAX_NODES"
	envGraphMaxEdges     = "FAULTLINE_GRAPH_MAX_EDGES"
	envLogLevel          = "FAULTLINE_LOG_LEVEL"
	envSelfTracing       = "FAULTLINE_SELF_TRACING_ENDPOINT"

	defaultOTLPAddr          = "127.0.0.1:4317"
	defaultAPIAddr           = "127.0.0.1:8080"
	defaultJournalMaxBytes   = 64 * mib
	minJournalMaxBytes       = 48 * mib
	maxJournalMaxBytes       = 512 * mib
	defaultJournalMaxBatches = 4096
	maxJournalMaxBatches     = 1_000_000
	defaultIdleAdvanceAfter  = 35 * time.Second
	minIdleAdvanceAfter      = 100 * time.Millisecond
	maxIdleAdvanceAfter      = 5 * time.Minute
	defaultPairingTTL        = 10 * time.Second
	minPairingTTL            = time.Second
	maxPairingTTL            = 5 * time.Minute
	defaultMaxPending        = 100_000
	maxMaxPending            = 1_000_000
	defaultTombstoneTTL      = time.Minute
	minTombstoneTTL          = time.Second
	maxTombstoneTTL          = 10 * time.Minute
	defaultTombstoneMax      = 1_000_000
	maxTombstoneMax          = 5_000_000
	defaultGraphMaxNodes     = 10_000
	maxGraphMaxNodes         = 20_000
	defaultGraphMaxEdges     = 50_000
	maxGraphMaxEdges         = 100_000
	minCount                 = 1
	defaultWindowSize        = 10 * time.Second
	defaultWatermarkDelay    = 30 * time.Second
	defaultAllowedLateness   = 30 * time.Second
	defaultSlackWindows      = 2
	defaultEarlyFirePeriod   = time.Second
	defaultMaxSeries         = 100_000
	defaultOperationsEach    = 500
	defaultSketchEntries     = 4 << 20
)

type Config struct {
	OTLPAddr            string
	APIAddr             string
	Journal             memjournal.Limits
	Topology            topology.Limits
	LogLevel            slog.Level
	SelfTracingEndpoint string
}

func Load(lookup lookupFunc) (Config, error) {
	reader := newEnvReader(lookup)
	cfg := Config{
		OTLPAddr:            reader.address(envOTLPAddr, defaultOTLPAddr),
		APIAddr:             reader.address(envAPIAddr, defaultAPIAddr),
		Journal:             journalLimits(reader),
		Topology:            topologyLimits(reader),
		LogLevel:            reader.level(envLogLevel, slog.LevelInfo),
		SelfTracingEndpoint: reader.endpoint(envSelfTracing),
	}
	reader.notBelow(envTombstoneTTL, cfg.Topology.TombstoneTTL, envPairingTTL, cfg.Topology.PairingTTL)
	reader.distinct(envSelfTracing, cfg.SelfTracingEndpoint, envOTLPAddr, cfg.OTLPAddr)
	return cfg, errors.Join(reader.errs...)
}

func journalLimits(reader *envReader) memjournal.Limits {
	return memjournal.Limits{
		MaxBytes:   reader.size(envJournalMaxBytes, defaultJournalMaxBytes, minJournalMaxBytes, maxJournalMaxBytes),
		MaxBatches: reader.count(envJournalMaxBatches, defaultJournalMaxBatches, maxJournalMaxBatches),
		IdleAfter:  reader.duration(envIdleAdvanceAfter, defaultIdleAdvanceAfter, minIdleAdvanceAfter, maxIdleAdvanceAfter),
	}
}

func topologyLimits(reader *envReader) topology.Limits {
	return topology.Limits{
		PairingTTL:    reader.duration(envPairingTTL, defaultPairingTTL, minPairingTTL, maxPairingTTL),
		MaxPending:    reader.count(envPairingMaxPending, defaultMaxPending, maxMaxPending),
		TombstoneTTL:  reader.duration(envTombstoneTTL, defaultTombstoneTTL, minTombstoneTTL, maxTombstoneTTL),
		MaxTombstones: reader.count(envTombstoneMax, defaultTombstoneMax, maxTombstoneMax),
		MaxNodes:      reader.count(envGraphMaxNodes, defaultGraphMaxNodes, maxGraphMaxNodes),
		MaxEdges:      reader.count(envGraphMaxEdges, defaultGraphMaxEdges, maxGraphMaxEdges),
		Windows: stream.Config{
			Size:            defaultWindowSize,
			Delay:           defaultWatermarkDelay,
			AllowedLateness: defaultAllowedLateness,
			SlackWindows:    defaultSlackWindows,
		},
		EarlyFirePeriod:         defaultEarlyFirePeriod,
		MaxSeries:               defaultMaxSeries,
		MaxOperationsPerService: defaultOperationsEach,
		SketchEntries:           defaultSketchEntries,
	}
}
