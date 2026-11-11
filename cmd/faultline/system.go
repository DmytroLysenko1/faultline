package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/query"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const healthPattern = "GET /healthz"

type ingestServer interface {
	Serve(listener net.Listener) error
	GracefulStop()
	Stop()
}

type apiServer interface {
	Serve(listener net.Listener) error
	Shutdown(ctx context.Context) error
	Close() error
}

type spanJournal interface {
	Close()
	Stats() memjournal.Stats
}

type pipelineRunner interface {
	Run(ctx context.Context) error
}

type spanFeed interface {
	Receive(ctx context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error)
}

type statsRunner interface {
	Run(ctx context.Context) error
}

type selfTracing interface {
	TracerProvider() trace.TracerProvider
	Shutdown(ctx context.Context) error
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

type storage struct {
	journal *memjournal.Journal
	builder *topology.Builder
	store   *memgraph.Store
}

type system struct {
	journal   spanJournal
	ingest    ingestServer
	api       apiServer
	pipeline  pipelineRunner
	stats     statsRunner
	tracing   selfTracing
	store     *memgraph.Store
	lifecycle *lifecycle
	logger    *slog.Logger
}

type feedSource func(stored storage) spanFeed

type wiring struct {
	feedOf  feedSource
	tracing selfTracing
}

func newSystem(ctx context.Context, cfg *Config, logger *slog.Logger) (*system, error) {
	tracing, err := provideTracing(ctx, cfg.SelfTracingEndpoint, logger)
	if err != nil {
		return nil, err
	}
	sys, err := assemble(cfg, logger, wiring{
		feedOf:  journalFeed,
		tracing: tracing,
	})
	if err != nil {
		return nil, errors.Join(err, discardTracing(ctx, tracing))
	}
	return sys, nil
}

func provideTracing(ctx context.Context, endpoint string, logger *slog.Logger) (selfTracing, error) {
	if endpoint == "" {
		return selftrace.Disabled(), nil
	}
	tracing, err := selftrace.New(ctx, endpoint, logger)
	if err != nil {
		return nil, fmt.Errorf("build self-tracing: %w", err)
	}
	return tracing, nil
}

func discardTracing(ctx context.Context, tracing selfTracing) error {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), traceFlush)
	defer cancel()
	return tracing.Shutdown(flushCtx)
}

func journalFeed(stored storage) spanFeed {
	return stored.journal
}

func assemble(cfg *Config, logger *slog.Logger, seams wiring) (*system, error) {
	stored, err := provideStorage(cfg)
	if err != nil {
		return nil, err
	}
	state := newLifecycle(logger)
	api, err := provideAPI(stored.store, state, seams.tracing.TracerProvider(), logger, cfg.APIResponseBudget)
	if err != nil {
		return nil, err
	}
	server, counters := provideIngest(stored.journal, cfg.RouteRules, logger)
	pipeline, err := providePipeline(seams.feedOf(stored), stored, cfg)
	if err != nil {
		return nil, err
	}
	return &system{
		journal:   stored.journal,
		ingest:    server,
		api:       api,
		pipeline:  pipeline,
		stats:     provideStats(stored, counters, pipeline, cfg, logger),
		tracing:   seams.tracing,
		store:     stored.store,
		lifecycle: state,
		logger:    logger,
	}, nil
}

func provideStorage(cfg *Config) (storage, error) {
	journal, err := memjournal.New(cfg.Journal, systemClock{})
	if err != nil {
		return storage{}, fmt.Errorf("build journal: %w", err)
	}
	builder, err := topology.NewBuilder(&cfg.Topology, systemClock{}.Now())
	if err != nil {
		return storage{}, fmt.Errorf("build topology: %w", err)
	}
	store, err := memgraph.New(builder.Snapshot())
	if err != nil {
		return storage{}, fmt.Errorf("build graph store: %w", err)
	}
	return storage{
		journal: journal,
		builder: builder,
		store:   store,
	}, nil
}

func provideIngest(
	journal *memjournal.Journal,
	rules *operation.Rules,
	logger *slog.Logger,
) (ingestServer, *ingest.AcceptBatch) {
	accept := ingest.NewAcceptBatch(journal, operation.NewNormalizer(rules))
	return otlp.NewServer(accept, otlp.DefaultOptions(logger, systemClock{})), accept
}

func provideAPI(
	store *memgraph.Store,
	state *lifecycle,
	tracer trace.TracerProvider,
	logger *slog.Logger,
	responseBudget int64,
) (apiServer, error) {
	options := connectapi.DefaultOptions(logger)
	options.TracerProvider = tracer
	options.ResponseBudget = responseBudget
	mux, err := connectapi.NewMux(graphservice.NewHandler(query.NewGetGraph(store), logger), &options)
	if err != nil {
		return nil, fmt.Errorf("build api: %w", err)
	}
	mux.Handle(healthPattern, connectapi.Health(state))
	return connectapi.NewServer(mux), nil
}

func providePipeline(feed spanFeed, stored storage, cfg *Config) (*processing.Pipeline, error) {
	pipeline, err := processing.NewPipeline(feed, stored.builder, stored.store, systemClock{}, pipelineCadence(cfg))
	if err != nil {
		return nil, fmt.Errorf("build pipeline: %w", err)
	}
	return pipeline, nil
}

// pipelineCadence builds the Fire cadence from the same period traffic sizes its early panes by,
// and keeps the held batches small enough for the journal to take the largest request.
func pipelineCadence(cfg *Config) processing.Cadence {
	return processing.Cadence{
		FirePeriod:   cfg.Topology.EarlyFirePeriod,
		MaxHeldBytes: cfg.Journal.MaxBytes - otlp.MaxBatchFootprint(),
	}
}
