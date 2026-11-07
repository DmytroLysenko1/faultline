package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/query"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
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
	Receive(ctx context.Context, limit int) ([]processing.Delivery, error)
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
	store     *memgraph.Store
	lifecycle *lifecycle
	logger    *slog.Logger
}

type feedSource func(stored storage) spanFeed

func newSystem(cfg *Config, logger *slog.Logger) (*system, error) {
	return assemble(cfg, logger, journalFeed)
}

func journalFeed(stored storage) spanFeed {
	return stored.journal
}

func assemble(cfg *Config, logger *slog.Logger, feedOf feedSource) (*system, error) {
	stored, err := provideStorage(cfg)
	if err != nil {
		return nil, err
	}
	state := newLifecycle(logger)
	api, err := provideAPI(stored.store, state, logger)
	if err != nil {
		return nil, err
	}
	return &system{
		journal:   stored.journal,
		ingest:    provideIngest(stored.journal, logger),
		api:       api,
		pipeline:  providePipeline(feedOf(stored), stored),
		store:     stored.store,
		lifecycle: state,
		logger:    logger,
	}, nil
}

func provideStorage(cfg *Config) (storage, error) {
	journal, err := memjournal.New(cfg.Journal)
	if err != nil {
		return storage{}, fmt.Errorf("build journal: %w", err)
	}
	builder, err := topology.NewBuilder(cfg.Topology, systemClock{}.Now())
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

func provideIngest(journal *memjournal.Journal, logger *slog.Logger) ingestServer {
	accept := ingest.NewAcceptBatch(journal, systemClock{})
	return otlp.NewServer(accept, otlp.DefaultOptions(logger, systemClock{}))
}

func provideAPI(store *memgraph.Store, state *lifecycle, logger *slog.Logger) (apiServer, error) {
	options := connectapi.DefaultOptions(logger)
	mux, err := connectapi.NewMux(graphservice.NewHandler(query.NewGetGraph(store), logger), options)
	if err != nil {
		return nil, fmt.Errorf("build api: %w", err)
	}
	mux.Handle(healthPattern, connectapi.Health(state))
	return connectapi.NewServer(mux), nil
}

func providePipeline(feed spanFeed, stored storage) pipelineRunner {
	return processing.NewPipeline(feed, stored.builder, stored.store)
}
