package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	preloadedBatches = 3
	harnessBatches   = 64
	harnessBytes     = 64 << 20
)

type harness struct {
	events  *eventLog
	logs    *logBuffer
	ingest  *fakeServer
	api     *fakeServer
	journal recordingJournal
	tracing *fakeTracing
	crash   chan error
	release chan struct{}
	sys     *system
	signal  context.CancelFunc
}

type pipelineKind func(t *testing.T, h *harness) pipelineRunner

func newHarness(t *testing.T, kind pipelineKind, preload int) *harness {
	t.Helper()
	journal, err := memjournal.New(memjournal.Limits{
		MaxBytes:   harnessBytes,
		MaxBatches: harnessBatches,
		IdleAfter:  defaultIdleAdvanceAfter,
	}, systemClock{})
	if err != nil {
		t.Fatalf("memjournal.New: %v", err)
	}
	for index := range preload {
		if err := journal.Append(t.Context(), slices.Clone(processingtest.Pairs(t, uint64(index), 1).Spans())); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	events, logs := &eventLog{}, &logBuffer{}
	h := &harness{
		events:  events,
		logs:    logs,
		ingest:  newFakeIngest(events),
		api:     newFakeAPI(events),
		journal: recordingJournal{Journal: journal, events: events},
		tracing: &fakeTracing{events: events},
		crash:   make(chan error, 1),
		release: make(chan struct{}),
	}
	h.sys = &system{
		journal:   h.journal,
		ingest:    h.ingest,
		api:       h.api,
		stats:     recordingRunner{inner: harnessStats(h), events: events, exit: eventStatsExit},
		tracing:   h.tracing,
		store:     nil,
		lifecycle: newLifecycle(testLogger(logs)),
		logger:    testLogger(logs),
	}
	h.sys.pipeline = recordingRunner{inner: kind(t, h), events: events, exit: eventPipelineExit}
	return h
}

func harnessStats(h *harness) *statsReporter {
	return &statsReporter{
		snapshots: fixedSnapshots{
			snapshot: &topology.Snapshot{},
		},
		ingest:   fixedCounters{},
		journal:  h.journal,
		logger:   testLogger(h.logs),
		interval: statsInterval,
	}
}

func (h *harness) stop() {
	h.events.add(eventSignalStop)
}

func drainingPipeline(t *testing.T, h *harness) pipelineRunner {
	t.Helper()
	builder := processingtest.NewBuilder(t)
	store, err := memgraph.New(builder.Snapshot())
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	return processing.NewPipeline(h.journal.Journal, builder, store)
}

func crashingPipeline(_ *testing.T, h *harness) pipelineRunner {
	return runnerFunc(func(ctx context.Context) error {
		select {
		case err := <-h.crash:
			return err
		case <-ctx.Done():
			return nil
		}
	})
}

func deafPipeline(_ *testing.T, _ *harness) pipelineRunner {
	return runnerFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
}

func stuckPipeline(_ *testing.T, h *harness) pipelineRunner {
	return runnerFunc(func(context.Context) error {
		<-h.release
		return nil
	})
}

func failingAfterDrainPipeline(_ *testing.T, h *harness) pipelineRunner {
	return runnerFunc(func(ctx context.Context) error {
		for {
			if _, err := h.journal.Receive(ctx, processing.MaxBatchesPerIteration, processing.ReceiveWait); err != nil {
				return errCrash
			}
		}
	})
}

type shutdownOutcome struct {
	Events          []string
	Timeline        []string
	ReporterInOrder bool
}

func reporterInOrder(events []string) bool {
	stats := slices.Index(events, eventStatsExit)
	return stats > slices.Index(events, eventPipelineExit) && stats < slices.Index(events, eventTraceFlush)
}

func withoutReporter(events []string) []string {
	return slices.DeleteFunc(slices.Clone(events), func(event string) bool {
		return event == eventStatsExit
	})
}

func runHarness(t *testing.T, h *harness, trigger func(h *harness)) (shutdownOutcome, error) {
	t.Helper()
	ctx, signal := context.WithCancel(t.Context())
	defer signal()
	h.signal = signal
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, h.stop, h.sys, listeners{}, defaultShutdownBudget())
	}()
	synctest.Wait()
	trigger(h)
	err := <-result
	return shutdownOutcome{
		Events:          withoutReporter(h.events.list()),
		Timeline:        h.logs.timeline(t),
		ReporterInOrder: reporterInOrder(h.events.list()),
	}, err
}

func signalled(h *harness) {
	h.signal()
}

func otlpCrashes(h *harness) {
	h.ingest.crash <- nil
}

func apiCrashes(h *harness) {
	h.api.crash <- errCrash
}

func pipelineCrashes(h *harness) {
	h.crash <- errCrash
}

func pipelineReturnsNil(h *harness) {
	h.crash <- nil
}

func TestShutdownOrder(t *testing.T) {
	type args struct {
		pipeline pipelineKind
		preload  int
		prepare  func(h *harness)
		trigger  func(h *harness)
	}
	tests := []struct {
		Name    string
		Args    args
		Want    shutdownOutcome
		WantErr error
	}{
		{
			Name: "signal drains the journal and stops cleanly",
			Args: args{
				pipeline: drainingPipeline,
				preload:  preloadedBatches,
				trigger:  signalled,
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventJournalClose, eventPipelineExit,
					eventAPIShutdown, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
		},
		{
			Name: "otlp exit drains and releases the signal last",
			Args: args{
				pipeline: drainingPipeline,
				trigger:  otlpCrashes,
			},
			Want: shutdownOutcome{
				Events: []string{eventIngestGraceful, eventJournalClose, eventPipelineExit, eventAPIShutdown,
					eventTraceFlush, eventSignalStop},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errUnexpectedExit,
		},
		{
			Name: "api exit drains and keeps its error",
			Args: args{
				pipeline: drainingPipeline,
				trigger:  apiCrashes,
			},
			Want: shutdownOutcome{
				Events: []string{eventIngestGraceful, eventJournalClose, eventPipelineExit, eventAPIShutdown,
					eventTraceFlush, eventSignalStop},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errCrash,
		},
		{
			Name: "pipeline exit closes the journal before stopping otlp",
			Args: args{
				pipeline: crashingPipeline,
				trigger:  pipelineReturnsNil,
			},
			Want: shutdownOutcome{
				Events: []string{eventPipelineExit, eventJournalClose, eventIngestGraceful, eventAPIShutdown,
					eventTraceFlush, eventSignalStop},
				Timeline:        []string{"serving", "aborting", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errUnexpectedExit,
		},
		{
			Name: "pipeline error aborts and keeps the error",
			Args: args{
				pipeline: crashingPipeline,
				trigger:  pipelineCrashes,
			},
			Want: shutdownOutcome{
				Events: []string{eventPipelineExit, eventJournalClose, eventIngestGraceful, eventAPIShutdown,
					eventTraceFlush, eventSignalStop},
				Timeline:        []string{"serving", "aborting", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errCrash,
		},
		{
			Name: "pipeline error after the journal closes is a failure",
			Args: args{
				pipeline: failingAfterDrainPipeline,
				trigger:  signalled,
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventJournalClose, eventPipelineExit,
					eventAPIShutdown, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errCrash,
		},
		{
			Name: "otlp that ignores graceful stop is stopped after 4.5 s",
			Args: args{
				pipeline: drainingPipeline,
				prepare: func(h *harness) {
					h.ingest.hangGraceful = true
				},
				trigger: signalled,
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventIngestStop, eventJournalClose,
					eventPipelineExit, eventAPIShutdown, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
		},
		{
			Name: "api that misses its shutdown deadline is closed",
			Args: args{
				pipeline: drainingPipeline,
				prepare: func(h *harness) {
					h.api.hangShutdown = true
				},
				trigger: signalled,
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventJournalClose, eventPipelineExit,
					eventAPIShutdown, eventAPIClose, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
		},
		{
			Name: "pipeline that ignores the closed journal is cancelled and leaves an exact remainder",
			Args: args{
				pipeline: deafPipeline,
				preload:  preloadedBatches,
				trigger:  signalled,
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventJournalClose, eventPipelineExit,
					eventAPIShutdown, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errDrainIncomplete,
		},
		{
			Name: "pipeline dying while otlp stops closes the journal at once",
			Args: args{
				pipeline: crashingPipeline,
				preload:  preloadedBatches,
				prepare: func(h *harness) {
					h.ingest.hangGraceful = true
				},
				trigger: func(h *harness) {
					h.signal()
					synctest.Wait()
					h.crash <- nil
				},
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventPipelineExit, eventJournalClose,
					eventIngestStop, eventAPIShutdown, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errDrainIncomplete,
		},
		{
			Name: "api crashing during the drain is not lost",
			Args: args{
				pipeline: deafPipeline,
				trigger: func(h *harness) {
					h.signal()
					synctest.Wait()
					h.api.crash <- errCrash
				},
			},
			Want: shutdownOutcome{
				Events: []string{eventSignalStop, eventIngestGraceful, eventJournalClose, eventPipelineExit,
					eventAPIShutdown, eventTraceFlush},
				Timeline:        []string{"serving", "draining", timelineFinalStats, "stopped"},
				ReporterInOrder: true,
			},
			WantErr: errCrash,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, tt.Args.pipeline, tt.Args.preload)
				if tt.Args.prepare != nil {
					tt.Args.prepare(h)
				}
				got, err := runHarness(t, h, tt.Args.trigger)
				if !errors.Is(err, tt.WantErr) {
					t.Fatalf("run = %v, want %v", err, tt.WantErr)
				}
				if diff := cmp.Diff(tt.Want, got); diff != "" {
					t.Errorf("shutdown (-want +got):\n%s", diff)
				}
			})
		})
	}
}
