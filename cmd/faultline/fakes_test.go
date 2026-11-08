package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	eventSignalStop     = "signal.stop"
	eventIngestGraceful = "otlp.graceful_stop"
	eventIngestStop     = "otlp.stop"
	eventJournalClose   = "journal.close"
	eventPipelineExit   = "pipeline.exit"
	eventAPIShutdown    = "api.shutdown"
	eventAPIClose       = "api.close"
	eventTraceFlush     = "tracing.shutdown"
	eventStatsExit      = "stats.exit"
	timelineFinalStats  = "final_stats"
)

var errCrash = errors.New("component crashed")

type eventLog struct {
	lock   sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) list() []string {
	l.lock.Lock()
	defer l.lock.Unlock()
	return slices.Clone(l.events)
}

type fakeServer struct {
	events       *eventLog
	name         string
	crash        chan error
	stopped      chan struct{}
	stopOnce     sync.Once
	hangGraceful bool
	hangShutdown bool
	ignoreStop   bool
	sentinel     error
}

func newFakeServer(events *eventLog, name string, sentinel error) *fakeServer {
	return &fakeServer{
		events:   events,
		name:     name,
		crash:    make(chan error, 1),
		stopped:  make(chan struct{}),
		sentinel: sentinel,
	}
}

func (s *fakeServer) Serve(net.Listener) error {
	select {
	case err := <-s.crash:
		return err
	case <-s.stopped:
		return s.sentinel
	}
}

func (s *fakeServer) release() {
	s.stopOnce.Do(func() {
		close(s.stopped)
	})
}

func (s *fakeServer) GracefulStop() {
	s.events.add(s.name + ".graceful_stop")
	if s.hangGraceful {
		<-s.stopped
		return
	}
	s.release()
}

func (s *fakeServer) Stop() {
	s.events.add(s.name + ".stop")
	if s.ignoreStop {
		return
	}
	s.release()
}

func (s *fakeServer) Shutdown(ctx context.Context) error {
	s.events.add(s.name + ".shutdown")
	if s.hangShutdown {
		<-ctx.Done()
		return ctx.Err()
	}
	s.release()
	return nil
}

func (s *fakeServer) Close() error {
	s.events.add(s.name + ".close")
	s.release()
	return nil
}

type recordingJournal struct {
	*memjournal.Journal
	events *eventLog
}

func (j recordingJournal) Close() {
	j.events.add(eventJournalClose)
	j.Journal.Close()
}

type recordingRunner struct {
	inner  pipelineRunner
	events *eventLog
	exit   string
}

func (r recordingRunner) Run(ctx context.Context) error {
	err := r.inner.Run(ctx)
	r.events.add(r.exit)
	return err
}

type runnerFunc func(ctx context.Context) error

func (f runnerFunc) Run(ctx context.Context) error {
	return f(ctx)
}

type feedFunc func(ctx context.Context, limit int) ([]processing.Delivery, error)

func (f feedFunc) Receive(ctx context.Context, limit int) ([]processing.Delivery, error) {
	return f(ctx, limit)
}

type logBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buffer.Write(p)
}

func (b *logBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.lock.Lock()
	defer b.lock.Unlock()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(b.buffer.Bytes()))
	for scanner.Scan() {
		record := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		records = append(records, record)
	}
	return records
}

func (b *logBuffer) timeline(t *testing.T) []string {
	t.Helper()
	var timeline []string
	for _, record := range b.records(t) {
		switch {
		case record["msg"] == "lifecycle.state":
			timeline = append(timeline, record["state"].(string))
		case record["msg"] == "pipeline.stats" && record["final"] == true:
			timeline = append(timeline, timelineFinalStats)
		}
	}
	return timeline
}

func (b *logBuffer) all(t *testing.T, name string) []map[string]any {
	t.Helper()
	var matched []map[string]any
	for _, record := range b.records(t) {
		if record["msg"] == name {
			matched = append(matched, record)
		}
	}
	return matched
}

func (b *logBuffer) event(t *testing.T, name string) map[string]any {
	t.Helper()
	for _, record := range b.records(t) {
		if record["msg"] == name {
			return record
		}
	}
	return nil
}

func testLogger(logs *logBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func newFakeIngest(events *eventLog) *fakeServer {
	return newFakeServer(events, componentOTLP, grpc.ErrServerStopped)
}

func newFakeAPI(events *eventLog) *fakeServer {
	return newFakeServer(events, componentAPI, http.ErrServerClosed)
}

type fakeTracing struct {
	events *eventLog
	hang   bool
}

func (f *fakeTracing) TracerProvider() trace.TracerProvider {
	return noop.NewTracerProvider()
}

func (f *fakeTracing) Shutdown(ctx context.Context) error {
	f.events.add(eventTraceFlush)
	if f.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

type fixedSnapshots struct {
	snapshot *topology.Snapshot
	err      error
}

func (f fixedSnapshots) Latest(context.Context) (*topology.Snapshot, error) {
	return f.snapshot, f.err
}

type fixedCounters struct {
	stats ingest.Stats
}

func (f fixedCounters) Stats() ingest.Stats {
	return f.stats
}

type fixedJournal struct {
	stats memjournal.Stats
}

func (f fixedJournal) Stats() memjournal.Stats {
	return f.stats
}
