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

	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
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
}

func (r recordingRunner) Run(ctx context.Context) error {
	err := r.inner.Run(ctx)
	r.events.add(eventPipelineExit)
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

func (b *logBuffer) states(t *testing.T) []string {
	t.Helper()
	var states []string
	for _, record := range b.records(t) {
		if record["msg"] == "lifecycle.state" {
			states = append(states, record["state"].(string))
		}
	}
	return states
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
