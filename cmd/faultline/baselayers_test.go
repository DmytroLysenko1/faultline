package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

var (
	errServe  = errors.New("serve failed on stop")
	errRoutes = errors.New("routes failed")
	errFlush  = errors.New("flush failed")
	errClose  = errors.New("close failed")
)

type noopTracer struct{}

func (noopTracer) tracerProvider() trace.TracerProvider {
	return noop.NewTracerProvider()
}

func healthRoutes(trace.TracerProvider) (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc(healthPath, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	return mux, nil
}

func brokenRoutes(trace.TracerProvider) (http.Handler, error) {
	return nil, errRoutes
}

func newLayerSupervisor(t *testing.T, logs *logBuffer, current *layer) layerSupervisor {
	t.Helper()
	_, failStart := context.WithCancelCause(t.Context())
	t.Cleanup(func() {
		failStart(nil)
	})
	return current.supervisedBy(newSupervisor(failStart, testLogger(logs)))
}

func stopLayer(t *testing.T, current *layer, view layerSupervisor) error {
	t.Helper()
	view.sup.stopping(current.components...)
	ctx, cancel := context.WithTimeout(t.Context(), current.stopBudget)
	defer cancel()
	return current.unit.stop(ctx)
}

func TestTheRealLayersStopWithoutALateExit(t *testing.T) {
	tests := []struct {
		name string
		args func(t *testing.T, life *lifecycle, logs *logBuffer) *stack
	}{
		{
			name: "tracing, http and stats",
			args: baseStack,
		},
		{
			name: "the ingester on NATS",
			args: func(t *testing.T, life *lifecycle, logs *logBuffer) *stack {
				cluster := natsjstest.Start(t, singleNATSNode)
				ensureStreams(t, cluster.URLs()...)
				grantTestToken(t, defaultTenant, cluster.URLs()...)
				setup, _, _ := ingesterOn(t, cluster.URLs())
				return ingesterStack(setup, life, testLogger(logs))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs, notify := &logBuffer{}, newFakeNotify()
			life := newLifecycle(testLogger(logs))
			done := launchRoleWith(t, tt.args(t, life, logs), notify.notify, life)
			eventually(t, "serving", life.Served)
			notify.send()
			assertCode(t, <-done, exitOK, logs)
			assertNoLine(t, logs, eventComponentExit)
			assertNoLine(t, logs, eventForcedStop)
			assertNoLine(t, logs, eventLayerStopFailed)
		})
	}
}

func baseStack(_ *testing.T, life *lifecycle, logs *logBuffer) *stack {
	logger := testLogger(logs)
	cfg := &Config{
		APIAddr: loopback,
	}
	role := &stack{}
	tracing, traced := tracingLayer(cfg, logger)
	role.layers = []layer{
		tracing,
		httpLayer(cfg, traced, healthRoutes, "", logger),
		statsLayer(role.hooks, life.Served, calmThresholds, logger),
	}
	return role
}

func TestABrokenTracingEndpointFailsItsStage(t *testing.T) {
	logs := &logBuffer{}
	current, _ := tracingLayer(&Config{
		SelfTracingEndpoint: "%zz",
	}, testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	err := current.unit.start(t.Context(), view)
	failure, found := errors.AsType[*stageError](err)
	if !found || failure.stage != stageTracingStart {
		t.Fatalf("start = %v, want the %s stage", err, stageTracingStart)
	}
	if err := stopLayer(t, &current, view); err != nil {
		t.Errorf("stop of a tracing layer that never started = %v, want nil", err)
	}
}

func TestARealLayerKeepsASignalDuringItsStart(t *testing.T) {
	tests := []struct {
		Name string
		Args func(cfg *Config, logs *logBuffer) layer
	}{
		{
			Name: "tracing",
			Args: func(cfg *Config, logs *logBuffer) layer {
				tracing, _ := tracingLayer(cfg, testLogger(logs))
				return tracing
			},
		},
		{
			Name: "http",
			Args: func(cfg *Config, logs *logBuffer) layer {
				return httpLayer(cfg, noopTracer{}, healthRoutes, "", testLogger(logs))
			},
		},
		{
			Name: "stats",
			Args: func(_ *Config, logs *logBuffer) layer {
				return statsLayer(func() []layer { return nil }, neverServed, calmThresholds, testLogger(logs))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			logs := &logBuffer{}
			current := tt.Args(&Config{
				APIAddr: loopback,
			}, logs)
			view := newLayerSupervisor(t, logs, &current)
			signalled, cancel := context.WithCancelCause(t.Context())
			cancel(errTestSignal)
			err := current.unit.start(signalled, view)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("start = %v, want nil or a cancel", err)
			}
			seen := &observed{
				err:    err,
				stage:  current.name,
				cause:  errTestSignal,
				signal: errTestSignal,
			}
			if code := outcomeOf(seen).code; code != exitOK {
				t.Errorf("code = %d, want %d for %v", code, exitOK, err)
			}
			if err := stopLayer(t, &current, view); err != nil {
				t.Errorf("stop = %v, want nil", err)
			}
		})
	}
}

func TestTheHTTPLayerServesUntilStopped(t *testing.T) {
	logs := &logBuffer{}
	address := freeAddress(t)
	current := httpLayer(&Config{
		APIAddr: address,
	}, noopTracer{}, healthRoutes, "", testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	if err := current.unit.start(t.Context(), view); err != nil {
		t.Fatalf("start = %v", err)
	}
	client := &http.Client{}
	if status := getStatus(t, client, address); status != http.StatusOK {
		t.Errorf("GET %s = %d, want %d", healthPath, status, http.StatusOK)
	}
	client.CloseIdleConnections()
	if err := stopLayer(t, &current, view); err != nil {
		t.Fatalf("stop = %v, want nil", err)
	}
	var dialer net.Dialer
	if connection, err := dialer.DialContext(t.Context(), network, address); err == nil {
		closeErr := connection.Close()
		t.Fatalf("the stopped API still accepts connections (close: %v)", closeErr)
	}
}

func getStatus(t *testing.T, client *http.Client, address string) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address+healthPath, http.NoBody)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close body: %v", err)
	}
	return response.StatusCode
}

func TestTheHTTPLayerReturnsItsServersStopError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		current := layer{
			components: []string{componentAPI},
			stopBudget: httpGrace + exitGrace,
		}
		view := newLayerSupervisor(t, logs, &current)
		server := newFakeServer(&eventLog{}, componentAPI, errServe)
		current.unit = &apiUnit{
			logger: testLogger(logs),
			sup:    view,
			server: server,
		}
		view.spawn(componentAPI, func() error {
			return withoutSentinel(server.Serve(nil), http.ErrServerClosed)
		})
		if err := stopLayer(t, &current, view); !errors.Is(err, errServe) {
			t.Fatalf("stop = %v, want %v", err, errServe)
		}
	})
}

func startedAPI(t *testing.T, cfg *Config, routes apiRoutes, probe string) error {
	t.Helper()
	logs := &logBuffer{}
	current := httpLayer(cfg, noopTracer{}, routes, probe, testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	err := current.unit.start(t.Context(), view)
	if stopErr := stopLayer(t, &current, view); stopErr != nil {
		t.Errorf("stop = %v, want nil", stopErr)
	}
	return err
}

func TestTheOTLPProbeLeavesItsPortFree(t *testing.T) {
	probe := freeAddress(t)
	if err := startedAPI(t, &Config{
		APIAddr: loopback,
	}, healthRoutes, probe); err != nil {
		t.Fatalf("start = %v", err)
	}
	assertFree(t, probe, "")
}

func TestABusyAPIPortFailsTheListenStage(t *testing.T) {
	busy := occupy(t)
	err := startedAPI(t, &Config{
		APIAddr: busy,
	}, healthRoutes, "")
	failure, found := errors.AsType[*stageError](err)
	if !found || failure.stage != stageHTTPListen || !strings.Contains(err.Error(), busy) {
		t.Fatalf("start = %v, want the %s stage naming %s", err, stageHTTPListen, busy)
	}
}

func TestABusyOTLPPortFailsBeforeTheAPIServes(t *testing.T) {
	address, busy := freeAddress(t), occupy(t)
	err := startedAPI(t, &Config{
		APIAddr: address,
	}, healthRoutes, busy)
	failure, found := errors.AsType[*stageError](err)
	if !found || failure.stage != stageHTTPProbe || !strings.Contains(err.Error(), busy) {
		t.Fatalf("start = %v, want the %s stage naming %s", err, stageHTTPProbe, busy)
	}
	assertFree(t, address, busy)
}

func TestBrokenRoutesFreeTheAPIPort(t *testing.T) {
	address := freeAddress(t)
	err := startedAPI(t, &Config{
		APIAddr: address,
	}, brokenRoutes, "")
	failure, found := errors.AsType[*stageError](err)
	if !found || failure.stage != stageHTTPRoutes || !errors.Is(err, errRoutes) {
		t.Fatalf("start = %v, want the %s stage carrying %v", err, stageHTTPRoutes, errRoutes)
	}
	assertFree(t, address, "")
}

func TestAStuckShutdownFallsBackToClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, events := &logBuffer{}, &eventLog{}
		current := httpLayer(&Config{}, noopTracer{}, healthRoutes, "", testLogger(logs))
		view := newLayerSupervisor(t, logs, &current)
		server := newFakeAPI(events)
		server.hangShutdown = true
		current.unit = &apiUnit{
			logger: testLogger(logs),
			sup:    view,
			server: server,
		}
		view.spawn(componentAPI, func() error {
			return withoutSentinel(server.Serve(nil), http.ErrServerClosed)
		})
		began := time.Now()
		err := stopLayer(t, &current, view)
		elapsed := time.Since(began)
		if err != nil {
			t.Fatalf("stop = %v, want nil", err)
		}
		assertDiff(t, "stop elapsed", httpGrace, elapsed)
		assertDiff(t, "server calls", []string{componentAPI + ".shutdown", componentAPI + ".close"}, events.list())
		assertDiff(t, "forced_stop lines", []map[string]any{{
			"level": levelWarn,
			"layer": layerHTTP,
		}},
			picked(logs.all(t, eventForcedStop), "level", "layer"))
	})
}

type failingFlush struct{}

func (failingFlush) TracerProvider() trace.TracerProvider {
	return noop.NewTracerProvider()
}

func (failingFlush) Shutdown(context.Context) error {
	return errFlush
}

func TestATracingFlushFailureIsAWarning(t *testing.T) {
	logs := &logBuffer{}
	traced := &tracingUnit{
		logger:  testLogger(logs),
		tracing: failingFlush{},
	}
	if err := traced.stop(t.Context()); err != nil {
		t.Fatalf("stop = %v, want nil", err)
	}
	want := []map[string]any{{
		"level": levelWarn,
		"layer": layerTracing,
		"error": errFlush.Error(),
	}}
	assertDiff(t, "forced_stop lines", want, picked(logs.all(t, eventForcedStop), "level", "layer", "error"))
	if err := (&tracingUnit{}).stop(t.Context()); err != nil {
		t.Errorf("stop of an unstarted tracing layer = %v, want nil", err)
	}
}

type unclosableServer struct {
	*fakeServer
}

func (s unclosableServer) Close() error {
	s.events.add(s.name + ".close")
	return nil
}

func TestAClosedServerThatKeepsServingIsAbandoned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		current := httpLayer(&Config{}, noopTracer{}, healthRoutes, "", testLogger(logs))
		view := newLayerSupervisor(t, logs, &current)
		inner := newFakeAPI(&eventLog{})
		inner.hangShutdown = true
		server := unclosableServer{inner}
		current.unit = &apiUnit{
			logger: testLogger(logs),
			sup:    view,
			server: server,
		}
		view.spawn(componentAPI, func() error {
			return withoutSentinel(server.Serve(nil), http.ErrServerClosed)
		})
		err := stopLayer(t, &current, view)
		inner.release()
		synctest.Wait()
		if !errors.Is(err, errStepAbandoned) {
			t.Fatalf("stop = %v, want %v", err, errStepAbandoned)
		}
		assertDiff(t, "forced_stop lines", []map[string]any{{
			"level": levelWarn,
			"layer": layerHTTP,
		}},
			picked(logs.all(t, eventForcedStop), "level", "layer"))
	})
}

func TestAFailedCloseIsASecondForcedStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		current := httpLayer(&Config{}, noopTracer{}, healthRoutes, "", testLogger(logs))
		view := newLayerSupervisor(t, logs, &current)
		server := newFakeAPI(&eventLog{})
		server.hangShutdown = true
		server.closeErr = errClose
		current.unit = &apiUnit{
			logger: testLogger(logs),
			sup:    view,
			server: server,
		}
		view.spawn(componentAPI, func() error {
			return withoutSentinel(server.Serve(nil), http.ErrServerClosed)
		})
		if err := stopLayer(t, &current, view); err != nil {
			t.Fatalf("stop = %v, want nil", err)
		}
		want := []map[string]any{
			{
				"layer": layerHTTP,
				"error": context.DeadlineExceeded.Error(),
			},
			{
				"layer": layerHTTP,
				"error": errClose.Error(),
			},
		}
		assertDiff(t, "forced_stop lines", want, picked(logs.all(t, eventForcedStop), "layer", "error"))
	})
}

func TestTheTracingLayerFlushesWithinTraceFlush(t *testing.T) {
	current, _ := tracingLayer(&Config{}, testLogger(&logBuffer{}))
	assertDiff(t, "tracing stop budget", traceFlush, current.stopBudget)
}
