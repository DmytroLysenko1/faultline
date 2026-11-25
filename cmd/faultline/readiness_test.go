package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type readyProbe struct {
	ready atomic.Bool
	calls atomic.Int32
}

func (p *readyProbe) hook() bool {
	p.calls.Add(1)
	return p.ready.Load()
}

type readyArgs struct {
	state lifecycleState
	ready bool
}

type readyWant struct {
	ready bool
	calls int32
}

func TestReadinessLoadsTheStateFirst(t *testing.T) {
	tests := []struct {
		name string
		args readyArgs
		want readyWant
	}{
		{
			name: "starting: not ready, no hook asked",
			args: readyArgs{
				state: stateStarting,
				ready: true,
			},
		},
		{
			name: "draining: not ready, no hook asked",
			args: readyArgs{
				state: stateDraining,
				ready: true,
			},
		},
		{
			name: "serving with a ready layer",
			args: readyArgs{
				state: stateServing,
				ready: true,
			},
			want: readyWant{
				ready: true,
				calls: 1,
			},
		},
		{
			name: "serving with a layer that is not ready",
			args: readyArgs{
				state: stateServing,
			},
			want: readyWant{
				calls: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			life := newLifecycle(testLogger(&logBuffer{}))
			probe := &readyProbe{}
			probe.ready.Store(tt.args.ready)
			layers := []layer{
				{
					name: "no hook",
				},
				{
					name:  "hooked",
					ready: probe.hook,
				},
			}
			life.transition(tt.args.state, reasonStarted)
			ready := readiness{
				life:   life,
				layers: func() []layer { return layers },
			}

			got := ready.Ready()

			assertDiff(t, "ready", tt.want.ready, got)
			assertDiff(t, "hook calls", tt.want.calls, probe.calls.Load())
		})
	}
}

func TestTheRoleRoutesAnswerHealthAndReadiness(t *testing.T) {
	life := newLifecycle(testLogger(&logBuffer{}))
	life.transition(stateWaitingStreams, reasonStreamsNotReady)
	handler, err := roleRoutes(readiness{
		life:   life,
		layers: func() []layer { return nil },
	})(noopTracer{}.tracerProvider())
	if err != nil {
		t.Fatalf("routes: %v", err)
	}
	status := func(path string) int {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		return recorder.Code
	}

	before := []int{status(healthPath), status(readyPath)}
	life.serve()
	after := []int{status(healthPath), status(readyPath)}

	assertDiff(t, "health and readiness while waiting", []int{http.StatusOK, http.StatusServiceUnavailable}, before)
	assertDiff(t, "health and readiness while serving", []int{http.StatusOK, http.StatusOK}, after)
}
