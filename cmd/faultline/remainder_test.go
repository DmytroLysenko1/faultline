package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const waitAttempts = 200

type remainder struct {
	Spans   float64
	Batches float64
	Exact   bool
}

func remainderOf(t *testing.T, h *harness) remainder {
	t.Helper()
	record := h.logs.event(t, "lifecycle.drain_incomplete")
	if record == nil {
		t.Fatal("no lifecycle.drain_incomplete in the log")
	}
	return remainder{
		Spans:   record["spans"].(float64),
		Batches: record["batches"].(float64),
		Exact:   record["exact"].(bool),
	}
}

func finalJournalOf(t *testing.T, h *harness, exact bool) remainder {
	t.Helper()
	for _, record := range h.logs.all(t, "pipeline.stats") {
		if record["final"] == true {
			journal := record["journal"].(map[string]any)
			return remainder{
				Spans:   journal["spans"].(float64),
				Batches: journal["batches"].(float64),
				Exact:   exact,
			}
		}
	}
	t.Fatal("no final pipeline.stats in the log")
	return remainder{}
}

func TestDrainIncompleteReportsTheJournalRemainder(t *testing.T) {
	tests := []struct {
		Name string
		Args pipelineKind
		Want remainder
	}{
		{
			Name: "pipeline exits after cancel: the count is exact",
			Args: deafPipeline,
			Want: remainder{
				Spans:   2 * preloadedBatches,
				Batches: preloadedBatches,
				Exact:   true,
			},
		},
		{
			Name: "pipeline never exits: the count is an upper bound",
			Args: stuckPipeline,
			Want: remainder{
				Spans:   2 * preloadedBatches,
				Batches: preloadedBatches,
				Exact:   false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, tt.Args, preloadedBatches)
				_, err := runHarness(t, h, signalled)
				close(h.release)
				synctest.Wait()
				if !errors.Is(err, errDrainIncomplete) {
					t.Fatalf("run = %v, want errDrainIncomplete", err)
				}
				if diff := cmp.Diff(tt.Want, remainderOf(t, h)); diff != "" {
					t.Errorf("drain_incomplete (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(tt.Want, finalJournalOf(t, h, tt.Want.Exact)); diff != "" {
					t.Errorf("journal of the final pipeline.stats (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestSignalAfterPipelineExitAborts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, crashingPipeline, 0)
		pipelineCtx, cancelPipeline := context.WithCancel(t.Context())
		defer cancelPipeline()
		reportCtx, cancelReport := context.WithCancel(t.Context())
		defer cancelReport()
		down := &shutdown{
			sys:            h.sys,
			parts:          startComponents(pipelineCtx, reportCtx, h.sys, listeners{}),
			budget:         defaultShutdownBudget(),
			cancelPipeline: cancelPipeline,
			cancelReport:   cancelReport,
		}
		h.crash <- nil
		synctest.Wait()
		down.record(<-down.parts.firstExit())
		err := down.afterSignal(t.Context(), h.stop)
		if !errors.Is(err, errUnexpectedExit) {
			t.Fatalf("afterSignal = %v, want errUnexpectedExit", err)
		}
		want := []string{eventPipelineExit, eventJournalClose, eventIngestGraceful, eventAPIShutdown, eventTraceFlush,
			eventSignalStop}
		if diff := cmp.Diff(want, withoutReporter(h.events.list())); diff != "" {
			t.Errorf("events (-want +got):\n%s", diff)
		}
	})
}

func TestOTLPStuckAfterStopIsReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, drainingPipeline, 0)
		h.ingest.hangGraceful = true
		h.ingest.ignoreStop = true
		got, err := runHarness(t, h, signalled)
		h.ingest.release()
		synctest.Wait()
		if !errors.Is(err, errNotStopped) {
			t.Fatalf("run = %v, want errNotStopped", err)
		}
		want := shutdownOutcome{
			ReporterInOrder: true,
			Events: []string{eventSignalStop, eventIngestGraceful, eventIngestStop, eventJournalClose,
				eventPipelineExit, eventAPIShutdown, eventTraceFlush},
			Timeline: []string{"serving", "draining", timelineFinalStats, "stopped"},
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("shutdown (-want +got):\n%s", diff)
		}
	})
}

func TestEveryComponentReportsExactlyOneExit(t *testing.T) {
	names := []string{componentOTLP, componentAPI, componentPipeline, componentStats}
	parts := newComponents(names...)
	for _, name := range names {
		parts.start(name, func() error {
			return nil
		})
	}
	var seen []string
	for range names {
		seen = append(seen, (<-parts.firstExit()).name)
	}
	if extra := parts.remainingExits(); len(extra) != 0 {
		t.Errorf("extra exits %v", extra)
	}
	if diff := cmp.Diff(names, seen, cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})); diff != "" {
		t.Errorf("exits (-want +got):\n%s", diff)
	}
}

func TestOnlyServingIsReady(t *testing.T) {
	tests := []struct {
		Name string
		Args lifecycleState
		Want bool
	}{
		{
			Name: "serving",
			Args: stateServing,
			Want: true,
		},
		{
			Name: "draining",
			Args: stateDraining,
		},
		{
			Name: "aborting",
			Args: stateAborting,
		},
		{
			Name: "stopped",
			Args: stateStopped,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			state := newLifecycle(testLogger(&logBuffer{}))
			if state.Ready() {
				t.Fatal("ready before serving")
			}
			state.transition(tt.Args, reasonStarted)
			if got := state.Ready(); got != tt.Want {
				t.Errorf("Ready() in %s = %v, want %v", tt.Args, got, tt.Want)
			}
		})
	}
}

func TestWaitPrefersAnExitOverAnExpiredDeadline(t *testing.T) {
	parts := newComponents(componentAPI)
	parts.start(componentAPI, func() error {
		return nil
	})
	<-parts.firstExit()
	expired, cancel := context.WithCancel(t.Context())
	cancel()
	for attempt := range waitAttempts {
		if !parts.wait(expired, componentAPI) {
			t.Fatalf("attempt %d: wait reported a component that already exited as stuck", attempt)
		}
	}
}

func TestPipelineStuckWithAnEmptyJournalIsReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, stuckPipeline, 0)
		_, err := runHarness(t, h, signalled)
		close(h.release)
		synctest.Wait()
		if err != nil {
			t.Fatalf("run = %v, want nil: nothing was lost", err)
		}
		var forced []string
		for _, record := range h.logs.all(t, "lifecycle.forced_stop") {
			forced = append(forced, record["component"].(string))
		}
		if diff := cmp.Diff([]string{componentPipeline}, forced); diff != "" {
			t.Errorf("forced stops (-want +got):\n%s", diff)
		}
	})
}
