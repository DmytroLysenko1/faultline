package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

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
			})
		})
	}
}

func TestSignalAfterPipelineExitAborts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, crashingPipeline, 0)
		pipelineCtx, cancelPipeline := context.WithCancel(t.Context())
		defer cancelPipeline()
		down := &shutdown{
			sys:            h.sys,
			parts:          startComponents(pipelineCtx, h.sys, listeners{}),
			budget:         defaultShutdownBudget(),
			cancelPipeline: cancelPipeline,
		}
		h.crash <- nil
		synctest.Wait()
		down.record(<-down.parts.firstExit())
		err := down.afterSignal(t.Context(), h.stop)
		if !errors.Is(err, errUnexpectedExit) {
			t.Fatalf("afterSignal = %v, want errUnexpectedExit", err)
		}
		want := []string{eventPipelineExit, eventJournalClose, eventIngestGraceful, eventAPIShutdown, eventSignalStop}
		if diff := cmp.Diff(want, h.events.list()); diff != "" {
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
			Events: []string{eventSignalStop, eventIngestGraceful, eventIngestStop, eventJournalClose, eventPipelineExit,
				eventAPIShutdown},
			States: []string{"serving", "draining", "stopped"},
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("shutdown (-want +got):\n%s", diff)
		}
	})
}

func TestEveryComponentReportsExactlyOneExit(t *testing.T) {
	names := []string{componentOTLP, componentAPI, componentPipeline}
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
