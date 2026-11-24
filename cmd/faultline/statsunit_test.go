package main

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
)

const (
	quietTicks       = 3
	emptyTicks       = 10
	droppedSamples   = 5
	tickSlack        = time.Millisecond
	tickWatermarkLag = 2 * time.Minute
	lagRaiseTicks    = 3
)

var calmThresholds = selfalert.Thresholds{WatermarkLag: time.Minute}

type runningStats struct {
	stats layer
	sup   *supervisor
}

func startStats(t *testing.T, logs *logBuffer, served func() bool, hooks ...layer) runningStats {
	t.Helper()
	_, failStart := context.WithCancelCause(t.Context())
	t.Cleanup(func() {
		failStart(nil)
	})
	sup := newSupervisor(failStart, testLogger(logs))
	stats := statsLayer(func() []layer { return hooks }, served, calmThresholds, testLogger(logs))
	if err := stats.unit.start(t.Context(), stats.supervisedBy(sup)); err != nil {
		t.Fatalf("start stats: %v", err)
	}
	return runningStats{
		stats: stats,
		sup:   sup,
	}
}

func (r runningStats) stop(t *testing.T) (time.Duration, error) {
	t.Helper()
	r.sup.stopping(r.stats.components...)
	ctx, cancel := context.WithTimeout(t.Context(), r.stats.stopBudget)
	defer cancel()
	began := time.Now()
	err := r.stats.unit.stop(ctx)
	return time.Since(began), err
}

func alwaysServed() bool {
	return true
}

func neverServed() bool {
	return false
}

func windowsLines(t *testing.T, logs *logBuffer) []string {
	t.Helper()
	var events []string
	for _, message := range messages(logs.records(t)) {
		if strings.HasPrefix(message, "windows.") {
			events = append(events, message)
		}
	}
	return events
}

func seriesLimitLayer() layer {
	return layer{
		name: "a",
		sample: func(_ context.Context, sample *selfalert.Sample) {
			sample.SeriesLimitReached = true
		},
	}
}

func TestStatsDoesNotReportBeforeServing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		var served atomic.Bool
		running := startStats(t, logs, served.Load, seriesLimitLayer())
		time.Sleep(quietTicks*statsInterval + tickSlack)
		synctest.Wait()
		assertDiff(t, "lines before serving", []string(nil), messages(logs.records(t)))
		served.Store(true)
		time.Sleep(statsInterval)
		synctest.Wait()
		assertDiff(t, "stats lines", 1, len(logs.all(t, eventPipelineStats)))
		assertDiff(t, "alert lines", []string{selfalert.SeriesLimitReached}, windowsLines(t, logs))
		served.Store(false)
		if _, err := running.stop(t); err != nil {
			t.Fatalf("stop: %v", err)
		}
	})
}

func TestTheFinalReportNeedsServing(t *testing.T) {
	tests := []struct {
		Name string
		Args func() bool
		Want []map[string]any
	}{
		{
			Name: "a stack that never served writes no final report",
			Args: neverServed,
			Want: nil,
		},
		{
			Name: "a served stack writes one final report",
			Args: alwaysServed,
			Want: []map[string]any{{"final": true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &logBuffer{}
				running := startStats(t, logs, tt.Args, seriesLimitLayer())
				if _, err := running.stop(t); err != nil {
					t.Fatalf("stop: %v", err)
				}
				assertDiff(t, "final reports", tt.Want, picked(logs.all(t, eventPipelineStats), "final"))
				assertDiff(t, "alert lines", []string(nil), windowsLines(t, logs))
			})
		})
	}
}

func TestLayersFillOneSampleForTheAlerts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		first := seriesLimitLayer()
		first.statsGroups = func(context.Context) []slog.Attr {
			return []slog.Attr{slog.Group("first", slog.Int("value", 1))}
		}
		second := layer{
			name: "b",
			sample: func(_ context.Context, sample *selfalert.Sample) {
				sample.DroppedSamples = droppedSamples
			},
			statsGroups: func(context.Context) []slog.Attr {
				return []slog.Attr{slog.Group("second", slog.Int("value", 2))}
			},
		}
		running := startStats(t, logs, alwaysServed, first, second)
		time.Sleep(statsInterval + tickSlack)
		synctest.Wait()
		want := []string{selfalert.SeriesLimitReached, selfalert.SketchBudgetExhausted}
		assertDiff(t, "alert lines", want, windowsLines(t, logs))
		line := statsLine(t, logs)
		firstAt, secondAt, alertsAt := strings.Index(line, `"first"`), strings.Index(line, `"second"`),
			strings.Index(line, `"alerts"`)
		if firstAt < 0 || firstAt > secondAt || secondAt > alertsAt {
			t.Errorf("groups out of order in %s", line)
		}
		raised := map[string]any{
			selfalert.SeriesLimitReached:    true,
			selfalert.SketchBudgetExhausted: true,
		}
		assertDiff(t, "alerts of the first tick", raised, firstAlerts(t, logs, slices.Collect(maps.Keys(raised))))
		if _, err := running.stop(t); err != nil {
			t.Fatalf("stop: %v", err)
		}
	})
}

func firstAlerts(t *testing.T, logs *logBuffer, events []string) map[string]any {
	t.Helper()
	reports := logs.all(t, eventPipelineStats)
	if len(reports) == 0 {
		t.Fatalf("no stats line:\n%s", logs.String())
	}
	alerts, _ := reports[0]["alerts"].(map[string]any)
	return lo.PickByKeys(alerts, events)
}

func TestTheSampleCarriesTheTimeOfItsTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		behind := layer{
			name: "behind",
			sample: func(_ context.Context, sample *selfalert.Sample) {
				sample.Watermark = time.Now().Add(-tickWatermarkLag)
				sample.WatermarkKnown = true
			},
		}
		running := startStats(t, logs, alwaysServed, behind)
		time.Sleep(lagRaiseTicks*statsInterval + tickSlack)
		synctest.Wait()
		want := []map[string]any{
			{
				"level": levelWarn,
				"msg":   selfalert.WatermarkLagHigh,
			},
		}
		assertDiff(t, "watermark lines", want, picked(logs.all(t, selfalert.WatermarkLagHigh), "level", "msg"))
		if _, err := running.stop(t); err != nil {
			t.Fatalf("stop: %v", err)
		}
	})
}

func statsLine(t *testing.T, logs *logBuffer) string {
	t.Helper()
	for line := range strings.Lines(logs.String()) {
		if strings.Contains(line, `"msg":"`+eventPipelineStats+`"`) {
			return line
		}
	}
	t.Fatalf("no stats line:\n%s", logs.String())
	return ""
}

func TestNilHooksAreSkipped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		filled := seriesLimitLayer()
		filled.name = "c"
		running := startStats(
			t,
			logs,
			alwaysServed,
			layer{name: "a"},
			layer{name: "b"},
			filled,
		)
		time.Sleep(statsInterval + tickSlack)
		synctest.Wait()
		if _, err := running.stop(t); err != nil {
			t.Fatalf("stop: %v", err)
		}
		assertDiff(t, "alert lines", []string{selfalert.SeriesLimitReached}, windowsLines(t, logs))
		want := []string{"alerts", "final", "level", "msg", "time"}
		for _, record := range logs.all(t, eventPipelineStats) {
			assertDiff(t, "stats line keys", want, slices.Sorted(maps.Keys(record)))
		}
	})
}

func TestAnEmptySampleRaisesNoAlert(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		running := startStats(t, logs, alwaysServed)
		time.Sleep(emptyTicks*statsInterval + tickSlack)
		synctest.Wait()
		if _, err := running.stop(t); err != nil {
			t.Fatalf("stop: %v", err)
		}
		assertDiff(t, "alert lines", []string(nil), windowsLines(t, logs))
		for _, record := range logs.all(t, eventPipelineStats) {
			alerts, _ := record["alerts"].(map[string]any)
			for event, raised := range alerts {
				if raised != false {
					t.Errorf("alert %s raised on an empty sample", event)
				}
			}
		}
	})
}

func TestAStatsUnitThatMissesItsBudgetIsAbandoned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		release := make(chan struct{})
		hanging := layer{
			name: "hanging",
			statsGroups: func(context.Context) []slog.Attr {
				<-release
				return nil
			},
		}
		running := startStats(t, logs, alwaysServed, hanging)
		time.Sleep(statsInterval + tickSlack)
		synctest.Wait()
		elapsed, err := running.stop(t)
		close(release)
		synctest.Wait()
		if !errors.Is(err, errStepAbandoned) {
			t.Fatalf("stop = %v, want %v", err, errStepAbandoned)
		}
		assertDiff(t, "stop elapsed", exitGrace, elapsed)
		assertDiff(t, "final reports", []map[string]any(nil), picked(finalReports(t, logs), "final"))
	})
}

func TestAStuckStatsLayerOutlivesItsStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, notify := &logBuffer{}, newFakeNotify()
		logger := testLogger(logs)
		life := newLifecycle(logger)
		release := make(chan struct{})
		role := &stack{}
		hanging := (&fakeLayer{name: "hanging"}).layer(&eventLog{})
		hanging.statsGroups = func(context.Context) []slog.Attr {
			<-release
			return nil
		}
		role.layers = []layer{statsLayer(role.hooks, life.Served, calmThresholds, logger), hanging}
		done := launchRoleWith(t, role, notify.notify, life)
		time.Sleep(statsInterval + tickSlack)
		synctest.Wait()
		notify.send()
		code := <-done
		close(release)
		synctest.Wait()
		assertCode(t, code, exitFailure, logs)
		want := []map[string]any{
			{
				"layer":           layerStats,
				"elapsed_seconds": (exitGrace + finalReportBudget).Seconds(),
				"error":           errStepAbandoned.Error() + "\n" + componentStats + ": " + errComponentOutlivedStop.Error(),
			},
		}
		assertDiff(t, "layer_stop_failed lines", want,
			picked(logs.all(t, eventLayerStopFailed), "layer", "elapsed_seconds", "error"))
	})
}

func TestABlockingHookCannotHoldTheFinalReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		blocking := layer{
			name: "blocking",
			statsGroups: func(ctx context.Context) []slog.Attr {
				<-ctx.Done()
				return []slog.Attr{slog.String("blocking", "released")}
			},
		}
		var served atomic.Bool
		running := startStats(t, logs, served.Load, blocking)
		served.Store(true)
		elapsed, err := running.stop(t)
		if err != nil || elapsed > running.stats.stopBudget {
			t.Fatalf("stop = %v after %v, want nil within %v", err, elapsed, running.stats.stopBudget)
		}
		assertDiff(t, "final reports", []map[string]any{{"final": true, "blocking": "released"}},
			picked(finalReports(t, logs), "final", "blocking"))
	})
}

func TestAnAbandonedReporterCallsNoHookAfterItsStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		release := make(chan struct{})
		var reported atomic.Int32
		hanging := layer{
			name: "hanging",
			sample: func(context.Context, *selfalert.Sample) {
				<-release
			},
			statsGroups: func(context.Context) []slog.Attr {
				reported.Add(1)
				return nil
			},
		}
		running := startStats(t, logs, alwaysServed, hanging)
		time.Sleep(statsInterval + tickSlack)
		synctest.Wait()
		_, err := running.stop(t)
		close(release)
		synctest.Wait()
		if !errors.Is(err, errStepAbandoned) {
			t.Fatalf("stop = %v, want %v", err, errStepAbandoned)
		}
		assertDiff(t, "stats lines", []map[string]any(nil), logs.all(t, eventPipelineStats))
		assertDiff(t, "statsGroups calls", int32(0), reported.Load())
	})
}

func TestAnUnstartedStatsUnitStopsCleanly(t *testing.T) {
	if err := (&statsUnit{}).stop(t.Context()); err != nil {
		t.Fatalf("stop = %v, want nil", err)
	}
}

func TestAnAbandonedReporterCallsNoLaterLayersHook(t *testing.T) {
	tests := []struct {
		Name     string
		Blocking func(release <-chan struct{}) layer
		Counted  func(calls *atomic.Int32) layer
	}{
		{
			Name: "a sample that returns after the abandon",
			Blocking: func(release <-chan struct{}) layer {
				return layer{
					name: "blocking",
					sample: func(_ context.Context, sample *selfalert.Sample) {
						<-release
						sample.SeriesLimitReached = true
					},
				}
			},
			Counted: func(calls *atomic.Int32) layer {
				return layer{
					name: "counted",
					sample: func(context.Context, *selfalert.Sample) {
						calls.Add(1)
					},
				}
			},
		},
		{
			Name: "statsGroups that return after the abandon",
			Blocking: func(release <-chan struct{}) layer {
				return layer{
					name: "blocking",
					statsGroups: func(context.Context) []slog.Attr {
						<-release
						return nil
					},
				}
			},
			Counted: func(calls *atomic.Int32) layer {
				return layer{
					name: "counted",
					statsGroups: func(context.Context) []slog.Attr {
						calls.Add(1)
						return nil
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				assertNoHookAfterTheAbandon(t, tt.Blocking, tt.Counted)
			})
		})
	}
}

func assertNoHookAfterTheAbandon(
	t *testing.T,
	blocking func(release <-chan struct{}) layer,
	counted func(calls *atomic.Int32) layer,
) {
	t.Helper()
	logs := &logBuffer{}
	release := make(chan struct{})
	var calls atomic.Int32
	running := startStats(t, logs, alwaysServed, blocking(release), counted(&calls))
	time.Sleep(statsInterval + tickSlack)
	synctest.Wait()
	_, err := running.stop(t)
	close(release)
	synctest.Wait()
	if !errors.Is(err, errStepAbandoned) {
		t.Fatalf("stop = %v, want %v", err, errStepAbandoned)
	}
	assertDiff(t, "stats lines", []map[string]any(nil), logs.all(t, eventPipelineStats))
	assertDiff(t, "later layer's hook calls", int32(0), calls.Load())
	assertDiff(t, "alert lines", []string(nil), windowsLines(t, logs))
}
