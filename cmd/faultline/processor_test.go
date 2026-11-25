package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	processorBreakdown = "pairing, graph and traffic 551.6 MiB, api responses 256.0 MiB, feed credit and pull 88.0 MiB, " +
		"batch apply 27.8 MiB, snapshots 23.5 MiB, nats connections 9.0 MiB, checkpoint 6.2 MiB"
	defaultLagSeconds = 96

	missingCheckpointDir  = "missing"
	fileCheckpointDir     = "file"
	readOnlyCheckpointDir = "read-only"
	readOnlyMode          = 0o500
	writableMode          = 0o700
	plainFileMode         = 0o600
)

var finalStatsKeys = []string{
	"final",
	"revision",
	"nodes",
	"edges",
	"pairing",
	"graph",
	"windows",
	"cardinality",
	"sketch",
	"pipeline",
	"alerts",
}

func checkpointDirFor(t *testing.T, kind string) string {
	t.Helper()
	dir := t.TempDir()
	switch kind {
	case missingCheckpointDir:
		return filepath.Join(dir, kind)
	case fileCheckpointDir:
		file := filepath.Join(dir, kind)
		if err := os.WriteFile(file, nil, plainFileMode); err != nil {
			t.Fatalf("write file: %v", err)
		}
		return file
	case readOnlyCheckpointDir:
		return readOnlyDir(t, dir)
	}
	return kind
}

func readOnlyDir(t *testing.T, dir string) string {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}
	if err := os.Chmod(dir, readOnlyMode); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(dir, writableMode); err != nil {
			t.Errorf("chmod back: %v", err)
		}
	})
	return dir
}

func processorEnvironment(t *testing.T, extra map[string]string) map[string]string {
	t.Helper()
	values := map[string]string{
		envNATSURL:       testNATSURL,
		envCheckpointDir: t.TempDir(),
	}
	for name, value := range extra {
		values[name] = value
	}
	return values
}

func processOf(values map[string]string) process {
	return process{
		Lookup:   environment(values),
		Hostname: podHostname,
	}
}

func TestLoadPartitionDefaultsAndRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg, err := loadPartition(environment(map[string]string{
		envCheckpointDir:      dir,
		envCheckpointRecovery: "discard:p0:none@7",
	}))
	if err != nil {
		t.Fatalf("loadPartition: %v", err)
	}
	want := partitionConfig{
		FeedMaxBytes:       defaultFeedMaxBytes,
		CheckpointInterval: defaultCheckpointInterval,
		CheckpointDir:      dir,
	}
	if diff := cmp.Diff(want, cfg, cmpopts.IgnoreFields(partitionConfig{}, "Recovery")); diff != "" {
		t.Errorf("config (-want +got):\n%s", diff)
	}
	if cfg.Recovery == nil || cfg.Recovery.FirstSeq != 7 {
		t.Errorf("recovery %v, want none@7 of p0", cfg.Recovery)
	}
}

const memoryOverBudget = "worst-case memory exceeds"

func TestTheProcessorCommandRefusesItsConfigWithCodeTwo(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want string
	}{
		{
			"no checkpoint directory",
			map[string]string{
				envCheckpointDir: "",
			},
			"CHECKPOINT_DIR: ",
		},
		{
			"a missing checkpoint directory",
			map[string]string{
				envCheckpointDir: missingCheckpointDir,
			},
			"CHECKPOINT_DIR: ",
		},
		{
			"a checkpoint directory that is a file",
			map[string]string{
				envCheckpointDir: fileCheckpointDir,
			},
			"CHECKPOINT_DIR: ",
		},
		{
			"a read-only checkpoint directory",
			map[string]string{
				envCheckpointDir: readOnlyCheckpointDir,
			},
			"CHECKPOINT_DIR: ",
		},
		{
			"a feed past its ceiling",
			map[string]string{
				envFeedMaxBytes: "1GiB",
			},
			"FEED_MAX_BYTES: ",
		},
		{
			"a checkpoint interval below a second",
			map[string]string{
				envCheckpointInterval: "500ms",
			},
			"CHECKPOINT_INTERVAL: ",
		},
		{
			"a feed below its floor",
			map[string]string{
				envFeedMaxBytes: "32MiB",
			},
			"FEED_MAX_BYTES: ",
		},
		{
			"a checkpoint interval past a minute",
			map[string]string{
				envCheckpointInterval: "2m",
			},
			"CHECKPOINT_INTERVAL: ",
		},
		{
			"a recovery of another partition",
			map[string]string{
				envCheckpointRecovery: "discard:p1:none@0",
			},
			"another partition",
		},
		{
			"a recovery in the wrong form",
			map[string]string{
				envCheckpointRecovery: "discard",
			},
			"CHECKPOINT_RECOVERY: must be",
		},
		{
			"a memory limit below the worst case",
			map[string]string{
				envMemoryLimit: "1GiB",
			},
			memoryOverBudget,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := processorEnvironment(t, tt.args)
			if values[envCheckpointDir] == "" {
				delete(values, envCheckpointDir)
			}
			if dir, set := values[envCheckpointDir]; set {
				values[envCheckpointDir] = checkpointDirFor(t, dir)
			}
			_, loadErr := loadProcessor(process{
				Lookup:   environment(values),
				Hostname: podHostname,
			})
			if loadErr == nil && tt.want != memoryOverBudget {
				t.Fatalf("loadProcessor accepted the config, want an error containing %q", tt.want)
			}
			var stderr bytes.Buffer
			code := realMain(process{
				Args:           []string{commandProcessor},
				Lookup:         environment(values),
				ReadCgroup:     func() ([]byte, error) { return nil, os.ErrNotExist },
				SetMemoryLimit: (&recordedLimit{}).setLimit,
				Stderr:         &stderr,
				Hostname:       podHostname,
			})
			assertDiff(t, "code", exitConfig, code)
			if !strings.Contains(stderr.String(), tt.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.want)
			}
		})
	}
}

func TestTheProcessorFitsItsMemoryBudgetAtDefaults(t *testing.T) {
	setup, err := loadProcessor(process{
		Lookup:   environment(processorEnvironment(t, nil)),
		Hostname: podHostname,
	})
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	logs := &logBuffer{}
	parts := processorStack(&setup, newLifecycle(testLogger(logs)), testLogger(logs)).contributions()
	assertDiff(t, "breakdown", processorBreakdown, breakdown(parts))
	if err := checkParts(parts, defaultMemoryLimit); err != nil {
		t.Errorf("the processor does not fit the default GOMEMLIMIT: %v", err)
	}
	assertDiff(t, "pending", processorMaxPending, setup.Partition.Limits.MaxPending)
}

func TestTheProcessorCarriesItsRulesHashAndRestoreBudget(t *testing.T) {
	path := writeRules(t, catalogRules)
	values := processorEnvironment(t, map[string]string{
		envRouteRulesFile: path,
	})
	setup, err := loadProcessor(processOf(values))
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	rules, err := loadRules(path)
	if err != nil {
		t.Fatalf("loadRules: %v", err)
	}
	if rules.Hash == 0 {
		t.Fatalf("the rules file has no hash")
	}
	assertDiff(t, "rules hash", rules.Hash, setup.Partition.RulesHash)
	assertDiff(t, "restore budget", restoreBudget(setup.Partition.Limits.WorstCheckpointBytes()), setup.Partition.RestoreBudget)
}

func TestTheProcessorPendingDefaultYieldsToTheEnvironment(t *testing.T) {
	values := map[string]string{
		envPairingMaxPending: "5000",
	}
	cfg, err := loadGraph(processorDefaults(environment(values)))
	if err != nil {
		t.Fatalf("loadGraph: %v", err)
	}
	assertDiff(t, "pending", 5000, cfg.Topology.MaxPending)
}

func TestProcessorLagThreshold(t *testing.T) {
	tests := []struct {
		name string
		idle string
		want time.Duration
	}{
		{"the defaults", "", defaultLagSeconds * time.Second},
		{"a longer idle advance", "5m", (defaultLagSeconds - 35 + 300) * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := processorEnvironment(t, nil)
			if tt.idle != "" {
				values[envIdleAdvanceAfter] = tt.idle
			}
			setup, err := loadProcessor(processOf(values))
			if err != nil {
				t.Fatalf("loadProcessor: %v", err)
			}
			assertDiff(t, "lag", tt.want, processorThresholds(&setup.Partition).WatermarkLag)
		})
	}
}

func TestThePartitionFillsTheSample(t *testing.T) {
	limits := topologytest.Limits()
	builder, err := topology.NewBuilder(&limits, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	snapshot := builder.Snapshot()
	graph, err := memgraph.New(snapshot)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	cfg := partitionConfig{
		Limits: limits,
	}
	observed := observedPartition(&cfg, nil, graph, nil, nil)
	now := time.Unix(100, 0)
	got := selfalert.Sample{
		Now: now,
	}
	observed.sample(t.Context(), &got)
	want := selfalert.Sample{
		Now: now,
	}
	fillSample(snapshot, &want)
	assertDiff(t, "sample", want, got)
	if observed.statsGroups == nil || len(observed.contributions) == 0 {
		t.Errorf("the role's partition layer has no stats groups or memory contributions")
	}

	empty := newPartitionUnit(&partitionConfig{}, nil, &memgraph.Store{}, nil, testLogger(&logBuffer{}))
	untouched := selfalert.Sample{
		Now: now,
	}
	empty.sample(t.Context(), &untouched)
	want = selfalert.Sample{
		Now: now,
	}
	assertDiff(t, "sample without a graph", want, untouched)
	if groups := empty.statsGroups(t.Context()); groups != nil {
		t.Errorf("stats groups without a graph = %v, want none", groups)
	}
}

func TestThePartitionStatsReadTheRoleClock(t *testing.T) {
	limits := topologytest.Limits()
	builder, err := topology.NewBuilder(&limits, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	snapshot := builder.Snapshot()
	snapshot.Watermark = telemetrytest.Epoch()
	snapshot.WatermarkKnown = true
	graph, err := memgraph.New(snapshot)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	cfg := partitionConfig{
		Limits: limits,
	}
	part := newPartitionUnit(&cfg, nil, graph, nil, nil)
	part.pipeline = &processing.Pipeline{}
	now := telemetrytest.Epoch().Add(watermarkBehind)
	part.clock = fixedClock{
		now: now,
	}
	windows, found := lo.Find(part.statsGroups(t.Context()), func(attr slog.Attr) bool { return attr.Key == "windows" })
	if !found {
		t.Fatalf("the partition stats have no windows group")
	}
	assertDiff(t, "windows", windowsGroup(snapshot, now).String(), windows.String())
}

func TestProcessorStopOrderAndReadiness(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	values := processorEnvironment(t, map[string]string{
		envNATSURL: strings.Join(cluster.URLs(), ","),
		envAPIAddr: localAddress(t),
	})
	setup, err := loadProcessor(processOf(values))
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	role := processorStack(&setup, life, testLogger(logs))
	var down atomic.Bool
	partition := &role.layers[len(role.layers)-1]
	partitionReady := partition.ready
	partition.ready = func() bool { return !down.Load() && partitionReady() }
	done := launchRoleWith(t, role, notify.notify, life)
	eventually(t, "serving", life.Served)
	assertDiff(t, "readyz", 200, statusOf(t, setup.Config.APIAddr, readyPath))
	down.Store(true)
	assertDiff(t, "readyz while p0 is not ready", 503, statusOf(t, setup.Config.APIAddr, readyPath))
	down.Store(false)
	assertDiff(t, "readyz once p0 is ready again", 200, statusOf(t, setup.Config.APIAddr, readyPath))

	notify.send()

	assertCode(t, <-done, exitOK, logs)
	stopped := valuesOf(logs.all(t, eventLayerStopped), "layer")
	assertDiff(t, "stop order", []any{"p0", "stats", "conn", "http", "tracing"}, stopped)
	final := logs.all(t, eventPipelineStats)
	last := final[len(final)-1]
	missing := lo.Reject(finalStatsKeys, func(key string, _ int) bool { return lo.HasKey(last, key) })
	assertDiff(t, "keys missing from the final pipeline.stats", []string{}, missing)
	assertDiff(t, "final", true, last["final"])
	assertErrors(t, logs)
}

func TestAStandbyProcessorStoppedWhileWaitingForTheLeaseWritesNoFinalStats(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	first := startProcessor(t, cluster.URLs())
	eventually(t, "first serving", first.life.Served)
	standby := startProcessor(t, cluster.URLs())
	eventually(t, "standby waiting", lineWith(t, standby.logs, eventState, "state", string(stateWaitingLease)))

	standby.sig.send()

	assertCode(t, <-standby.done, exitOK, standby.logs)
	assertNoLine(t, standby.logs, eventPipelineStats)
	first.sig.send()
	assertCode(t, <-first.done, exitOK, first.logs)
}

func startProcessor(t *testing.T, urls []string) *partitionRun {
	t.Helper()
	values := processorEnvironment(t, map[string]string{
		envNATSURL: strings.Join(urls, ","),
		envAPIAddr: localAddress(t),
	})
	setup, err := loadProcessor(processOf(values))
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	run := &partitionRun{
		logs: &logBuffer{},
		sig:  newFakeNotify(),
	}
	run.life = newLifecycle(testLogger(run.logs))
	run.role = processorStack(&setup, run.life, testLogger(run.logs))
	run.done = launchRoleWith(t, run.role, run.sig.notify, run.life)
	return run
}

type drainHold struct {
	release chan struct{}
}

func (h *drainHold) start(context.Context, unitSupervisor) error { return nil }

func (h *drainHold) stop(context.Context) error {
	<-h.release
	return nil
}

func TestALeaseLostWhileTheProcessorDrainsIsAStopFailureUntilTheEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		below := &drainHold{
			release: make(chan struct{}),
		}
		role := &stack{
			layers: []layer{
				{
					name:       "conn",
					unit:       below,
					stopBudget: time.Minute,
				},
				{
					name: "p0",
					unit: &leaseUnit{
						waits: true,
					},
					components: []string{"p0/lease"},
					stopBudget: time.Second,
				},
			},
		}
		done := launchRole(t, role, signal.notify, logs)
		synctest.Wait()
		signal.send()
		synctest.Wait()

		assertErrors(t, logs)
		assertNoLine(t, logs, eventComponentExit)
		assertDiff(t, "stop failed", "p0", logs.event(t, eventLayerStopFailed)["layer"])

		close(below.release)
		assertCode(t, <-done, exitFailure, logs)
		assertErrors(t, logs, eventLeaseLost)
		lost := lo.PickByKeys(logs.event(t, eventLeaseLost), []string{"stage", "partition"})
		wantLost := map[string]any{
			"stage":     stageStopping,
			"partition": "p0",
		}
		assertDiff(t, "lease.lost", wantLost, lost)
		assertDiff(t, "stop_failed followup", true, logs.event(t, eventStopFailed)["followup"])
		assertNoLine(t, logs, eventComponentExit)
	})
}
