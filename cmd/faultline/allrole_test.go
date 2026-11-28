package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	acceptedBeforeSigterm = 200
	tinyPairingTTL        = "1ms"
)

var (
	errOTLPRefused = errors.New("otlp port refused")
	allStopOrder   = []any{"otlp", "journal", "directory", "p0", "stats", "conn", "http", "tracing"}
)

type allRun struct {
	setup *allSetup
	logs  *logBuffer
	life  *lifecycle
	role  *stack
	sig   *fakeNotify
	done  <-chan int
}

func allEnvironment(t *testing.T, urls []string) map[string]string {
	t.Helper()
	return processorEnvironment(t, map[string]string{
		envNATSURL:  strings.Join(urls, ","),
		envAPIAddr:  localAddress(t),
		envOTLPAddr: localAddress(t),
	})
}

func buildAll(t *testing.T, values map[string]string) *allRun {
	t.Helper()
	setup, err := loadAll(processOf(values))
	if err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	run := &allRun{
		setup: &setup,
		logs:  &logBuffer{},
		sig:   newFakeNotify(),
	}
	run.life = newLifecycle(testLogger(run.logs))
	run.role = allStack(run.setup, run.life, testLogger(run.logs))
	return run
}

func (r *allRun) launch(t *testing.T) *allRun {
	t.Helper()
	grantTestToken(t, r.setup.Journal.Tenant, r.setup.NATS.URLs...)
	r.done = launchRoleWith(t, r.role, r.sig.notify, r.life)
	return r
}

func startAll(t *testing.T, values map[string]string) *allRun {
	t.Helper()
	run := buildAll(t, values).launch(t)
	eventually(t, "all serving", run.life.Served)
	return run
}

func (r *allRun) graphs(t *testing.T) faultlinev1connect.GraphServiceClient {
	t.Helper()
	return faultlinev1connect.NewGraphServiceClient(h2cClient(t), "http://"+r.setup.Config.APIAddr, connect.WithGRPC())
}

func (r *allRun) exporter(t *testing.T) ptraceotlp.GRPCClient {
	t.Helper()
	return ptraceotlp.NewGRPCClient(dialOTLP(t, r.setup.Config.OTLPAddr))
}

func (r *allRun) layersUpTo(name string) int {
	return slices.IndexFunc(r.role.layers, func(entry layer) bool {
		return entry.name == name
	}) + 1
}

func (r *allRun) layer(t *testing.T, name string) *layer {
	t.Helper()
	for index := range r.role.layers {
		if r.role.layers[index].name == name {
			return &r.role.layers[index]
		}
	}
	t.Fatalf("the all stack has no layer %s", name)
	return nil
}

func TestTheAllCommandRefusesToStartWithoutNATS(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want string
	}{
		{
			name: "no NATS url",
			args: map[string]string{},
			want: "NATS_URL: required",
		},
		{
			name: "credentials in the url",
			args: map[string]string{
				envNATSURL: userinfoURL,
			},
			want: "NATS_URL: url 1 carries credentials, put them in NATS_CREDS_FILE",
		},
		{
			name: "a stream replica count NATS does not allow",
			args: map[string]string{
				envNATSURL:        testNATSURL,
				envStreamReplicas: "2",
			},
			want: envStreamReplicas,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			tt.args[envMemoryLimit] = "1MiB"
			tt.args[envCheckpointDir] = t.TempDir()

			code := realMain(&process{
				Args:           []string{commandAll},
				Lookup:         environment(tt.args),
				ReadCgroup:     func() ([]byte, error) { return nil, os.ErrNotExist },
				SetMemoryLimit: (&recordedLimit{}).setLimit,
				Stderr:         &stderr,
				Hostname:       podHostname,
			})

			assertDiff(t, "code", exitConfig, code)
			if !strings.Contains(stderr.String(), tt.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.want)
			}
			if strings.Contains(stderr.String(), secretMarker) {
				t.Errorf("stderr = %q carries the password", stderr.String())
			}
		})
	}
}

func TestTheIngesterDoesNotReadTheProcessorVariables(t *testing.T) {
	values := map[string]string{
		envNATSURL:           testNATSURL,
		envPairingTTL:        tinyPairingTTL,
		envPairingMaxPending: "abc",
		envGraphMaxNodes:     "abc",
		envTombstoneTTL:      tinyPairingTTL,
		envWindowSize:        "7s",
		envAPIResponseBudget: "1",
		envIdleAdvanceAfter:  tinyPairingTTL,
	}

	_, err := loadIngester(processOf(values))

	if err != nil {
		t.Errorf("loadIngester = %v, want the processor variables left unread", err)
	}
}

func TestAnIngesterWithAOneMillisecondPairingTTLReachesServing(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	setup, err := loadIngester(processOf(map[string]string{
		envNATSURL:    strings.Join(cluster.URLs(), ","),
		envAPIAddr:    localAddress(t),
		envOTLPAddr:   localAddress(t),
		envPairingTTL: tinyPairingTTL,
	}))
	if err != nil {
		t.Fatalf("loadIngester: %v", err)
	}
	conn := dialTest(t, cluster.URLs()...)
	if err := conn.EnsureTopology(t.Context(), setup.Journal.Topology, natsjstest.Held{}); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	logs := &logBuffer{}
	notify, done, life := serveIngester(t, &setup, logs)

	eventually(t, "ingester serving", life.Served)
	assertDiff(t, "readyz", http.StatusOK, statusOf(t, setup.Config.APIAddr, readyPath))

	notify.send()
	assertCode(t, <-done, exitOK, logs)
}

func TestAllRoleServesAnExportedCallAndStopsCleanly(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := startAll(t, allEnvironment(t, cluster.URLs()))
	if _, err := run.exporter(t).Export(t.Context(), callRequest(1)); err != nil {
		t.Fatalf("Export: %v", err)
	}
	graphs := run.graphs(t)
	eventually(t, "the call in GetGraph", func() bool { return calls(t, graphs) == 1 })
	assertDiff(t, "healthz", http.StatusOK, statusOf(t, run.setup.Config.APIAddr, healthPath))

	run.sig.send()

	assertCode(t, <-run.done, exitOK, run.logs)
	assertDiff(t, "stop order", allStopOrder, valuesOf(run.logs.all(t, eventLayerStopped), "layer"))
	final := run.logs.all(t, eventPipelineStats)
	last := final[len(final)-1]
	missing := lo.Reject(finalStatsKeys, func(key string, _ int) bool { return lo.HasKey(last, key) })
	assertDiff(t, "keys missing from the final pipeline.stats", []string{}, missing)
	assertDiff(t, "final", true, last["final"])
	assertErrors(t, run.logs)
	assertDiff(t, "lease released", false, leaseKeyExists(t, cluster.URLs()[0]))
}

func TestEverythingAllAcceptedBeforeSigtermIsInTheGraphAfterARestart(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	values := allEnvironment(t, cluster.URLs())
	first := startAll(t, values)
	exporter := first.exporter(t)
	for exported := range uint32(acceptedBeforeSigterm) {
		if _, err := exporter.Export(t.Context(), callRequest(exported)); err != nil {
			t.Fatalf("Export %d: %v", exported, err)
		}
	}

	first.sig.send()
	assertCode(t, <-first.done, exitOK, first.logs)
	second := startAll(t, values)

	graphs := second.graphs(t)
	eventually(t, "every accepted export in the graph", func() bool {
		return calls(t, graphs) == acceptedBeforeSigterm
	})
	second.sig.send()
	assertCode(t, <-second.done, exitOK, second.logs)
}

func TestAllIsReadyOnlyWhileServingAndEveryLayerIsReady(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := buildAll(t, allEnvironment(t, cluster.URLs()))
	var partitionDown, journalDown atomic.Bool
	gateReady(run.layer(t, partitionLabel(0)), &partitionDown)
	gateReady(run.layer(t, layerJournal), &journalDown)
	run.launch(t)
	eventually(t, "all serving", run.life.Served)
	address := run.setup.Config.APIAddr

	got := []int{statusOf(t, address, readyPath)}
	partitionDown.Store(true)
	got = append(got, statusOf(t, address, readyPath))
	partitionDown.Store(false)
	journalDown.Store(true)
	got = append(got, statusOf(t, address, readyPath), statusOf(t, address, healthPath))
	journalDown.Store(false)
	got = append(got, statusOf(t, address, readyPath))

	want := []int{
		http.StatusOK,
		http.StatusServiceUnavailable,
		http.StatusServiceUnavailable,
		http.StatusOK,
		http.StatusOK,
	}
	assertDiff(t, "readyz, p0 down, journal down, healthz, readyz", want, got)
	run.sig.send()
	assertCode(t, <-run.done, exitOK, run.logs)
}

func gateReady(gated *layer, down *atomic.Bool) {
	ready := gated.ready
	gated.ready = func() bool { return !down.Load() && ready() }
}

type refusedOTLP struct{}

func (refusedOTLP) start(context.Context, unitSupervisor) error {
	return &stageError{
		stage: stageOTLPListen,
		err:   errOTLPRefused,
	}
}

func (refusedOTLP) stop(context.Context) error { return nil }

func TestAllIngestStartFailureStopsTheProcessorThroughItsLayers(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := buildAll(t, allEnvironment(t, cluster.URLs()))
	run.layer(t, layerOTLP).unit = refusedOTLP{}
	run.launch(t)

	assertCode(t, <-run.done, exitFailure, run.logs)
	assertErrors(t, run.logs, eventStartFailed)
	assertDiff(t, "stage", stageOTLPListen, run.logs.event(t, eventStartFailed)["stage"])
	assertDiff(t, "stop lines", []string{
		"layer_stopped otlp",
		"layer_stopped journal",
		"layer_stopped directory",
		"layer_stopped p0",
		"layer_stopped stats",
		"layer_stopped conn",
		"layer_stopped http",
		"layer_stopped tracing",
	}, stopLines(t, run.logs))
	assertDiff(t, "lease released", false, leaseKeyExists(t, cluster.URLs()[0]))
	assertNoLine(t, run.logs, eventPipelineStats)
}

func TestALostLeaseWhileAllWaitsForStreamsStopsWithCodeOne(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the lease TTL")
	}
	cluster := natsjstest.Start(t, singleNATSNode)
	run := buildAll(t, allEnvironment(t, cluster.URLs()))
	run.setup.Journal.Topology.DuplicateWindow = 2 * run.setup.Partition.Streams.DuplicateWindow
	run.role = allStack(run.setup, run.life, testLogger(run.logs))
	run.launch(t)
	eventuallyWithin(t, "journal waiting for the streams", leaseLossDeadline,
		lineWith(t, run.logs, eventState, "state", string(stateWaitingStreams)))
	kv, err := natsjstest.JetStream(t, cluster.URLs()[0]).KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}

	if _, err := kv.Put(t.Context(), leaseKeyP0, []byte("intruder/1")); err != nil {
		t.Fatalf("take the key: %v", err)
	}

	select {
	case code := <-run.done:
		assertCode(t, code, exitFailure, run.logs)
	case <-time.After(leaseLossDeadline):
		t.Fatalf("all did not stop within %v of losing its lease", leaseLossDeadline)
	}
	assertErrors(t, run.logs, eventLeaseLost)
	lost := run.logs.event(t, eventLeaseLost)
	assertDiff(t, "lease.lost", []any{"p0", "journal/await_streams"}, []any{lost["partition"], lost["stage"]})
	assertDiff(t, "served", false, run.life.Served())
	assertNoLine(t, run.logs, eventPipelineStats)
	assertDiff(t, "layers entered", run.layersUpTo(layerJournal), run.role.entered)
}

func TestTheAllRoleFitsTheDefaultMemoryLimit(t *testing.T) {
	setup, err := loadAll(processOf(processorEnvironment(t, map[string]string{})))
	if err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	parts := allStack(&setup, newLifecycle(testLogger(&logBuffer{})), testLogger(&logBuffer{})).contributions()

	if err := checkParts(parts, defaultMemoryLimit); err != nil {
		t.Errorf("all does not fit the default GOMEMLIMIT: %v", err)
	}
	want := []contribution{
		{
			name:  "api responses",
			bytes: 268435456,
		},
		{
			name:  "nats connections",
			bytes: 7340032,
		},
		{
			name:  "feed credit and pull",
			bytes: 92274688,
		},
		{
			name:  "pairing, graph and traffic",
			bytes: 421652392,
		},
		{
			name:  "snapshots",
			bytes: 24611072,
		},
		{
			name:  "batch apply",
			bytes: 29100120,
		},
		{
			name:  "checkpoint",
			bytes: 6491273,
		},
		{
			name:  "token directory",
			bytes: directoryHeap,
		},
		{
			name:  "records before pubAck",
			bytes: 62914560,
		},
		{
			name:  "publish buffer",
			bytes: 8388608,
		},
		{
			name:  "otlp requests",
			bytes: 461373440,
		},
		{
			name:  "otlp connections",
			bytes: 21299200,
		},
		{
			name:  "admission tokens",
			bytes: 458752,
		},
	}
	if diff := cmp.Diff(want, parts, cmp.AllowUnexported(contribution{})); diff != "" {
		t.Errorf("all contributions at defaults (-want +got):\n%s", diff)
	}
}

func TestAPartitionWithoutAGraphWarnsThatItsStatsFailed(t *testing.T) {
	logs := &logBuffer{}
	empty := newPartitionUnit(&partitionConfig{}, nil, &memgraph.Store{}, nil, testLogger(logs))

	groups := empty.statsGroups(t.Context())

	assertDiff(t, "groups", []slog.Attr(nil), groups)
	failed := logs.all(t, eventStatsFailed)
	assertDiff(t, "warnings", 1, len(failed))
	if len(failed) == 1 && !strings.Contains(failed[0]["error"].(string), "no graph") {
		t.Errorf("pipeline.stats_failed = %v, want the missing graph as its error", failed[0])
	}
}

type tickingClock struct {
	now time.Time
}

func (c *tickingClock) Now() time.Time {
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

type droppedSnapshots struct{}

func (droppedSnapshots) Publish(context.Context, *topology.Snapshot) error {
	return nil
}

func TestAStatsLineWithoutAGraphKeepsThePipelineTimingsForTheNextLine(t *testing.T) {
	feed := processingtest.NewIterationFeed([]processing.Delivery{processingtest.NewDelivery(processingtest.Pairs(t, 1, 1))})
	clock := &tickingClock{
		now: telemetrytest.Epoch(),
	}
	pipeline := processingtest.NewPipeline(t, feed, processingtest.NewBuilder(t), droppedSnapshots{}, clock)
	if err := pipeline.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	unit := newPartitionUnit(&partitionConfig{}, nil, &memgraph.Store{}, nil, testLogger(&logBuffer{}))
	unit.pipeline = pipeline

	assertDiff(t, "groups", []slog.Attr(nil), unit.statsGroups(t.Context()))
	if kept := pipeline.TakeTimings(); kept.Iteration <= 0 {
		t.Errorf("timings after a stats line without a graph = %+v, want the iteration kept for the next line", kept)
	}
}
