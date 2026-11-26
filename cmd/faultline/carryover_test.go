package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/samber/lo"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	routedService     = "catalog"
	routedNamespace   = "shop"
	routedPath        = "/products/ABC123/reviews"
	routedTemplate    = "GET /products/{sku}/reviews"
	routedRaw         = "GET " + routedPath
	routedSpanSeconds = 1_700_000_000
	recordFootprint   = int64(1) << 40
	tracedNATSExports = 20
	pipelineExitWait  = 15 * time.Second
	ingesterStopLimit = 10 * time.Second
	getGraphSpan      = "faultline.v1.GraphService/GetGraph"
	attrNamespace     = "k8s.namespace.name"
	attrMethod        = "http.request.method"
	attrURLPath       = "url.path"
)

type publishedRecord struct {
	partition uint16
	data      []byte
}

type capturingPublisher struct {
	mu        sync.Mutex
	published []publishedRecord
}

func (p *capturingPublisher) Publish(_ context.Context, partition uint16, data []byte, _ string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, publishedRecord{
		partition: partition,
		data:      append([]byte(nil), data...),
	})
	return false, nil
}

func (p *capturingPublisher) operations(t *testing.T) []string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	var names []string
	for _, made := range p.published {
		header, err := record.ReadHeader(made.data, made.partition, recordFootprint)
		if err != nil {
			t.Fatalf("ReadHeader: %v", err)
		}
		spans, err := record.Decode(made.data, header)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		for _, stored := range spans {
			names = append(names, stored.Operation.Name)
		}
	}
	return names
}

func routedRequest() ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(attrServiceName, routedService)
	resourceSpans.Resource().Attributes().PutStr(attrNamespace, routedNamespace)
	served := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	served.SetTraceID(pcommon.TraceID{1})
	served.SetSpanID(pcommon.SpanID{calleeSpan})
	served.SetKind(ptrace.SpanKindServer)
	served.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(routedSpanSeconds, 0)))
	served.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(routedSpanSeconds, int64(time.Millisecond))))
	served.Attributes().PutStr(attrMethod, http.MethodGet)
	served.Attributes().PutStr(attrURLPath, routedPath)
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func TestTheIngesterNamesOperationsWithItsRouteRules(t *testing.T) {
	tests := []struct {
		name string
		args string
		want []string
	}{
		{
			name: "no rules file keeps the raw path",
			args: "",
			want: []string{routedRaw},
		},
		{
			name: "the catalog rules name the route",
			args: catalogRules,
			want: []string{routedTemplate},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := ""
			if tt.args != "" {
				path = writeRules(t, tt.args)
			}
			rules, err := loadRules(path)
			if err != nil {
				t.Fatalf("rules: %v", err)
			}
			publisher := &capturingPublisher{}
			opened, err := journal.New(publisher, testJournalOptions())
			if err != nil {
				t.Fatalf("journal: %v", err)
			}
			logs, address := &logBuffer{}, localAddress(t)
			current := otlpLayer(&Config{
				OTLPAddr:   address,
				RouteRules: rules.Rules,
			}, openedJournal{
				journal: opened,
			}, admittingTaps(), testLogger(logs))
			view := newLayerSupervisor(t, logs, &current)
			if err := current.unit.start(t.Context(), view); err != nil {
				t.Fatalf("start: %v", err)
			}
			response, err := ptraceotlp.NewGRPCClient(dialOTLP(t, address)).Export(t.Context(), routedRequest())
			if err != nil {
				t.Fatalf("Export: %v", err)
			}
			assertDiff(t, "rejected spans", int64(0), response.PartialSuccess().RejectedSpans())
			assertDiff(t, "journaled operations", tt.want, publisher.operations(t))
			if err := stopLayer(t, &current, view); err != nil {
				t.Errorf("stop = %v, want nil", err)
			}
		})
	}
}

func TestAPipelineExitWhileServingIsCodeOneAndAborting(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "serving", run.life.Served)

	run.unit.cancelPipeline()

	select {
	case code := <-run.done:
		assertCode(t, code, exitFailure, run.logs)
	case <-time.After(pipelineExitWait):
		t.Fatalf("the partition kept running %v after its pipeline exited", pipelineExitWait)
	}
	if !lo.Contains(run.logs.timeline(t), "aborting") {
		t.Errorf("timeline %v has no aborting", run.logs.timeline(t))
	}
	exits := picked(run.logs.all(t, eventComponentExit), "level", "component")
	want := []map[string]any{
		{
			"level":     levelError,
			"component": "p0/pipeline",
		},
	}
	assertDiff(t, "component exits", want, exits)
}

type recorderTracing struct {
	provider *sdktrace.TracerProvider
}

func (r recorderTracing) TracerProvider() trace.TracerProvider {
	return r.provider
}

func (r recorderTracing) Shutdown(ctx context.Context) error {
	return r.provider.Shutdown(ctx)
}

type recordingTracingUnit struct {
	traced    *tracingUnit
	recording recorderTracing
}

func (u recordingTracingUnit) start(context.Context, unitSupervisor) error {
	u.traced.tracing = u.recording
	return nil
}

func (u recordingTracingUnit) stop(ctx context.Context) error {
	return u.traced.stop(ctx)
}

func recordTracing(t *testing.T, role *stack) *tracetest.SpanRecorder {
	t.Helper()
	traced, found := role.layers[0].unit.(*tracingUnit)
	if !found {
		t.Fatalf("the first layer is %T, want the tracing unit", role.layers[0].unit)
	}
	recorder := tracetest.NewSpanRecorder()
	role.layers[0].unit = recordingTracingUnit{
		traced: traced,
		recording: recorderTracing{
			provider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)),
		},
	}
	return recorder
}

type endedSpan struct {
	Name      string
	Kind      trace.SpanKind
	HasParent bool
}

func endedSpans(recorder *tracetest.SpanRecorder) []endedSpan {
	return lo.Map(recorder.Ended(), func(ended sdktrace.ReadOnlySpan, _ int) endedSpan {
		return endedSpan{
			Name:      ended.Name(),
			Kind:      ended.SpanKind(),
			HasParent: ended.Parent().IsValid(),
		}
	})
}

func getGraphSpans(count int) []endedSpan {
	return lo.Times(count, func(int) endedSpan {
		return endedSpan{
			Name: getGraphSpan,
			Kind: trace.SpanKindServer,
		}
	})
}

type tracedRoles struct {
	processor     *partitionRun
	processorAPI  string
	processorSpan *tracetest.SpanRecorder
	ingester      *fakeNotify
	ingesterDone  <-chan int
	ingesterLogs  *logBuffer
	ingesterAPI   string
	ingesterOTLP  string
	ingesterSpan  *tracetest.SpanRecorder
}

func startTracedRoles(t *testing.T, urls []string) *tracedRoles {
	t.Helper()
	traced := &tracedRoles{
		processorAPI: localAddress(t),
		ingesterLogs: &logBuffer{},
		ingester:     newFakeNotify(),
	}
	values := processorEnvironment(t, map[string]string{
		envNATSURL: strings.Join(urls, ","),
		envAPIAddr: traced.processorAPI,
	})
	setup, err := loadProcessor(processOf(values))
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	traced.processor = &partitionRun{
		logs: &logBuffer{},
		sig:  newFakeNotify(),
	}
	traced.processor.life = newLifecycle(testLogger(traced.processor.logs))
	traced.processor.role = processorStack(&setup, traced.processor.life, testLogger(traced.processor.logs))
	traced.processorSpan = recordTracing(t, traced.processor.role)
	traced.processor.done = launchRoleWith(t, traced.processor.role, traced.processor.sig.notify, traced.processor.life)
	eventually(t, "processor serving", traced.processor.life.Served)
	ingester, api, otlpAddress := ingesterOn(t, urls)
	ingester.Journal.Topology = setup.Partition.Streams
	traced.ingesterAPI, traced.ingesterOTLP = api, otlpAddress
	grantTestToken(t, ingester.Journal.Tenant, urls...)
	life := newLifecycle(testLogger(traced.ingesterLogs))
	role := ingesterStack(ingester, life, testLogger(traced.ingesterLogs))
	traced.ingesterSpan = recordTracing(t, role)
	traced.ingesterDone = launchRoleWith(t, role, traced.ingester.notify, life)
	eventually(t, "ingester serving", life.Served)
	return traced
}

func TestIngestPathIsNotTracedOnNATS(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	roles := startTracedRoles(t, cluster.URLs())
	exporter := ptraceotlp.NewGRPCClient(dialOTLP(t, roles.ingesterOTLP))
	for exported := range uint32(tracedNATSExports) {
		if _, err := exporter.Export(t.Context(), callRequest(exported)); err != nil {
			t.Fatalf("Export %d: %v", exported, err)
		}
	}
	graphs := faultlinev1connect.NewGraphServiceClient(h2cClient(t), "http://"+roles.processorAPI, connect.WithGRPC())
	polls := 0
	eventually(t, "every export in the graph", func() bool {
		polls++
		return calls(t, graphs) == tracedNATSExports
	})
	assertDiff(t, "ingester healthz", http.StatusOK, statusOf(t, roles.ingesterAPI, healthPath))
	assertDiff(t, "ingester spans after the exports", []endedSpan(nil), endedSpans(roles.ingesterSpan))
	assertDiff(t, "processor spans after processing", getGraphSpans(polls), endedSpans(roles.processorSpan))

	calls(t, graphs)
	assertDiff(t, "processor healthz", http.StatusOK, statusOf(t, roles.processorAPI, healthPath))

	assertDiff(t, "processor spans after one GetGraph and one /healthz", getGraphSpans(polls+1),
		endedSpans(roles.processorSpan))
	roles.ingester.send()
	assertCode(t, <-roles.ingesterDone, exitOK, roles.ingesterLogs)
	roles.processor.stopCleanly(t)
}

func TestTheIngesterStopFitsTenSeconds(t *testing.T) {
	setup, _, _ := ingesterOn(t, []string{testNATSURL})
	role := ingesterStack(setup, newLifecycle(testLogger(&logBuffer{})), testLogger(&logBuffer{}))
	worst := lo.SumBy(role.layers, func(current layer) time.Duration { return current.stopBudget })
	if worst > ingesterStopLimit {
		t.Errorf("worst-case ingester stop = %v, want at most %v", worst, ingesterStopLimit)
	}
}
