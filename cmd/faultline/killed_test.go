package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	killPhases          = 3
	killBatchesPerPhase = 20
	killCallsPerBatch   = 50
	killServices        = 6
	killSteps           = killPhases * killBatchesPerPhase
	killWindowSize      = 2 * time.Second
	killEndStep         = killWindowSize / (killSteps + 1)
	killWrittenAfter    = time.Minute
	killHeartbeatAfter  = time.Hour
	killHeartbeatID     = "kill-final-heartbeat"
	killIdleAfter       = "5m"
	killCheckpointEvery = time.Second
	killInstance        = "processor-a"
	killReachDeadline   = leaseTTL + 30*time.Second
	killCatchUpDeadline = 60 * time.Second
	killGraphPoll       = 100 * time.Millisecond
	killRandomPoints    = 3
	killWriteFrames     = 3
	killStopDeadline    = 30 * time.Second
	killLeaseQuiet      = time.Second
	killIngesterSettle  = 5 * time.Second
	killLogTail         = 40
	killExporters       = 8
	killAckedBeforeKill = 200
	killIntruder        = "intruder/1"
	killIngesterName    = "ingester-a"
	killStressVariable  = "TEST_KILL_STRESS"
	jsAckPrefix         = "$JS.ACK."
	objectSubjectFormat = "$O.%s."
	nodeIDFormat        = "svc:%s/%s"
	ackStreamSequenceAt = 3
	killCheckpointTick  = max(killCheckpointEvery, killWindowSize)
	killMaxDelay        = 2 * killCheckpointTick
	killSeedVariable    = "TEST_KILL_SEED"
	killPointsVariable  = "TEST_KILL_RANDOM_POINTS"
	killTTLVariable     = "TEST_KILL_REAL_TTL"
	killPointerKey      = "p0"
	headersEnd          = "\r\n\r\n"
	chunkSubjectFormat  = "$O.%s.C."
	purgeSubjectPrefix  = "$JS.API.STREAM.PURGE."
	createSubjectPrefix = "$JS.API.CONSUMER.CREATE."
	directGetPrefix     = "$JS.API.DIRECT.GET."
	streamGetPrefix     = "$JS.API.STREAM.MSG.GET."
	pointerSubject      = "$KV." + checkpoint.PointerBucket + "." + killPointerKey
	objectStreamFormat  = "OBJ_%s"
	objectBucketP0      = "checkpoints_p0"
)

type nodeView struct {
	ID               string
	Served           uint64
	Unanswered       uint64
	UnansweredFailed uint64
	Final            string
}

type edgeView struct {
	Source string
	Target string
	Calls  uint64
	Failed uint64
	Final  string
}

type graphView struct {
	Watermark time.Time
	Nodes     []nodeView
	Edges     []edgeView
}

func (v graphView) calls() uint64 {
	var total uint64
	for _, edge := range v.Edges {
		total += edge.Calls
	}
	return total
}

func graphViewOf(graph *faultlinev1.Graph) graphView {
	nodes := graph.GetNodes()
	view := graphView{
		Watermark: graph.GetWatermark().AsTime(),
		Nodes: lo.Map(nodes, func(node *faultlinev1.Node, _ int) nodeView {
			return nodeView{
				ID:               node.GetId(),
				Served:           node.GetServedRequests(),
				Unanswered:       node.GetUnansweredCalls(),
				UnansweredFailed: node.GetUnansweredFailedCalls(),
				Final:            node.GetFinal().String(),
			}
		}),
		Edges: lo.Map(graph.GetEdges(), func(edge *faultlinev1.Edge, _ int) edgeView {
			return edgeView{
				Source: nodes[edge.GetSourceIndex()].GetId(),
				Target: nodes[edge.GetTargetIndex()].GetId(),
				Calls:  edge.GetCalls(),
				Failed: edge.GetFailedCalls(),
				Final:  edge.GetFinal().String(),
			}
		}),
	}
	slices.SortFunc(view.Nodes, func(a, b nodeView) int {
		return strings.Compare(a.ID, b.ID)
	})
	slices.SortFunc(view.Edges, func(a, b edgeView) int {
		return strings.Compare(a.Source+" "+a.Target, b.Source+" "+b.Target)
	})
	return view
}

type killWorld struct {
	cluster *natsjstest.Cluster
	proxy   *natsjstest.Proxy
	stream  string
	values  map[string]string
	graphs  faultlinev1connect.GraphServiceClient
	feed    *feedGate
}

func newKillWorld(t *testing.T, proxied bool) *killWorld {
	t.Helper()
	world := &killWorld{
		cluster: natsjstest.Start(t, singleNATSNode),
	}
	url := world.cluster.URLs()[0]
	if proxied {
		world.proxy = natsjstest.StartProxy(t, url)
		url = world.proxy.URL()
	}
	apiAddress := localAddress(t)
	world.values = map[string]string{
		envNATSURL:            url,
		envInstance:           killInstance,
		envAPIAddr:            apiAddress,
		envCheckpointDir:      t.TempDir(),
		envCheckpointInterval: killCheckpointEvery.String(),
		envIdleAdvanceAfter:   killIdleAfter,
		envWindowSize:         killWindowSize.String(),
		envEarlyFirePeriod:    killWindowSize.String(),
	}
	setup, err := loadProcessor(processOf(world.values))
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	world.stream = setup.Partition.Streams.Stream(0)
	world.graphs = faultlinev1connect.NewGraphServiceClient(h2cClient(t), "http://"+apiAddress, connect.WithGRPC())
	conn := dialTest(t, world.cluster.URLs()...)
	if err := conn.EnsureTopology(t.Context(), setup.Partition.Streams, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	return world
}

func (w *killWorld) topology(t *testing.T) natsjs.Topology {
	t.Helper()
	setup, err := loadProcessor(processOf(w.values))
	if err != nil {
		t.Fatalf("loadProcessor: %v", err)
	}
	return setup.Partition.Streams
}

type nodeCounts struct {
	ID               string
	Served           uint64
	Unanswered       uint64
	UnansweredFailed uint64
}

type edgeCounts struct {
	Source string
	Target string
	Calls  uint64
	Failed uint64
}

type countsView struct {
	Nodes []nodeCounts
	Edges []edgeCounts
}

func (v graphView) counts() countsView {
	return countsView{
		Nodes: lo.Map(v.Nodes, func(node nodeView, _ int) nodeCounts {
			return nodeCounts{
				ID:               node.ID,
				Served:           node.Served,
				Unanswered:       node.Unanswered,
				UnansweredFailed: node.UnansweredFailed,
			}
		}),
		Edges: lo.Map(v.Edges, func(edge edgeView, _ int) edgeCounts {
			return edgeCounts{
				Source: edge.Source,
				Target: edge.Target,
				Calls:  edge.Calls,
				Failed: edge.Failed,
			}
		}),
	}
}

type killLedger struct {
	phases [][][]span.Span
	want   countsView
}

func killService(step int) string {
	return fmt.Sprintf("svc-%d", step%killServices)
}

func isServerHalf(half span.Span, _ int) bool {
	return half.Kind == kind.Server
}

func killRecords(t *testing.T) killLedger {
	t.Helper()
	phases := make([][][]span.Span, killPhases)
	var carried []span.Span
	for step := range killSteps {
		phase, index := step/killBatchesPerPhase, step%killBatchesPerPhase
		end := telemetrytest.Epoch().Add(time.Duration(step+1) * killEndStep)
		caller, callee := killService(step), killService(step+1)
		created := processingtest.TimedCalls(t, uint64(step*killCallsPerBatch), killCallsPerBatch, caller, callee, end)
		spans := slices.Concat(carried, created.Spans)
		carried = nil
		if index == killBatchesPerPhase-1 && phase < killPhases-1 {
			carried = lo.Filter(spans, isServerHalf)
			spans = lo.Reject(spans, isServerHalf)
		}
		phases[phase] = append(phases[phase], spans)
	}
	return killLedger{
		phases: phases,
		want:   expectedCounts(),
	}
}

func nodeID(service string) string {
	return fmt.Sprintf(nodeIDFormat, telemetrytest.Namespace, service)
}

func expectedCounts() countsView {
	served := make(map[string]uint64, killServices)
	calls := make(map[edgeCounts]uint64, killServices)
	for step := range killSteps {
		edge := edgeCounts{
			Source: nodeID(killService(step)),
			Target: nodeID(killService(step + 1)),
		}
		calls[edge] += killCallsPerBatch
		served[edge.Target] += killCallsPerBatch
	}
	want := countsView{
		Nodes: lo.MapToSlice(served, func(id string, requests uint64) nodeCounts {
			return nodeCounts{
				ID:     id,
				Served: requests,
			}
		}),
		Edges: lo.MapToSlice(calls, func(edge edgeCounts, total uint64) edgeCounts {
			edge.Calls = total
			return edge
		}),
	}
	slices.SortFunc(want.Nodes, func(a, b nodeCounts) int {
		return strings.Compare(a.ID, b.ID)
	})
	slices.SortFunc(want.Edges, func(a, b edgeCounts) int {
		return strings.Compare(a.Source+" "+a.Target, b.Source+" "+b.Target)
	})
	return want
}

func (w *killWorld) write(t *testing.T, batches [][]span.Span) {
	t.Helper()
	publisher, err := dialTest(t, w.cluster.URLs()...).Publisher(w.topology(t))
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	written, err := journal.New(publisher, &journal.Options{
		Tenant:         defaultTenant,
		Topology:       w.topology(t),
		PublishTimeout: testPublishBudget,
		Clock: fixedClock{
			now: telemetrytest.Epoch().Add(killWrittenAfter),
		},
	})
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	for _, spans := range batches {
		if err := written.Append(t.Context(), spans); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := written.Close(t.Context()); err != nil {
		t.Fatalf("close journal: %v", err)
	}
}

func (w *killWorld) closeWindows(t *testing.T) {
	t.Helper()
	data, err := record.EncodeHeartbeat(record.HeartbeatTenant, 0, telemetrytest.Epoch().Add(killHeartbeatAfter))
	if err != nil {
		t.Fatalf("encode the heartbeat: %v", err)
	}
	js := natsjstest.JetStream(t, w.cluster.URLs()[0])
	message := &nats.Msg{
		Subject: w.topology(t).Subject(0),
		Data:    data,
	}
	if _, err := js.PublishMsg(t.Context(), message, jetstream.WithMsgID(killHeartbeatID)); err != nil {
		t.Fatalf("publish the heartbeat: %v", err)
	}
}

func (w *killWorld) writeLast(t *testing.T, batches [][]span.Span) {
	t.Helper()
	w.write(t, batches)
	w.closeWindows(t)
}

type processorLife struct {
	command *exec.Cmd
	logs    *logBuffer
	exited  chan struct{}
	waitErr error
}

func (w *killWorld) start(t *testing.T) *processorLife {
	t.Helper()
	return startRole(t, commandProcessor, w.values)
}

func startRole(t *testing.T, role string, values map[string]string) *processorLife {
	t.Helper()
	command := exec.CommandContext(t.Context(), os.Args[0], role)
	command.Env = append(os.Environ(), reexecFlag+"=1")
	for name, value := range values {
		command.Env = append(command.Env, name+"="+value)
	}
	life := &processorLife{
		command: command,
		logs:    &logBuffer{},
		exited:  make(chan struct{}),
	}
	command.Stderr = life.logs
	if err := command.Start(); err != nil {
		t.Fatalf("start the %s: %v", role, err)
	}
	go func() {
		defer close(life.exited)
		life.waitErr = command.Wait()
	}()
	t.Cleanup(func() {
		<-life.exited
	})
	return life
}

func (l *processorLife) tail() string {
	lines := strings.Split(strings.TrimRight(l.logs.String(), "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-killLogTail):], "\n")
}

func (l *processorLife) exitedBefore(t *testing.T, what string) {
	t.Helper()
	t.Fatalf("the process exited with %v (%v) before %s:\n%s", l.command.ProcessState, l.waitErr, what, l.tail())
}

func (l *processorLife) await(t *testing.T, what string, reached <-chan struct{}) {
	t.Helper()
	select {
	case <-reached:
	case <-l.exited:
		l.exitedBefore(t, what)
	case <-time.After(killReachDeadline):
		t.Fatalf("no %s within %v:\n%s", what, killReachDeadline, l.tail())
	}
}

func (l *processorLife) poll(t *testing.T, what string, within time.Duration, done func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if done() {
			return true
		}
		select {
		case <-l.exited:
			l.exitedBefore(t, what)
		case <-time.After(killGraphPoll):
		}
	}
	return false
}

func (l *processorLife) reach(t *testing.T, what string, done func() bool) {
	t.Helper()
	if !l.poll(t, what, killReachDeadline, done) {
		t.Fatalf("no %s within %v:\n%s", what, killReachDeadline, l.tail())
	}
}

func (l *processorLife) kill(t *testing.T) {
	t.Helper()
	if err := l.command.Process.Signal(syscall.SIGKILL); err != nil {
		<-l.exited
		l.exitedBefore(t, "its SIGKILL")
	}
	<-l.exited
	status, _ := l.command.ProcessState.Sys().(syscall.WaitStatus)
	if !status.Signaled() || status.Signal() != syscall.SIGKILL {
		l.exitedBefore(t, "its SIGKILL")
	}
}

func (l *processorLife) exit(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case <-l.exited:
		return l.command.ProcessState.ExitCode()
	case <-time.After(within):
	}
	if err := l.command.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("SIGKILL: %v", err)
	}
	<-l.exited
	t.Fatalf("the process did not exit within %v:\n%s", within, l.tail())
	return 0
}

func (l *processorLife) stop(t *testing.T) {
	t.Helper()
	if err := l.command.Process.Signal(syscall.SIGTERM); err != nil {
		<-l.exited
		l.exitedBefore(t, "its SIGTERM")
	}
	if code := l.exit(t, killStopDeadline); code != exitOK {
		t.Errorf("the last life ended with %v (%v):\n%s", l.command.ProcessState, l.waitErr, l.tail())
	}
}

func (l *processorLife) resumedOnce(t *testing.T, life string) {
	t.Helper()
	if resumed := len(l.logs.all(t, eventFeedResumed)); resumed != 1 {
		t.Errorf("the %s logged %s %d times, want once per start:\n%s", life, eventFeedResumed, resumed, l.tail())
	}
}

func (w *killWorld) leases(t *testing.T) jetstream.KeyValue {
	t.Helper()
	kv, err := natsjstest.JetStream(t, w.cluster.URLs()[0]).KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}
	return kv
}

func (w *killWorld) expireLease(t *testing.T) {
	t.Helper()
	if os.Getenv(killTTLVariable) != "" {
		t.Logf("%s: the next life waits for the lease to expire", killTTLVariable)
		return
	}
	if err := w.leases(t).Delete(t.Context(), leaseKeyP0); err != nil {
		t.Fatalf("delete the lease key: %v", err)
	}
}

type pointerState struct {
	Epoch   uint64 `json:"epoch"`
	Current *struct {
		NUID     string `json:"nuid"`
		Position uint64 `json:"position"`
	} `json:"current"`
}

func (w *killWorld) pointer(t *testing.T) (pointerState, bool) {
	t.Helper()
	kv, err := natsjstest.JetStream(t, w.cluster.URLs()[0]).KeyValue(t.Context(), checkpoint.PointerBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return pointerState{}, false
	}
	if err != nil {
		t.Fatalf("pointer bucket: %v", err)
	}
	entry, err := kv.Get(t.Context(), killPointerKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return pointerState{}, false
	}
	if err != nil {
		t.Fatalf("pointer: %v", err)
	}
	var state pointerState
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		t.Fatalf("decode the pointer: %v", err)
	}
	return state, true
}

func (w *killWorld) committed(t *testing.T) func() bool {
	t.Helper()
	return func() bool {
		state, found := w.pointer(t)
		return found && state.Current != nil
	}
}

func (w *killWorld) logPointer(t *testing.T, after string) {
	t.Helper()
	state, found := w.pointer(t)
	if !found || state.Current == nil {
		t.Logf("after %s: no committed checkpoint", after)
		return
	}
	t.Logf("after %s: epoch %d, checkpoint %s through %d", after, state.Epoch, state.Current.NUID, state.Current.Position)
}

func (w *killWorld) logKilledState(t *testing.T, point string) {
	t.Helper()
	state, found := w.pointer(t)
	through := "none"
	if found && state.Current != nil {
		through = strconv.FormatUint(state.Current.Position, 10)
	}
	t.Logf("state after %s: epoch %d, pointer through %s, feed forwarded through %d",
		point, state.Epoch, through, w.feed.forwarded.Load())
}

func (w *killWorld) graph(t *testing.T, life *processorLife, done func(v graphView) bool) (graphView, bool) {
	t.Helper()
	var last graphView
	reached := life.poll(t, "the graph", killCatchUpDeadline, func() bool {
		response, err := w.graphs.GetGraph(t.Context(), connect.NewRequest(&faultlinev1.GetGraphRequest{}))
		if err != nil {
			return false
		}
		last = graphViewOf(response.Msg.GetGraph())
		return done(last)
	})
	return last, reached
}

func windowsClosed(v graphView) bool {
	return !v.Watermark.Before(telemetrytest.Epoch().Add(killWindowSize))
}

func uninterruptedGraph(t *testing.T, ledger killLedger) graphView {
	t.Helper()
	world := newKillWorld(t, false)
	world.write(t, slices.Concat(ledger.phases[:killPhases-1]...))
	world.writeLast(t, ledger.phases[killPhases-1])
	life := world.start(t)
	view, reached := world.graph(t, life, windowsClosed)
	life.stop(t)
	if !reached {
		t.Fatalf("the uninterrupted run did not close its windows: %+v\n%s", view, life.tail())
	}
	if diff := cmp.Diff(ledger.want, view.counts()); diff != "" {
		t.Fatalf("the uninterrupted graph (-records +graph):\n%s", diff)
	}
	return view
}

type killWait func(t *testing.T, life *processorLife)

type killPoint struct {
	name string
	arm  func(w *killWorld) killWait
}

func (w *killWorld) holdFirst(match func(f *natsjstest.Frame) bool) killWait {
	reached := make(chan struct{})
	var taken atomic.Bool
	verdict, _ := natsjstest.Hold()
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !match(f) || !taken.CompareAndSwap(false, true) {
			return natsjstest.Forward()
		}
		close(reached)
		return verdict
	})
	return func(t *testing.T, life *processorLife) {
		t.Helper()
		life.await(t, "held frame", reached)
	}
}

func (w *killWorld) loseFirst(match func(f *natsjstest.Frame) bool) killWait {
	before := w.proxy.AnswersAltered()
	var taken atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !match(f) || !taken.CompareAndSwap(false, true) {
			return natsjstest.Forward()
		}
		return natsjstest.LoseReply()
	})
	return func(t *testing.T, life *processorLife) {
		t.Helper()
		life.reach(t, "lost answer", func() bool {
			return w.proxy.AnswersAltered() > before
		})
	}
}

func subjectWith(prefix string) func(f *natsjstest.Frame) bool {
	return func(f *natsjstest.Frame) bool {
		return strings.HasPrefix(f.Subject, prefix)
	}
}

func chunkRead(f *natsjstest.Frame) bool {
	object := fmt.Sprintf(objectStreamFormat, objectBucketP0)
	read := strings.HasPrefix(f.Subject, directGetPrefix+object) || strings.HasPrefix(f.Subject, streamGetPrefix+object)
	return read && bytes.Contains(f.Payload, []byte(fmt.Sprintf(chunkSubjectFormat, objectBucketP0)))
}

func checkpointWrite(f *natsjstest.Frame) bool {
	object := strings.HasPrefix(f.Subject, fmt.Sprintf(objectSubjectFormat, objectBucketP0))
	return object || f.Subject == pointerSubject
}

type commitCAS struct {
	lock   sync.Mutex
	writes int
	staked string
}

func (c *commitCAS) match(f *natsjstest.Frame) bool {
	if f.Subject != pointerSubject {
		return false
	}
	nuid := pointerNUID(f.Payload)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.writes++
	if c.writes == 1 {
		c.staked = nuid
		return false
	}
	return nuid != c.staked
}

func pointerNUID(payload []byte) string {
	body := payload
	if at := bytes.Index(payload, []byte(headersEnd)); at >= 0 {
		body = payload[at+len(headersEnd):]
	}
	var state pointerState
	if err := json.Unmarshal(body, &state); err != nil || state.Current == nil {
		return ""
	}
	return state.Current.NUID
}

func (w *killWorld) holdWrite(frame int, delay time.Duration) killWait {
	reached := make(chan struct{})
	var staked atomic.Bool
	var seen atomic.Int32
	var held *natsjstest.HeldFrame
	var subject string
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !checkpointWrite(f) {
			return natsjstest.Forward()
		}
		if f.Subject == pointerSubject && staked.CompareAndSwap(false, true) {
			return natsjstest.Forward()
		}
		if int(seen.Add(1)) != frame {
			return natsjstest.Forward()
		}
		verdict, frameHeld := natsjstest.Hold()
		held, subject = frameHeld, f.Subject
		close(reached)
		return verdict
	})
	return func(t *testing.T, life *processorLife) {
		t.Helper()
		life.await(t, fmt.Sprintf("checkpoint write %d", frame), reached)
		release := time.AfterFunc(delay, held.Release)
		defer release.Stop()
		life.await(t, fmt.Sprintf("answer to checkpoint write %d", frame), held.Answered())
		t.Logf("reached: checkpoint write %d after the stake (%s) answered", frame, subject)
	}
}

type feedGate struct {
	prefix    string
	from      int32
	seen      atomic.Int32
	forwarded atomic.Uint64
	reached   chan struct{}
}

func newFeedGate(stream string) *feedGate {
	return &feedGate{
		prefix:  jsAckPrefix + stream + ".",
		reached: make(chan struct{}),
	}
}

func (g *feedGate) withhold(f *natsjstest.Frame) bool {
	if !strings.HasPrefix(f.Reply, g.prefix) {
		return false
	}
	count := g.seen.Add(1)
	if g.from == 0 || count < g.from {
		g.forwarded.Store(streamPosition(f.Reply))
		return false
	}
	if count == g.from {
		close(g.reached)
	}
	return true
}

func streamPosition(reply string) uint64 {
	fields := strings.Split(strings.TrimPrefix(reply, jsAckPrefix), ".")
	position, err := strconv.ParseUint(fields[ackStreamSequenceAt], 10, 64)
	if err != nil {
		return 0
	}
	return position
}

func (w *killWorld) withholdFrom(frame int, delay time.Duration) killWait {
	w.feed.from = int32(frame)
	return func(t *testing.T, life *processorLife) {
		t.Helper()
		life.await(t, fmt.Sprintf("record %d withheld", frame), w.feed.reached)
		t.Logf("reached: record %d withheld, the feed forwarded through stream position %d", frame, w.feed.forwarded.Load())
		time.Sleep(delay)
	}
}

func killSeed(t *testing.T) uint64 {
	t.Helper()
	seed := rand.Uint64()
	if given := os.Getenv(killSeedVariable); given != "" {
		parsed, err := strconv.ParseUint(given, 10, 64)
		if err != nil {
			t.Fatalf("%s=%q: %v", killSeedVariable, given, err)
		}
		seed = parsed
	}
	t.Logf("%s=%d", killSeedVariable, seed)
	return seed
}

func randomPointCount(t *testing.T) int {
	t.Helper()
	given := os.Getenv(killPointsVariable)
	if given == "" {
		return killRandomPoints
	}
	count, err := strconv.Atoi(given)
	if err != nil {
		t.Fatalf("%s=%q: %v", killPointsVariable, given, err)
	}
	return count
}

type randomKind struct {
	name   string
	frames int
	arm    func(w *killWorld, frame int, delay time.Duration) killWait
}

func randomKinds() []randomKind {
	return []randomKind{
		{
			name:   "withheld_from_record",
			frames: killBatchesPerPhase,
			arm:    (*killWorld).withholdFrom,
		},
		{
			name:   "write",
			frames: killWriteFrames,
			arm:    (*killWorld).holdWrite,
		},
	}
}

func randomPoints(t *testing.T) []killPoint {
	t.Helper()
	rng := rand.New(rand.NewPCG(killSeed(t), 0))
	kinds := randomKinds()
	count := randomPointCount(t)
	points := make([]killPoint, 0, count)
	for index := range count {
		chosen := kinds[rng.IntN(len(kinds))]
		frame := 1 + rng.IntN(chosen.frames)
		delay := time.Duration(rng.Int64N(int64(killMaxDelay/time.Millisecond))) * time.Millisecond
		points = append(points, killPoint{
			name: fmt.Sprintf("random_%d_%s_%d_after_%s", index, chosen.name, frame, delay),
			arm: func(w *killWorld) killWait {
				return chosen.arm(w, frame, delay)
			},
		})
	}
	return points
}

func killPoints(t *testing.T) []killPoint {
	t.Helper()
	fixed := []killPoint{
		{
			name: "undelivered_object_chunk",
			arm: func(w *killWorld) killWait {
				return w.holdFirst(subjectWith(fmt.Sprintf(chunkSubjectFormat, objectBucketP0)))
			},
		},
		{
			name: "undelivered_journal_purge_after_the_pointer_update",
			arm: func(w *killWorld) killWait {
				return w.holdFirst(subjectWith(purgeSubjectPrefix + w.stream))
			},
		},
		{
			name: "undelivered_chunk_read_while_restoring",
			arm: func(w *killWorld) killWait {
				return w.holdFirst(chunkRead)
			},
		},
		{
			name: "undelivered_consumer_create_after_the_stake",
			arm: func(w *killWorld) killWait {
				return w.holdFirst(subjectWith(createSubjectPrefix + w.stream))
			},
		},
		{
			name: "unanswered_pointer_commit_cas",
			arm: func(w *killWorld) killWait {
				return w.loseFirst(new(commitCAS).match)
			},
		},
		{
			name: "unanswered_journal_purge",
			arm: func(w *killWorld) killWait {
				return w.loseFirst(subjectWith(purgeSubjectPrefix + w.stream))
			},
		},
		{
			name: "unanswered_consumer_create",
			arm: func(w *killWorld) killWait {
				return w.loseFirst(subjectWith(createSubjectPrefix + w.stream))
			},
		},
	}
	return append(fixed, randomPoints(t)...)
}

func killedGraph(t *testing.T, ledger killLedger, point killPoint, want graphView) graphView {
	t.Helper()
	world := newKillWorld(t, true)
	world.write(t, ledger.phases[0])
	warm := world.start(t)
	warm.reach(t, "first checkpoint", world.committed(t))
	warm.kill(t)
	warm.resumedOnce(t, "warm life")
	world.logPointer(t, "the warm life")
	world.expireLease(t)

	world.write(t, ledger.phases[1])
	world.feed = newFeedGate(world.stream)
	wait := point.arm(world)
	world.proxy.Withhold(world.feed.withhold)
	killed := world.start(t)
	wait(t, killed)
	killed.kill(t)
	world.proxy.Intercept(nil)
	world.proxy.Withhold(nil)
	world.logKilledState(t, point.name)
	world.expireLease(t)

	world.writeLast(t, ledger.phases[killPhases-1])
	last := world.start(t)
	got, reached := world.graph(t, last, func(v graphView) bool {
		return !v.Watermark.Before(want.Watermark)
	})
	last.stop(t)
	last.resumedOnce(t, "last life")
	if !reached {
		t.Fatalf("the restarted processor did not reach the watermark (-uninterrupted +restarted):\n%s\n%s",
			cmp.Diff(want, got), last.tail())
	}
	return got
}

func TestAKilledProcessorRestartsToTheSameGraphAsAnUninterruptedRun(t *testing.T) {
	ledger := killRecords(t)
	want := uninterruptedGraph(t, ledger)
	for _, point := range killPoints(t) {
		t.Run(point.name, func(t *testing.T) {
			got := killedGraph(t, ledger, point, want)

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("graph after a kill at %s (-uninterrupted +restarted):\n%s", point.name, diff)
			}
		})
	}
}

func stressOnly(t *testing.T) {
	t.Helper()
	if os.Getenv(killStressVariable) == "" {
		t.Skipf("runs in make test-stress (%s=1)", killStressVariable)
	}
}

func (w *killWorld) ingesterValues(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		envNATSURL:  w.values[envNATSURL],
		envInstance: killIngesterName,
		envAPIAddr:  localAddress(t),
		envOTLPAddr: localAddress(t),
	}
}

func (w *killWorld) holdPublishes(t *testing.T) *atomic.Int32 {
	t.Helper()
	var held atomic.Int32
	subject := w.topology(t).Subject(0)
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if f.Subject != subject {
			return natsjstest.Forward()
		}
		held.Add(1)
		verdict, _ := natsjstest.Hold()
		return verdict
	})
	return &held
}

func (w *killWorld) exportUntilKilled(t *testing.T, ingester *processorLife, exporter ptraceotlp.GRPCClient) uint64 {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	var exporters sync.WaitGroup
	defer exporters.Wait()
	defer cancel()
	var acknowledged atomic.Uint64
	var traces atomic.Uint32
	for range killExporters {
		exporters.Go(func() {
			for {
				if _, err := exporter.Export(ctx, callRequest(traces.Add(1))); err != nil {
					return
				}
				acknowledged.Add(1)
			}
		})
	}
	ingester.reach(t, "acknowledged exports", func() bool {
		return acknowledged.Load() >= killAckedBeforeKill
	})
	held := w.holdPublishes(t)
	ingester.reach(t, "held publishes", func() bool {
		return held.Load() >= killExporters
	})
	ingester.kill(t)
	w.proxy.Intercept(nil)
	exporters.Wait()
	return acknowledged.Load()
}

func TestAKilledIngesterLosesNoAcknowledgedRequest(t *testing.T) {
	stressOnly(t)
	world := newKillWorld(t, true)
	values := world.ingesterValues(t)
	ingester := startRole(t, commandIngester, values)
	ingester.reach(t, "ready ingester", func() bool {
		return statusOf(t, values[envAPIAddr], readyPath) == http.StatusOK
	})
	exporter := ptraceotlp.NewGRPCClient(dialOTLP(t, values[envOTLPAddr]))
	acknowledged := world.exportUntilKilled(t, ingester, exporter)

	processor := world.start(t)
	got, reached := world.graph(t, processor, func(v graphView) bool {
		return v.calls() >= acknowledged
	})
	if !reached {
		processor.stop(t)
		t.Fatalf("the graph holds %d of %d acknowledged calls:\n%s", got.calls(), acknowledged, processor.tail())
	}
	settled := time.Now().Add(killIngesterSettle)
	got, _ = world.graph(t, processor, func(graphView) bool {
		return time.Now().After(settled)
	})
	processor.stop(t)
	if diff := cmp.Diff(acknowledged, got.calls()); diff != "" {
		t.Errorf("calls in the graph against acknowledged requests (-acknowledged +graph):\n%s", diff)
	}
}

type lostLeaseOutcome struct {
	Code    int
	Updates []leaseUpdate
}

type leaseUpdate struct {
	Operation string
	Value     string
}

func TestALeaseLostWhileServingStopsWithoutRelease(t *testing.T) {
	stressOnly(t)
	world := newKillWorld(t, false)
	world.write(t, killRecords(t).phases[0])
	life := world.start(t)
	life.reach(t, "first checkpoint", world.committed(t))
	leases := world.leases(t)
	watcher, err := leases.Watch(t.Context(), leaseKeyP0, jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("watch the lease key: %v", err)
	}
	defer stopWatching(t, watcher)
	if _, err := leases.Put(t.Context(), leaseKeyP0, []byte(killIntruder)); err != nil {
		t.Fatalf("take the lease key: %v", err)
	}

	code := life.exit(t, leaseTTL+killStopDeadline)

	want := lostLeaseOutcome{
		Code: exitFailure,
		Updates: []leaseUpdate{
			{
				Operation: jetstream.KeyValuePut.String(),
				Value:     killIntruder,
			},
		},
	}
	got := lostLeaseOutcome{
		Code:    code,
		Updates: sinceIntruder(leaseUpdatesUntilQuiet(watcher, killLeaseQuiet)),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("the lease key of a processor that lost it (-want +got):\n%s\n%s", diff, life.tail())
	}
}

func leaseUpdatesUntilQuiet(watcher jetstream.KeyWatcher, quiet time.Duration) []leaseUpdate {
	var seen []leaseUpdate
	timer := time.NewTimer(quiet)
	defer timer.Stop()
	for {
		select {
		case entry, open := <-watcher.Updates():
			if !open {
				return seen
			}
			seen = appendLeaseUpdate(seen, entry)
		case <-timer.C:
			return seen
		}
	}
}

func sinceIntruder(updates []leaseUpdate) []leaseUpdate {
	for index, update := range updates {
		if update.Value == killIntruder {
			return updates[index:]
		}
	}
	return updates
}

func appendLeaseUpdate(seen []leaseUpdate, entry jetstream.KeyValueEntry) []leaseUpdate {
	if entry == nil {
		return seen
	}
	return append(seen, leaseUpdate{
		Operation: entry.Operation().String(),
		Value:     string(entry.Value()),
	})
}

func stopWatching(t *testing.T, watcher jetstream.KeyWatcher) {
	t.Helper()
	if err := watcher.Stop(); err != nil {
		t.Errorf("stop watching the lease key: %v", err)
	}
}
