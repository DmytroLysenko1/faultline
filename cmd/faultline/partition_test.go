package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	testFeedBytes       = 64 << 20
	testIdleAfter       = 200 * time.Millisecond
	testCheckpointEvery = time.Second
	leaseKeyP0          = "processor.p0"
	takeoverDeadline    = 15 * time.Second
	checkpointDeadline  = 15 * time.Second
	leaseLossDeadline   = 40 * time.Second
	changedMaxNodes     = 99
	laterEpoch          = 1000
	heldPointerWindow   = 2 * time.Second
	driftDeadline       = 10 * time.Second
	shortRestoreBudget  = 2 * time.Second
	finalOnlyInterval   = time.Minute
)

type partitionRun struct {
	logs  *logBuffer
	life  *lifecycle
	graph *memgraph.Store
	role  *stack
	unit  *partitionUnit
	sig   *fakeNotify
	done  <-chan int
}

func testPartitionConfig(t *testing.T) *partitionConfig {
	t.Helper()
	limits := topologytest.Limits()
	return &partitionConfig{
		Instance:           testInstance,
		Streams:            testTopology(),
		Limits:             limits,
		RestoreBudget:      restoreBudget(limits.WorstCheckpointBytes()),
		FeedMaxBytes:       testFeedBytes,
		IdleAfter:          testIdleAfter,
		CheckpointInterval: testCheckpointEvery,
		CheckpointDir:      t.TempDir(),
	}
}

func startPartition(t *testing.T, cfg *partitionConfig, urls ...string) *partitionRun {
	t.Helper()
	run := &partitionRun{
		logs:  &logBuffer{},
		graph: &memgraph.Store{},
		sig:   newFakeNotify(),
	}
	run.life = newLifecycle(testLogger(run.logs))
	conn, conns := connLayer(natsSetup(urls...), run.life, testLogger(run.logs))
	partition, unit := partitionLayer(cfg, conns, run.graph, run.life, testLogger(run.logs))
	run.unit = unit
	run.role = &stack{
		layers: []layer{
			conn,
			partition,
		},
	}
	run.done = launchRoleWith(t, run.role, run.sig.notify, run.life)
	return run
}

func (r *partitionRun) stopCleanly(t *testing.T) {
	t.Helper()
	r.sig.send()
	assertCode(t, <-r.done, exitOK, r.logs)
}

func appendSpans(t *testing.T, urls ...string) {
	t.Helper()
	publisher, err := dialTest(t, urls...).Publisher(testTopology())
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	written, err := journal.New(publisher, testJournalOptions())
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	if err := written.Append(t.Context(), []span.Span{telemetrytest.Client(t, 1, 1, "GET /", nil)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := written.Close(t.Context()); err != nil {
		t.Fatalf("close journal: %v", err)
	}
}

func servedWithRecords(t *testing.T, run *partitionRun, urls ...string) {
	t.Helper()
	eventually(t, "serving", run.life.Served)
	appendSpans(t, urls...)
	eventuallyWithin(t, "caught up", checkpointDeadline, lineWith(t, run.logs, eventFeedCaughtUp, "partition", "p0"))
}

func leaseKeyExists(t *testing.T, url string) bool {
	t.Helper()
	kv, err := natsjstest.JetStream(t, url).KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}
	_, err = kv.Get(t.Context(), leaseKeyP0)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("get lease key: %v", err)
	}
	return true
}

func TestAPartitionServesAndReleasesItsLeaseOnSigterm(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "serving", run.life.Served)
	if _, err := run.graph.Latest(t.Context()); err != nil {
		t.Errorf("graph after serving: %v, want the restored snapshot", err)
	}
	if !layerReady(&run.role.layers[1]) {
		t.Errorf("partition not ready while serving")
	}

	run.stopCleanly(t)

	want := []string{"starting", "connecting", "waiting_lease", "restoring", "serving", "draining", "stopped"}
	assertDiff(t, "timeline", want, run.logs.timeline(t))
	assertDiff(t, "stops", []string{"layer_stopped p0", "layer_stopped conn"}, stopLines(t, run.logs))
	resumed := run.logs.event(t, eventFeedResumed)
	got := lo.PickByKeys(resumed, []string{"partition", "found"})
	assertDiff(t, "feed_resumed", map[string]any{
		"partition": "p0",
		"found":     false,
	}, got)
	for _, event := range []string{eventFeedUnprocessed, eventCheckpointStopped} {
		if run.logs.event(t, event)["partition"] != "p0" {
			t.Errorf("no %s line of p0:\n%s", event, run.logs.String())
		}
	}
	assertDiff(t, "unprocessed batches", float64(0), run.logs.event(t, eventFeedUnprocessed)["batches"])
	assertErrors(t, run.logs)
	if leaseKeyExists(t, cluster.URLs()[0]) {
		t.Errorf("lease key %s still held after a clean stop", leaseKeyP0)
	}
}

func finalOnlyConfig(t *testing.T) *partitionConfig {
	t.Helper()
	cfg := testPartitionConfig(t)
	cfg.CheckpointInterval = finalOnlyInterval
	return cfg
}

func TestProcessorStopWritesAFinalCheckpointAndTheNextStartResumesFromIt(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	cfg := finalOnlyConfig(t)
	first := startPartition(t, cfg, cluster.URLs()...)
	servedWithRecords(t, first, cluster.URLs()...)
	first.stopCleanly(t)
	stopped := first.logs.event(t, eventCheckpointStopped)
	if through, _ := stopped["committed_through"].(float64); through == 0 {
		t.Fatalf("checkpoint.stopped %v, want a committed position", stopped)
	}

	second := startPartition(t, cfg, cluster.URLs()...)
	eventually(t, "serving again", second.life.Served)
	second.stopCleanly(t)

	resumed := second.logs.event(t, eventFeedResumed)
	if resumed["found"] != true || resumed["after"] == float64(0) {
		t.Errorf("second feed_resumed %v, want the final checkpoint found", resumed)
	}
}

func eventuallyWithin(t *testing.T, what string, within time.Duration, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s not reached within %v", what, within)
		}
		time.Sleep(pollInterval)
	}
}

func TestASecondProcessorWaitsForTheLeaseAndTakesOver(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	first := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "first serving", first.life.Served)
	second := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "second waiting", lineWith(t, second.logs, eventState, "state", string(stateWaitingLease)))
	time.Sleep(2 * leaseRetryEvery)
	if second.life.Served() {
		t.Fatalf("second processor serves while the first holds the lease")
	}

	first.stopCleanly(t)
	eventuallyWithin(t, "second serving after the release", takeoverDeadline, second.life.Served)
	second.stopCleanly(t)

	want := []string{"starting", "connecting", "waiting_lease", "restoring", "serving", "draining", "stopped"}
	assertDiff(t, "second timeline", want, second.logs.timeline(t))
	assertErrors(t, second.logs)
}

func TestASignalWhileWaitingForTheLeaseIsCodeZero(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	first := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "first serving", first.life.Served)
	second := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "second waiting", lineWith(t, second.logs, eventState, "state", string(stateWaitingLease)))
	time.Sleep(2 * leaseRetryEvery)

	second.stopCleanly(t)

	assertDiff(t, "stage", "p0/acquire", second.logs.event(t, eventStartCanceled)["stage"])
	assertNoLine(t, second.logs, eventLayerStopFailed)
	if !leaseKeyExists(t, cluster.URLs()[0]) {
		t.Errorf("the waiting processor's stop released the first processor's lease")
	}
	first.stopCleanly(t)
}

func TestAnIncompatibleCheckpointFailsClosedAndItsHintDiscardsIt(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	cfg := testPartitionConfig(t)
	first := startPartition(t, cfg, cluster.URLs()...)
	servedWithRecords(t, first, cluster.URLs()...)
	first.stopCleanly(t)

	changed := testPartitionConfig(t)
	changed.Limits.MaxNodes = changedMaxNodes
	refused := startPartition(t, changed, cluster.URLs()...)
	assertCode(t, <-refused.done, exitFailure, refused.logs)
	failed := refused.logs.event(t, eventRestoreFailed)
	hint, _ := failed["hint"].(string)
	if failed["stage"] != "p0/resume" || failed["discardable"] != true ||
		!strings.HasPrefix(hint, envCheckpointRecovery+"=discard:p0:") {
		t.Fatalf("restore_failed %v, want a discardable failure with its hint", failed)
	}
	assertErrors(t, refused.logs, eventRestoreFailed)
	assertDiff(t, "timeline", []string{"starting", "connecting", "waiting_lease", "restoring", "aborting", "stopped"},
		refused.logs.timeline(t))

	target, err := parseRecovery(strings.TrimPrefix(hint, envCheckpointRecovery+"="), 0)
	if err != nil {
		t.Fatalf("parse the hint %q: %v", hint, err)
	}
	changed.Recovery = target
	discarded := startPartition(t, changed, cluster.URLs()...)
	eventually(t, "serving after the discard", discarded.life.Served)
	discarded.stopCleanly(t)
	discarded.logs.event(t, eventCheckpointDiscarded)
	assertNoLine(t, discarded.logs, eventRecoveryUnused)
	assertErrors(t, discarded.logs)
}

func TestARecoveryTargetLeftAfterItsDiscardOnlyWarns(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	cfg := testPartitionConfig(t)
	target := processing.DiscardTarget{
		FirstSeq: 1,
	}
	cfg.Recovery = &target
	run := startPartition(t, cfg, cluster.URLs()...)
	eventually(t, "serving", run.life.Served)
	run.stopCleanly(t)
	assertDiff(t, "target", formatRecovery(target), run.logs.event(t, eventRecoveryUnused)["target"])
	assertErrors(t, run.logs)
}

func TestALeaseLostWhileServingStopsWithCodeOneAndOneError(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the lease TTL")
	}
	cluster := natsjstest.Start(t, singleNATSNode)
	run := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "serving", run.life.Served)
	kv, err := natsjstest.JetStream(t, cluster.URLs()[0]).KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}
	if _, err := kv.Put(t.Context(), leaseKeyP0, []byte("intruder/1")); err != nil {
		t.Fatalf("take the key: %v", err)
	}
	eventuallyWithin(t, "ready drops", leaseLossDeadline, func() bool { return !layerReady(&run.role.layers[1]) })

	assertCode(t, <-run.done, exitFailure, run.logs)
	assertErrors(t, run.logs, eventLeaseLost)
	lost := run.logs.event(t, eventLeaseLost)
	if lost["partition"] != "p0" || lost["stage"] != stageServing ||
		!strings.Contains(lost["error"].(string), "partition lease lost") {
		t.Errorf("lease.lost %v, want p0 lost while serving", lost)
	}
}

func TestTheWaitRemindersCarryTheirLevel(t *testing.T) {
	logs := &logBuffer{}
	began := time.Now().Add(-2 * waitReportPeriod)
	streams := &streamWait{}
	(&journalUnit{
		phases: newLifecycle(testLogger(&logBuffer{})),
	}).enterWait(streams, reasonStreamsNotReady)
	streams.began, streams.next = began, began
	dials := newDialWait(began)
	dials.next = began
	for _, wait := range []*reminder{&newLeaseWait(began, "p0").reminder, &streams.reminder, &dials.reminder} {
		if wait.next.After(time.Now()) {
			wait.next = began
		}
		wait.remind(t.Context(), testLogger(logs))
	}
	got := picked(logs.all(t, eventStillWaiting), "state", "level")
	want := []map[string]any{
		{
			"state": string(stateWaitingLease),
			"level": levelInfo,
		},
		{
			"state": string(stateWaitingStreams),
			"level": levelWarn,
		},
		{
			"state": string(stateConnecting),
			"level": levelWarn,
		},
	}
	assertDiff(t, "reminders", want, got)
}

func holdFirstSubject(proxy *natsjstest.Proxy, part string) (*natsjstest.HeldFrame, <-chan struct{}) {
	verdict, held := natsjstest.Hold()
	fired := make(chan struct{})
	var once sync.Once
	proxy.Intercept(func(frame *natsjstest.Frame) natsjstest.Verdict {
		if !strings.Contains(frame.Subject, part) {
			return natsjstest.Forward()
		}
		taken := false
		once.Do(func() {
			taken = true
			close(fired)
		})
		if taken {
			return verdict
		}
		return natsjstest.Forward()
	})
	return held, fired
}

func TestASignalWhileRestoringReleasesTheLeaseAndExitsZero(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	held, fired := holdFirstSubject(proxy, checkpoint.PointerBucket)
	t.Cleanup(held.Release)
	run := startPartition(t, testPartitionConfig(t), proxy.URL())
	<-fired
	eventually(t, "restoring", lineWith(t, run.logs, eventState, "state", string(stateRestoring)))

	run.stopCleanly(t)

	stage, _ := run.logs.event(t, eventStartCanceled)["stage"].(string)
	if !strings.HasPrefix(stage, "p0/") {
		t.Errorf("start_canceled stage %q, want a restore step of p0", stage)
	}
	assertNoLine(t, run.logs, eventLayerStopFailed)
	if leaseKeyExists(t, cluster.URLs()[0]) {
		t.Errorf("lease key %s still held after a signal while restoring", leaseKeyP0)
	}
}

func TestAPointerTakenByALaterEpochStopsTheStoreAsALostLease(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	servedWithRecords(t, run, cluster.URLs()...)
	part := run.unit
	eventuallyWithin(t, "a committed checkpoint", checkpointDeadline, func() bool {
		return part.store.Stats().Committed > 0
	})
	pointers, err := natsjstest.JetStream(t, cluster.URLs()[0]).KeyValue(t.Context(), checkpoint.PointerBucket)
	if err != nil {
		t.Fatalf("pointer bucket: %v", err)
	}
	entry, err := pointers.Get(t.Context(), "p0")
	if err != nil {
		t.Fatalf("pointer: %v", err)
	}
	var value map[string]any
	if err := json.Unmarshal(entry.Value(), &value); err != nil {
		t.Fatalf("decode the pointer: %v", err)
	}
	value["epoch"] = laterEpoch
	taken, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode the pointer: %v", err)
	}
	if _, err := pointers.Put(t.Context(), "p0", taken); err != nil {
		t.Fatalf("take the pointer: %v", err)
	}
	appendSpans(t, cluster.URLs()...)

	assertCode(t, <-run.done, exitFailure, run.logs)
	assertErrors(t, run.logs, eventLeaseLost)
	assertDiff(t, "lost", map[string]any{
		"partition": "p0",
		"stage":     stageServing,
	},
		lo.PickByKeys(run.logs.event(t, eventLeaseLost), []string{"partition", "stage"}))
	exit := run.logs.event(t, eventComponentExit)
	assertDiff(t, "exit", map[string]any{
		"component": "p0/checkpoint",
		"followup":  true,
	},
		lo.PickByKeys(exit, []string{"component", "followup"}))
}

func TestASeveredConnectionWaitsForItsStopInsteadOfExiting(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	severed := &connUnit{
		conn:     dialTest(t, cluster.URLs()...),
		stopping: make(chan struct{}),
	}
	watched := make(chan error, 1)
	go func() {
		watched <- severed.watch()
	}()

	if err := severed.sever(t.Context()); err != nil {
		t.Fatalf("sever: %v", err)
	}
	select {
	case err := <-watched:
		t.Fatalf("the watch exited with %v before the stop, want it waiting", err)
	case <-time.After(settleTime * 10):
	}
	close(severed.stopping)
	if err := <-watched; err != nil {
		t.Errorf("watch after the stop = %v, want nil", err)
	}
	if closed := severed.claimClosedBefore(); closed != nil {
		t.Errorf("the stop claims a close the sever already owns: %s", closed.NATS.Opts.Name)
	}
}

func TestALinkClosedBeforeTheSeverIsReportedOnceByTheWatch(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	severed := &connUnit{
		conn:     dialTest(t, cluster.URLs()...),
		stopping: make(chan struct{}),
	}
	severed.conn.Feed.NATS.Close()
	watched := severed.watch()

	if err := severed.sever(t.Context()); err != nil {
		t.Fatalf("sever: %v", err)
	}
	close(severed.stopping)
	if !errors.Is(watched, errConnectionClosed) {
		t.Errorf("watch = %v, want the closed link reported", watched)
	}
	if closed := severed.claimClosedBefore(); closed != nil {
		t.Errorf("the stop reports %s a second time", closed.NATS.Opts.Name)
	}
}

func TestTheLeaseOutlivesAHeldFinalCheckpoint(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	run := startPartition(t, finalOnlyConfig(t), proxy.URL())
	servedWithRecords(t, run, cluster.URLs()...)
	held, fired := holdFirstSubject(proxy, checkpoint.PointerBucket)
	t.Cleanup(held.Release)

	run.sig.send()
	<-fired
	time.Sleep(heldPointerWindow)

	if !leaseKeyExists(t, cluster.URLs()[0]) {
		t.Errorf("the lease key went away while the final checkpoint of the stopping partition was still held (R1)")
	}
	held.Release()
	assertCode(t, <-run.done, exitOK, run.logs)
}

func TestTheNextStartDoesNotReadWhatTheCheckpointCovers(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	cfg := finalOnlyConfig(t)
	first := startPartition(t, cfg, cluster.URLs()...)
	servedWithRecords(t, first, cluster.URLs()...)
	first.stopCleanly(t)

	second := startPartition(t, cfg, cluster.URLs()...)
	eventually(t, "serving again", second.life.Served)
	time.Sleep(3 * catchUpPoll)
	second.stopCleanly(t)

	assertNoLine(t, second.logs, eventFeedCaughtUp)
}

func TestALeaseBucketThatDriftedFailsTheStartInsteadOfWaiting(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	_, err := natsjstest.JetStream(t, cluster.URLs()[0]).CreateKeyValue(t.Context(), jetstream.KeyValueConfig{
		Bucket:  lease.Bucket,
		TTL:     leaseTTL,
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create a drifted bucket: %v", err)
	}
	run := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	select {
	case code := <-run.done:
		assertCode(t, code, exitFailure, run.logs)
	case <-time.After(driftDeadline):
		run.sig.send()
		<-run.done
		t.Fatalf("a drifted lease bucket was read as waiting for the lease")
	}
	assertDiff(t, "stage", "p0/acquire", run.logs.event(t, eventStartFailed)["stage"])
	assertErrors(t, run.logs, eventStartFailed)
}

func dropEverySubject(proxy *natsjstest.Proxy, part string) <-chan struct{} {
	fired := make(chan struct{})
	var once sync.Once
	proxy.Intercept(func(frame *natsjstest.Frame) natsjstest.Verdict {
		if !strings.Contains(frame.Subject, part) {
			return natsjstest.Forward()
		}
		once.Do(func() { close(fired) })
		return natsjstest.Drop()
	})
	return fired
}

func TestARestoreThatOutlivesItsBudgetFailsWithTheBudgetCause(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	fired := dropEverySubject(proxy, checkpoint.PointerBucket)
	cfg := testPartitionConfig(t)
	cfg.RestoreBudget = shortRestoreBudget
	run := startPartition(t, cfg, proxy.URL())
	<-fired

	select {
	case code := <-run.done:
		assertCode(t, code, exitFailure, run.logs)
	case <-time.After(shortRestoreBudget + takeoverDeadline):
		run.sig.send()
		<-run.done
		t.Fatalf("restore still runs past its budget of %v", shortRestoreBudget)
	}
	failed := errorLines(t, run.logs)
	if len(failed) != 1 || !strings.Contains(fmt.Sprint(failed[0]), errRestoreTimeout.Error()) {
		t.Errorf("error lines %v, want one naming the restore budget", failed)
	}
}

func TestAStaleRecoveryTargetKeepsTheCauseAndADiscardableHint(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	first := startPartition(t, finalOnlyConfig(t), cluster.URLs()...)
	servedWithRecords(t, first, cluster.URLs()...)
	first.stopCleanly(t)

	changed := testPartitionConfig(t)
	changed.Limits.MaxNodes = changedMaxNodes
	changed.Recovery = &processing.DiscardTarget{
		Checkpoint: testCheckpointName,
	}
	refused := startPartition(t, changed, cluster.URLs()...)
	assertCode(t, <-refused.done, exitFailure, refused.logs)

	failed := refused.logs.event(t, eventRestoreFailed)
	hint, _ := failed["hint"].(string)
	cause, _ := failed["cause"].(string)
	if failed["discardable"] != true || hint == "" || strings.HasSuffix(hint, testCheckpointName) {
		t.Errorf("restore_failed %v, want discardable with a hint naming the current checkpoint", failed)
	}
	if !strings.Contains(cause, processing.ErrCheckpointUnusable.Error()) {
		t.Errorf("cause %q lost why the checkpoint cannot start the partition", cause)
	}
	assertErrors(t, refused.logs, eventRestoreFailed)
}

func TestAWaitingProcessorRemindsAtInfoWithTheHolder(t *testing.T) {
	if testing.Short() {
		t.Skip("waits one report period")
	}
	cluster := natsjstest.Start(t, singleNATSNode)
	first := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventually(t, "first serving", first.life.Served)
	second := startPartition(t, testPartitionConfig(t), cluster.URLs()...)
	eventuallyWithin(t, "still_waiting", waitReportPeriod+takeoverDeadline,
		lineWith(t, second.logs, eventStillWaiting, "state", string(stateWaitingLease)))
	second.stopCleanly(t)
	first.stopCleanly(t)

	line := second.logs.event(t, eventStillWaiting)
	holder, _ := line["holder"].(string)
	if line["level"] != levelInfo || line["partition"] != "p0" || !strings.HasPrefix(holder, testInstance+"/") {
		t.Errorf("still_waiting %v, want INFO with p0 and the holder", line)
	}
}

func TestAnUnreachableLeaseBucketWarnsWithItsLastError(t *testing.T) {
	if testing.Short() {
		t.Skip("waits one report period")
	}
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	dropEverySubject(proxy, lease.Bucket)
	run := startPartition(t, testPartitionConfig(t), proxy.URL())
	eventuallyWithin(t, "still_waiting", waitReportPeriod+takeoverDeadline,
		lineWith(t, run.logs, eventStillWaiting, "state", string(stateWaitingLease)))
	run.stopCleanly(t)

	line := run.logs.event(t, eventStillWaiting)
	if _, named := line["last_error"]; line["level"] != levelWarn || !named {
		t.Errorf("still_waiting %v, want WARN with last_error", line)
	}
}

func TestALinkClosedJustBeforeTheSeverKeepsItsCause(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	severed := &connUnit{
		conn:     dialTest(t, cluster.URLs()...),
		stopping: make(chan struct{}),
	}
	severed.conn.Feed.NATS.Close()
	<-severed.conn.Feed.Closed

	err := severed.sever(t.Context())

	if !errors.Is(err, errConnectionClosed) {
		t.Errorf("sever = %v, want the link closed outside any stop named", err)
	}
}

func TestAPointerThatCannotBeReadIsNotDiscardable(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	dropEverySubject(proxy, "GET.KV_"+checkpoint.PointerBucket)
	cfg := testPartitionConfig(t)
	cfg.RestoreBudget = shortRestoreBudget
	run := startPartition(t, cfg, proxy.URL())

	assertCode(t, <-run.done, exitFailure, run.logs)

	failed := run.logs.event(t, eventRestoreFailed)
	if _, hinted := failed["hint"]; failed["stage"] != "p0/resume" || failed["discardable"] != false || hinted {
		t.Errorf("restore_failed %v, want p0/resume, not discardable and no hint", failed)
	}
	assertErrors(t, run.logs, eventRestoreFailed)
}
