package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	streamSubject    = "spans.p0"
	streamInfoPrefix = "$JS.API.STREAM.INFO."
	repeatedAsks     = 12
	foreignSubject   = "other.p0"
	fastStop         = 100 * time.Millisecond
	readyFlipWithin  = 10 * time.Second
)

func TestTheIngesterWaitsForTheProcessorToCreateTheStreams(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	role := &stack{
		layers: publishingLayers(natsSetup(cluster.URLs()...), life, logs),
	}
	done := launchRoleWith(t, role, notify.notify, life)
	eventually(t, "waiting for the streams", lineWith(t, logs, eventState, "state", string(stateWaitingStreams)))

	ensureStreams(t, cluster.URLs()...)

	eventually(t, "serving once the streams exist", life.Served)
	notify.send()
	assertCode(t, <-done, exitOK, logs)
	want := []string{"starting", "connecting", "waiting_streams", "serving", "draining", "stopped"}
	assertDiff(t, "timeline", want, logs.timeline(t))
	diff, _ := logs.event(t, eventWaitPrefix+reasonStreamsNotReady)["error"].(string)
	if !strings.Contains(diff, "SPANS_P0 stream: want present, nats has missing") {
		t.Errorf("streams_not_ready diff = %q, want the missing stream named", diff)
	}
	assertNoLine(t, logs, eventLayerStopFailed)
}

func TestABadTopologyConfigExitsOneNotWaits(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	js := natsjstest.JetStream(t, cluster.URLs()[0])
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     testTopology().Stream(0),
		Subjects: []string{foreignSubject},
	})
	if err != nil {
		t.Fatalf("create a foreign stream: %v", err)
	}
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	role := &stack{
		layers: publishingLayers(natsSetup(cluster.URLs()...), life, logs),
	}

	assertCode(t, <-launchRoleWith(t, role, notify.notify, life), exitFailure, logs)

	assertDiff(t, "stage", stageAwaitStreams, logs.event(t, eventStartFailed)["stage"])
	assertDiff(t, "timeline", []string{"starting", "connecting", "aborting", "stopped"}, logs.timeline(t))
	assertErrors(t, logs, eventStartFailed)
}

func TestAStreamNoteIsAWarningNotAWait(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs := &logBuffer{}
	life := newLifecycle(testLogger(logs))
	options := testJournalOptions()
	options.Topology.MaxBytesPerPartition = 2 * testStreamBytes
	conn, conns := connLayer(natsSetup(cluster.URLs()...), life, testLogger(logs))
	published, _ := journalLayer(conns, options, life, testLogger(logs))
	notify := newFakeNotify()
	done := launchRoleWith(t, &stack{
		layers: []layer{conn, published},
	}, notify.notify, life)

	eventually(t, "serving", life.Served)
	notify.send()

	assertCode(t, <-done, exitOK, logs)
	diff, _ := logs.event(t, eventStreamNotes)["diff"].(string)
	if !strings.Contains(diff, "max_bytes") {
		t.Errorf("stream_notes diff = %q, want the max_bytes note", diff)
	}
	assertNoLine(t, logs, eventWaitPrefix+reasonStreamsNotReady)
}

func TestTheJournalLayerKeepsASignalDuringItsWait(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	logs := &logBuffer{}
	life := newLifecycle(testLogger(logs))
	layers := publishingLayers(natsSetup(cluster.URLs()...), life, logs)
	connView := newLayerSupervisor(t, logs, &layers[0])
	if err := layers[0].unit.start(t.Context(), connView); err != nil {
		t.Fatalf("conn start: %v", err)
	}
	view := newLayerSupervisor(t, logs, &layers[1])
	ctx, cancel := context.WithCancelCause(t.Context())
	started := make(chan error, 1)
	go func() {
		started <- layers[1].unit.start(ctx, view)
	}()
	eventually(t, "waiting for the streams", lineWith(t, logs, eventState, "state", string(stateWaitingStreams)))

	cancel(errTestSignal)

	err := <-started
	seen := &observed{
		err:    err,
		stage:  layerJournal,
		cause:  errTestSignal,
		signal: errTestSignal,
	}
	if code := outcomeOf(seen).code; !errors.Is(err, context.Canceled) || code != exitOK {
		t.Errorf("start = %v (code %d), want the cancel and code %d", err, code, exitOK)
	}
	if err := stopLayer(t, &layers[1], view); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
	if err := stopLayer(t, &layers[0], connView); err != nil {
		t.Errorf("conn stop: %v", err)
	}
}

func holdFirstPublish(proxy *natsjstest.Proxy) (*natsjstest.HeldFrame, <-chan struct{}) {
	verdict, held := natsjstest.Hold()
	fired := make(chan struct{})
	var once atomic.Bool
	proxy.Intercept(func(frame *natsjstest.Frame) natsjstest.Verdict {
		if frame.Subject == streamSubject && once.CompareAndSwap(false, true) {
			close(fired)
			return verdict
		}
		return natsjstest.Forward()
	})
	return held, fired
}

func TestAJournalStopOnAClosedConnectionFailsFast(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	logs := &logBuffer{}
	life := newLifecycle(testLogger(logs))
	layers := publishingLayers(natsSetup(proxy.URL()), life, logs)
	views := []layerSupervisor{newLayerSupervisor(t, logs, &layers[0]), newLayerSupervisor(t, logs, &layers[1])}
	for index := range layers {
		if err := layers[index].unit.start(t.Context(), views[index]); err != nil {
			t.Fatalf("start %s: %v", layers[index].name, err)
		}
	}
	held, fired := holdFirstPublish(proxy)
	t.Cleanup(held.Release)
	appended := make(chan error, 1)
	journals := layers[1].unit.(*journalUnit)
	go func() {
		appended <- journals.opened().Append(t.Context(), []span.Span{telemetrytest.Client(t, 1, 1, "GET /", nil)})
	}()
	<-fired

	if err := stopLayer(t, &layers[0], views[0]); err != nil {
		t.Errorf("conn stop under an in-flight publish = %v, want nil", err)
	}
	began := time.Now()
	err := stopLayer(t, &layers[1], views[1])
	took := time.Since(began)

	if err != nil || took > fastStop {
		t.Errorf("journal stop on a closed connection = %v after %s, want nil within %s", err, took, fastStop)
	}
	if appendErr := <-appended; !errors.Is(appendErr, ingest.ErrUnavailable) {
		t.Errorf("the in-flight append = %v, want unavailable", appendErr)
	}
	if journals.ready() {
		t.Error("the journal reports ready on a closed connection")
	}
}

func TestTheIngesterReadinessFollowsThePublishConnection(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs, notify := &logBuffer{}, newFakeNotify()
	logger := testLogger(logs)
	life := newLifecycle(logger)
	address := localAddress(t)
	role := &stack{}
	release := make(chan struct{})
	held := &fakeLayer{
		name:     "held",
		stopHold: release,
	}
	routes := roleRoutes(readiness{
		life:   life,
		layers: role.hooks,
	})
	role.layers = append(
		[]layer{httpLayer(&Config{
			APIAddr: address,
		}, noopTracer{}, routes, "", logger)},
		append(publishingLayers(natsSetup(cluster.URLs()...), life, logs), held.layer(&eventLog{}))...,
	)
	done := launchRoleWith(t, role, notify.notify, life)
	eventually(t, "ready", func() bool { return statusOf(t, address, readyPath) == http.StatusOK })

	cluster.Stop(0)
	eventually(t, "not ready without NATS", func() bool {
		return statusOf(t, address, readyPath) == http.StatusServiceUnavailable
	})
	cluster.Restart(0)
	eventually(t, "ready again after the reconnect", func() bool { return statusOf(t, address, readyPath) == http.StatusOK })
	notify.send()
	eventually(t, "draining", lineWith(t, logs, eventState, "state", string(stateDraining)))

	draining := statusOf(t, address, readyPath)
	health := statusOf(t, address, healthPath)
	close(release)

	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "ready and healthy while draining", []int{http.StatusServiceUnavailable, http.StatusOK}, []int{
		draining,
		health,
	})
}

func TestTheJournalStopWaitsForAnInFlightAppendAndRefusesNewOnes(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	logs := &logBuffer{}
	life := newLifecycle(testLogger(logs))
	layers := publishingLayers(natsSetup(proxy.URL()), life, logs)
	views := []layerSupervisor{newLayerSupervisor(t, logs, &layers[0]), newLayerSupervisor(t, logs, &layers[1])}
	for index := range layers {
		if err := layers[index].unit.start(t.Context(), views[index]); err != nil {
			t.Fatalf("start %s: %v", layers[index].name, err)
		}
	}
	held, fired := holdFirstPublish(proxy)
	journals := layers[1].unit.(*journalUnit)
	appended := make(chan error, 1)
	go func() {
		appended <- journals.opened().Append(t.Context(), []span.Span{telemetrytest.Client(t, 1, 1, "GET /", nil)})
	}()
	<-fired
	release := time.AfterFunc(fastStop, held.Release)
	defer release.Stop()

	began := time.Now()
	err := stopLayer(t, &layers[1], views[1])
	took := time.Since(began)

	if err != nil || took < fastStop {
		t.Errorf("journal stop = %v after %s, want nil only once the held append ends (%s)", err, took, fastStop)
	}
	if appendErr := <-appended; appendErr != nil {
		t.Errorf("the held append = %v, want its PubAck", appendErr)
	}
	late := journals.opened().Append(t.Context(), []span.Span{telemetrytest.Client(t, 2, 1, "GET /", nil)})
	if late == nil || journals.opened().Stats().Closing != 1 {
		t.Errorf("an append after the stop = %v (closing %d), want it refused", late, journals.opened().Stats().Closing)
	}
	if err := stopLayer(t, &layers[0], views[0]); err != nil {
		t.Errorf("conn stop = %v, want nil", err)
	}
}

func TestAServerWithoutJetStreamIsAVisibleWait(t *testing.T) {
	proxy := natsjstest.StartProxy(t, natsjstest.StartWithoutJetStream(t))
	var asked atomic.Int32
	proxy.Intercept(func(frame *natsjstest.Frame) natsjstest.Verdict {
		if strings.HasPrefix(frame.Subject, streamInfoPrefix) {
			asked.Add(1)
		}
		return natsjstest.Forward()
	})
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	role := &stack{
		layers: publishingLayers(natsSetup(proxy.URL()), life, logs),
	}
	done := launchRoleWith(t, role, notify.notify, life)
	eventually(t, "waiting for JetStream", lineWith(t, logs, eventState, "reason", reasonJetStreamUnavailable))
	eventually(t, "several verification rounds", func() bool { return asked.Load() > repeatedAsks })

	notify.send()

	assertCode(t, <-done, exitOK, logs)
	warned := logs.all(t, eventWaitPrefix+reasonJetStreamUnavailable)
	assertDiff(t, "one warning for the class", 1, len(warned))
	assertDiff(t, "timeline", []string{"starting", "connecting", "waiting_streams", "draining", "stopped"}, logs.timeline(t))
	assertDiff(t, "canceled stage", stageAwaitStreams, logs.event(t, eventStartCanceled)["stage"])
}

func TestTheJournalBudgetFollowsThePublishTimeout(t *testing.T) {
	options := testJournalOptions()
	options.PublishTimeout = maxPublishTimeout

	current, _ := journalLayer(nil, options, newLifecycle(testLogger(&logBuffer{})), testLogger(&logBuffer{}))

	assertDiff(t, "stop budget", maxPublishTimeout+exitGrace, current.stopBudget)
}

func TestAStreamWaitWarnsOncePerClassAndRemindsWithTheCurrentError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		life := newLifecycle(testLogger(logs))
		waiting := &journalUnit{
			phases: life,
			logger: testLogger(logs),
		}
		wait := &streamWait{
			warned: map[string]bool{},
		}
		rounds := []struct {
			at     time.Duration
			reason string
			err    error
		}{
			{
				at:     0,
				reason: reasonStreamsNotReady,
				err:    errA,
			},
			{
				at:     10 * time.Second,
				reason: reasonJetStreamUnavailable,
				err:    errB,
			},
			{
				at:     31 * time.Second,
				reason: reasonJetStreamUnavailable,
				err:    errC,
			},
			{
				at:     61 * time.Second,
				reason: reasonStreamsNotReady,
				err:    errors.Join(errX, errZ),
			},
		}
		began := time.Now()

		for _, round := range rounds {
			time.Sleep(time.Until(began.Add(round.at)))
			waiting.keepWaiting(t.Context(), wait, round.reason, round.err)
		}

		assertDiff(t, "one warning per class", []any{errA.Error(), errB.Error()}, append(
			valuesOf(logs.all(t, eventWaitPrefix+reasonStreamsNotReady), "error"),
			valuesOf(logs.all(t, eventWaitPrefix+reasonJetStreamUnavailable), "error")...,
		))
		assertDiff(t, "the state is entered once", []string{"waiting_streams"}, logs.timeline(t))
		reminders := logs.all(t, eventStillWaiting)
		got := [][]any{valuesOf(reminders, "elapsed_seconds"), valuesOf(reminders, "reason"), valuesOf(reminders, "last_error")}
		want := [][]any{
			{31.0, 61.0},
			{reasonJetStreamUnavailable, reasonStreamsNotReady},
			{errC.Error(), errX.Error() + "; " + errZ.Error()},
		}
		assertDiff(t, "reminders carry the current class and error on one line", want, got)
	})
}
