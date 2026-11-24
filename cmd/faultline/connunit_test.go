package main

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	testConnectTimeout = time.Second
	testInstance       = "faultline-test"
	natsUser           = "faultline"
	natsPassword       = "secret"
	missingNATSHost    = "nats://faultline-missing.invalid:4222"
	waitedFakeTime     = 61 * time.Second
	badCredentialRuns  = 5
	singleNATSNode     = 1
	threeNATSNodes     = 3
	refusedAdvertised  = "127.0.0.1:1"
	connectingStage    = "conn/dial"
	stopRaceRounds     = 200
)

func natsSetup(urls ...string) *natsConfig {
	return &natsConfig{
		URLs:           urls,
		ConnectTimeout: testConnectTimeout,
		Instance:       testInstance,
	}
}

func hostPort(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	return parsed.Host
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := new(net.ListenConfig).Listen(t.Context(), network, loopback)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return port
}

func lineWith(t *testing.T, logs *logBuffer, event, key, value string) func() bool {
	t.Helper()
	return func() bool {
		for _, record := range logs.all(t, event) {
			if record[key] == value {
				return true
			}
		}
		return false
	}
}

func TestNATSComingUpLateIsJoinedWithoutRestart(t *testing.T) {
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	port := freePort(t)
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	role := connOnly(natsSetup("nats://"+address), life, logs)
	done := launchRoleWith(t, role, notify.notify, life)
	eventually(t, "the first unreachable class", lineWith(t, logs, eventNATSUnreachable, "reason", "no_servers"))

	natsjstest.Start(t, singleNATSNode, func(options *server.Options) {
		options.Port = port
	})

	eventually(t, "serving once NATS is up", life.Served)
	notify.send()
	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "timeline", []string{"starting", "connecting", "serving", "draining", "stopped"}, logs.timeline(t))
	assertDiff(t, "first class", "no_servers", logs.event(t, eventNATSUnreachable)["reason"])
	assertNoLine(t, logs, eventLayerStopFailed)
}

func connOnly(cfg *natsConfig, life *lifecycle, logs *logBuffer) *stack {
	conn, _ := connLayer(cfg, life, testLogger(logs))
	return &stack{
		layers: []layer{conn},
	}
}

func TestASilentPortIsReportedWhileWaiting(t *testing.T) {
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	silent := natsjstest.SilentEndpoint(t)
	done := launchRoleWith(t, connOnly(natsSetup(silent.URL), life, logs), notify.notify, life)

	eventually(t, "the silent port reported", lineWith(t, logs, eventNATSUnreachable, "reason", "no_nats_greeting"))
	notify.send()

	assertCode(t, <-done, exitOK, logs)
	warned := logs.event(t, eventNATSUnreachable)
	assertDiff(t, "level", levelWarn, warned["level"])
	if message, _ := warned["last_error"].(string); !strings.Contains(message, hostPort(t, silent.URL)) {
		t.Errorf("last_error = %q, want the silent port named", message)
	}
	assertDiff(t, "canceled stage", connectingStage, logs.event(t, eventStartCanceled)["stage"])
	assertDiff(t, "timeline", []string{"starting", "connecting", "draining", "stopped"}, logs.timeline(t))
}

func TestAlternatingDialClassesWarnOncePerClass(t *testing.T) {
	flapping := natsjstest.FlappingEndpoint(t)
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		life := newLifecycle(testLogger(logs))
		current, _ := connLayer(natsSetup(flapping.URL), life, testLogger(logs))
		view := newLayerSupervisor(t, logs, &current)
		ctx, cancel := context.WithCancelCause(t.Context())
		started := make(chan error, 1)
		go func() {
			started <- current.unit.start(ctx, view)
		}()

		time.Sleep(waitedFakeTime)
		cancel(errTestSignal)

		if err := <-started; !errors.Is(err, context.Canceled) {
			t.Fatalf("start = %v, want the cancel", err)
		}
		reasons := valuesOf(logs.all(t, eventNATSUnreachable), "reason")
		assertDiff(t, "warned classes", []any{"no_nats_greeting", "network"}, reasons)
		reminders := logs.all(t, eventStillWaiting)
		assertDiff(t, "reminders", []any{30.0, 60.0}, valuesOf(reminders, "elapsed_seconds"))
		assertDiff(t, "seen classes", []any{"network", "no_nats_greeting"}, reminders[1]["seen_reasons"])
		assertDiff(t, "state", "connecting", reminders[0]["state"])
		perURL, _ := reminders[1]["per_url"].([]any)
		assertDiff(t, "hosts of the last pass", []any{hostPort(t, flapping.URL)}, valuesOfAny(perURL, "host"))
		if reminders[1]["reason"] == nil || reminders[1]["last_error"] == nil {
			t.Errorf("still_waiting = %v, want the current class and its error", reminders[1])
		}
	})
}

func valuesOf(records []map[string]any, key string) []any {
	values := make([]any, 0, len(records))
	for _, record := range records {
		values = append(values, record[key])
	}
	return values
}

func authCluster(t *testing.T, nodes int, advertise string) *natsjstest.Cluster {
	t.Helper()
	return natsjstest.Start(t, nodes, func(options *server.Options) {
		options.Username = natsUser
		options.Password = natsPassword
		options.ClientAdvertise = advertise
	})
}

func TestBadCredentialsBehindAnotherFailingServerExitOne(t *testing.T) {
	tests := []struct {
		name string
		args func(t *testing.T) []string
		runs int
	}{
		{
			name: "a refused second url",
			args: func(t *testing.T) []string { return []string{natsjstest.RefusedURL(t)} },
			runs: badCredentialRuns,
		},
		{
			name: "a second url that does not resolve",
			args: func(*testing.T) []string { return []string{missingNATSHost} },
			runs: badCredentialRuns,
		},
		{
			name: "a second url that closes at once",
			args: func(t *testing.T) []string { return []string{natsjstest.ClosingEndpoint(t).URL} },
			runs: badCredentialRuns,
		},
		{
			name: "a silent second url",
			args: func(t *testing.T) []string { return []string{natsjstest.SilentEndpoint(t).URL} },
			runs: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urls := append([]string{authCluster(t, singleNATSNode, "").URLs()[0]}, tt.args(t)...)
			for range tt.runs {
				assertBadCredentialsExitOne(t, natsSetup(urls...))
			}
		})
	}
	t.Run("one url whose neighbour advertises an unreachable address", func(t *testing.T) {
		cluster := authCluster(t, threeNATSNodes, refusedAdvertised)
		assertBadCredentialsExitOne(t, natsSetup(cluster.URLs()[0]))
	})
}

func assertBadCredentialsExitOne(t *testing.T, cfg *natsConfig) {
	t.Helper()
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	done := launchRoleWith(t, connOnly(cfg, life, logs), notify.notify, life)
	assertCode(t, <-done, exitFailure, logs)
	failed := logs.event(t, eventStartFailed)
	assertDiff(t, "stage", connectingStage, failed["stage"])
	if message, _ := failed["error"].(string); !strings.Contains(strings.ToLower(message), "authorization violation") {
		t.Errorf("start_failed error = %q, want the authorization violation", message)
	}
	assertErrors(t, logs, eventStartFailed)
}

func TestASignalInterruptsTheURLPass(t *testing.T) {
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	first, second := natsjstest.SilentEndpoint(t), natsjstest.SilentEndpoint(t)
	done := launchRoleWith(t, connOnly(natsSetup(first.URL, second.URL), life, logs), notify.notify, life)
	eventually(t, "the url pass begins", func() bool {
		return first.Accepted()+second.Accepted() > 2
	})

	signalled := time.Now()
	notify.send()

	assertCode(t, <-done, exitOK, logs)
	if took := time.Since(signalled); took > 2*testConnectTimeout {
		t.Errorf("the pass ended %s after the signal, want at once (at most %s)", took, 2*testConnectTimeout)
	}
	assertDiff(t, "canceled stage", connectingStage, logs.event(t, eventStartCanceled)["stage"])
	assertDiff(t, "warned before the pass", []any{"no_nats_greeting"}, valuesOf(logs.all(t, eventNATSUnreachable), "reason"))
}

func TestTheConnLayerKeepsASignalDuringItsWait(t *testing.T) {
	refused := natsjstest.RefusedURL(t)
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		current, _ := connLayer(natsSetup(refused), newLifecycle(testLogger(logs)), testLogger(logs))
		view := newLayerSupervisor(t, logs, &current)
		ctx, cancel := context.WithCancelCause(t.Context())
		started := make(chan error, 1)
		go func() {
			started <- current.unit.start(ctx, view)
		}()
		time.Sleep(dialPause + dialPause/2)
		synctest.Wait()

		cancel(errTestSignal)

		err := <-started
		seen := &observed{
			err:    err,
			stage:  layerConn,
			cause:  errTestSignal,
			signal: errTestSignal,
		}
		if code := outcomeOf(seen).code; !errors.Is(err, context.Canceled) || code != exitOK {
			t.Errorf("start = %v (code %d), want the cancel and code %d", err, code, exitOK)
		}
		if err := stopLayer(t, &current, view); err != nil {
			t.Errorf("stop = %v, want nil", err)
		}
	})
}

func valuesOfAny(entries []any, key string) []any {
	values := make([]any, 0, len(entries))
	for _, entry := range entries {
		record, _ := entry.(map[string]any)
		values = append(values, record[key])
	}
	return values
}

func TestAConnectionClosedAfterServingIsCodeOne(t *testing.T) {
	port := freePort(t)
	withPassword := func(password string) func(options *server.Options) {
		return func(options *server.Options) {
			options.Port = port
			options.Username = natsUser
			options.Password = password
		}
	}
	cluster := natsjstest.Start(t, singleNATSNode, withPassword(natsPassword))
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	authed := "nats://" + natsUser + ":" + natsPassword + "@" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	done := launchRoleWith(t, connOnly(natsSetup(authed), life, logs), notify.notify, life)
	eventually(t, "serving", life.Served)

	cluster.Stop(0)
	natsjstest.Start(t, singleNATSNode, withPassword("rotated"))

	assertCode(t, <-done, exitFailure, logs)
	exit := logs.event(t, eventComponentExit)
	assertDiff(t, "component", componentConn, exit["component"])
	assertDiff(t, "level", levelError, exit["level"])
	assertErrors(t, logs, eventComponentExit)
	assertNoLine(t, logs, eventLayerStopFailed)
	assertNoLine(t, logs, eventStopFailed)
	if message, _ := exit["error"].(string); !strings.Contains(message, errConnectionClosed.Error()) ||
		!strings.Contains(strings.ToLower(message), "authorization violation") {
		t.Errorf("component_exit error = %q, want the closed connection and its cause", message)
	}
	if lineWith(t, logs, "nats.error", "connection", "publish")() == false {
		t.Errorf("no nats.error names the cause:\n%s", logs.String())
	}
}

func closedLinks(closedRole int) *natsjs.Conn {
	channels := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	links := make([]*natsjs.Link, len(channels))
	for index := range channels {
		links[index] = &natsjs.Link{
			NATS:   &nats.Conn{},
			Closed: channels[index],
		}
	}
	if closedRole >= 0 {
		close(channels[closedRole])
	}
	return &natsjs.Conn{
		Publish: links[0],
		Feed:    links[1],
		Control: links[2],
	}
}

func TestAnyLinkClosedOutsideItsStopIsAnExit(t *testing.T) {
	for role := range connectionsPerDial {
		conn := closedLinks(role)
		watched := &connUnit{
			conn:     conn,
			stopping: make(chan struct{}),
		}

		err := watched.watch()

		if !errors.Is(err, errConnectionClosed) {
			t.Errorf("link %d closed: watch = %v, want errConnectionClosed", role, err)
		}
	}
}

func TestALinkClosedByItsStopIsNoExit(t *testing.T) {
	for range stopRaceRounds {
		conn := closedLinks(0)
		watched := &connUnit{
			conn:     conn,
			stopping: make(chan struct{}),
		}
		close(watched.stopping)

		if err := watched.watch(); err != nil {
			t.Fatalf("watch with stopping and a closed link both ready = %v, want nil", err)
		}
	}
}

func TestTheStopSeesALinkThatClosedBeforeIt(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	conn := dialTest(t, cluster.URLs()...)
	stopped := &connUnit{
		conn: conn,
	}
	before := stopped.closedBefore()

	conn.Feed.NATS.Close()

	after := stopped.closedBefore()
	assertDiff(t, "no link closed, then the feed", []bool{true, true}, []bool{before == nil, after == conn.Feed})
}

func TestOnlyOneSideReportsAClosedConnection(t *testing.T) {
	tests := []struct {
		name string
		args bool
	}{
		{
			name: "the watcher sees it first, the stop stays clean",
		},
		{
			name: "the stop sees it first, the watcher stays quiet",
			args: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNATSNode)
			watched := &connUnit{
				conn:     dialTest(t, cluster.URLs()...),
				stopping: make(chan struct{}),
			}
			watched.conn.Feed.NATS.Close()
			stopFirst := []error{}
			if tt.args {
				stopFirst = append(stopFirst, closedOrNil(watched.claimClosedBefore()))
			}

			watchErr := watched.watch()
			claimed := closedOrNil(watched.claimClosedBefore())

			reported := lo.Filter(append(stopFirst, watchErr, claimed), func(err error, _ int) bool {
				return err != nil
			})
			assertDiff(t, "reports of one close", 1, len(reported))
		})
	}
}

func closedOrNil(link *natsjs.Link) error {
	if link == nil {
		return nil
	}
	return closedCause(link)
}

func TestTheStopReportsTheCauseOfALinkClosedBeforeIt(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	stopped := &connUnit{
		conn:     dialTest(t, cluster.URLs()...),
		stopping: make(chan struct{}),
	}
	stopped.conn.Feed.NATS.Close()

	err := stopped.stop(t.Context())

	if !errors.Is(err, errConnectionClosed) || !strings.Contains(err.Error(), testInstance+"-feed") {
		t.Errorf("stop = %v, want the closed feed link named", err)
	}
}
