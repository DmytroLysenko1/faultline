package main

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"
	"google.golang.org/grpc/codes"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	quickStatsInterval  = 100 * time.Millisecond
	quickReload         = time.Second
	quickRetry          = 200 * time.Millisecond
	quickCheck          = 200 * time.Millisecond
	quickReread         = 500 * time.Millisecond
	pastMaxStaleness    = 46 * time.Second
	alertRaised         = "raised"
	alertCleared        = "cleared"
	authAlertPrefix     = "auth."
	tokenInfoPrefix     = "$JS.API.STREAM.INFO." + tokenStream
	staleAlertEvent     = "auth.token_directory_stale"
	regressedAlertEvent = "auth.token_sequence_regressed"
	blockedAlertEvent   = "auth.token_revoke_blocked"
	driftAlertEvent     = "auth.token_bucket_drift"
	epochAlertEvent     = "auth.token_epoch_changed"
	massAlertEvent      = "auth.token_mass_revoke"
)

type shiftedClock struct {
	offset atomic.Int64
}

func (c *shiftedClock) Now() time.Time {
	return time.Now().Add(time.Duration(c.offset.Load()))
}

func (c *shiftedClock) shift(by time.Duration) {
	c.offset.Store(int64(by))
}

func layerNamed(t *testing.T, role *stack, name string) *layer {
	t.Helper()
	for index := range role.layers {
		if role.layers[index].name == name {
			return &role.layers[index]
		}
	}
	t.Fatalf("no layer %s", name)
	return nil
}

func tunedIngester(
	t *testing.T,
	setup *ingesterSetup,
	logs *logBuffer,
	tune func(directory *directoryUnit),
) (*fakeNotify, <-chan int, *lifecycle) {
	t.Helper()
	notify := newFakeNotify()
	life := newLifecycle(testLogger(logs))
	role := ingesterStack(setup, life, testLogger(logs))
	layerNamed(t, role, layerStats).unit.(*statsUnit).interval = quickStatsInterval
	tune(layerNamed(t, role, layerDirectory).unit.(*directoryUnit))
	return notify, launchRoleWith(t, role, notify.notify, life), life
}

func quickDirectory(directory *directoryUnit) {
	directory.options.ReloadInterval = quickReload
	directory.options.RetryEvery = quickRetry
	directory.options.CheckTime = quickCheck
	directory.options.RereadTime = quickReread
}

func TestAStaleDirectoryNeverAffectsLivenessOrReadiness(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	grantTestToken(t, defaultTenant, cluster.URLs()...)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	var checksLost atomic.Bool
	proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if checksLost.Load() && strings.HasPrefix(f.Subject, tokenInfoPrefix) {
			return natsjstest.LoseReply()
		}
		return natsjstest.Forward()
	})
	logs := &logBuffer{}
	setup, api, otlpAddress := ingesterOn(t, []string{proxy.URL()})
	clock := &shiftedClock{}
	notify, done, life := tunedIngester(t, setup, logs, func(directory *directoryUnit) {
		quickDirectory(directory)
		directory.clock = clock
	})
	eventually(t, "serving", life.Served)

	checksLost.Store(true)
	clock.shift(pastMaxStaleness)
	stale := []any{
		exportOnce(t, otlpAddress, otlptest.WithTestToken()),
		statusOf(t, api, healthPath),
		statusOf(t, api, readyPath),
	}
	eventually(t, "the stale alert", lineWith(t, logs, staleAlertEvent, "state", alertRaised))
	checksLost.Store(false)
	eventually(t, "the stale alert cleared", lineWith(t, logs, staleAlertEvent, "state", alertCleared))
	fresh := exportOnce(t, otlpAddress, otlptest.WithTestToken())

	notify.send()
	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "export, health and readiness while stale", []any{
		codes.Unavailable,
		http.StatusOK,
		http.StatusOK,
	}, stale)
	assertDiff(t, "export once fresh again on the shifted clock", codes.OK, fresh)
}

func TestIngesterRaisesAuthAlerts(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	grantTestToken(t, defaultTenant, cluster.URLs()...)
	grantToken(t, tokenWith(secondTokenID), defaultTenant, cluster.URLs()...)
	logs := &logBuffer{}
	setup, _, _ := ingesterOn(t, cluster.URLs())
	notify, done, life := tunedIngester(t, setup, logs, quickDirectory)
	eventually(t, "serving", life.Served)
	js := natsjstest.JetStream(t, cluster.URLs()[0])

	purgeTokenBucket(t, js)
	eventually(t, "the mass revoke alert", lineWith(t, logs, massAlertEvent, "state", alertRaised))
	changeTokenBucket(t, js, func(config *jetstream.StreamConfig) {
		config.AllowRollup = true
	})
	eventually(t, "the drift alert", lineWith(t, logs, driftAlertEvent, "state", alertRaised))

	notify.send()
	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "alerts of the final report", map[string]any{
		staleAlertEvent:     false,
		regressedAlertEvent: false,
		blockedAlertEvent:   false,
		driftAlertEvent:     true,
		epochAlertEvent:     false,
		massAlertEvent:      true,
	}, authAlerts(t, logs))
}

func TestIngesterStartsOnAnIrreversibleBucketAndAlerts(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	grantTestToken(t, defaultTenant, cluster.URLs()...)
	changeTokenBucket(t, natsjstest.JetStream(t, cluster.URLs()[0]), func(config *jetstream.StreamConfig) {
		config.DenyPurge = true
	})
	logs := &logBuffer{}
	setup, _, otlpAddress := ingesterOn(t, cluster.URLs())
	notify, done, life := tunedIngester(t, setup, logs, quickDirectory)
	eventually(t, "serving", life.Served)
	eventually(t, "the revoke blocked alert", lineWith(t, logs, blockedAlertEvent, "state", alertRaised))
	exported := exportOnce(t, otlpAddress, otlptest.WithTestToken())

	notify.send()
	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "an export on an irreversible bucket", codes.OK, exported)
	assertDiff(t, "alerts of the final report", map[string]any{
		staleAlertEvent:     false,
		regressedAlertEvent: false,
		blockedAlertEvent:   true,
		driftAlertEvent:     false,
		epochAlertEvent:     false,
		massAlertEvent:      false,
	}, authAlerts(t, logs))
}

func authAlerts(t *testing.T, logs *logBuffer) map[string]any {
	t.Helper()
	final := logs.all(t, eventPipelineStats)
	alerts, _ := final[len(final)-1]["alerts"].(map[string]any)
	return lo.PickBy(alerts, func(event string, _ any) bool {
		return strings.HasPrefix(event, authAlertPrefix)
	})
}

func purgeTokenBucket(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	stream, err := js.Stream(t.Context(), tokenStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if err := stream.Purge(t.Context()); err != nil {
		t.Fatalf("Purge: %v", err)
	}
}
