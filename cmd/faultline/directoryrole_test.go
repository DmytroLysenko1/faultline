package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	tokenStream          = "KV_" + tokens.Bucket
	driftedMaxAge        = time.Hour
	driftExitWithin      = 30 * time.Second
	strangerTokenID      = "strangerid22"
	defaultTenantTokenID = "defaultuser2"
	shopTenant           = "shop"
)

func TestIngesterWaitsForTheTokenBucket(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs := &logBuffer{}
	setup, api, _ := ingesterOn(t, cluster.URLs())
	notify, done, life := launchIngester(t, setup, logs)
	eventually(t, "waiting for the tokens", lineWith(t, logs, eventState, "state", string(stateWaitingTokens)))
	assertDiff(t, "health and readiness while waiting", []int{http.StatusOK, http.StatusServiceUnavailable}, []int{
		statusOf(t, api, healthPath),
		statusOf(t, api, readyPath),
	})

	createTokenBucket(t, cluster.URLs()...)
	eventually(t, "serving", life.Served)
	notify.send()

	assertCode(t, <-done, exitOK, logs)
	want := []string{
		"starting", "connecting", "waiting_tokens", "serving", "draining", timelineFinalStats, "stopped",
	}
	assertDiff(t, "timeline", want, logs.timeline(t))
	waited, _ := lo.Find(logs.all(t, eventState), func(record map[string]any) bool {
		return record["state"] == string(stateWaitingTokens)
	})
	assertDiff(t, "wait reason", reasonBucketMissing, waited["reason"])
	assertDiff(t, "warned once", 1, len(logs.all(t, eventTokenWaitPrefix+reasonBucketMissing)))
}

func TestASignalWhileWaitingForTheTokenBucketExitsZero(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs := &logBuffer{}
	setup, _, _ := ingesterOn(t, cluster.URLs())
	notify, done, _ := launchIngester(t, setup, logs)
	eventually(t, "waiting for the tokens", lineWith(t, logs, eventState, "state", string(stateWaitingTokens)))

	notify.send()

	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "canceled stage", stageAwaitTokens, logs.event(t, eventStartCanceled)["stage"])
	assertNoLine(t, logs, eventLayerStopFailed)
}

func TestIngesterExitsOnABucketThatInjectsGrants(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	createTokenBucket(t, cluster.URLs()...)
	driftTokenBucket(t, cluster.URLs()[0])
	logs := &logBuffer{}
	setup, _, _ := ingesterOn(t, cluster.URLs())
	_, done, _ := launchIngester(t, setup, logs)

	select {
	case code := <-done:
		assertCode(t, code, exitFailure, logs)
	case <-time.After(driftExitWithin):
		t.Fatalf("the ingester kept waiting %s on a bucket that injects grants", driftExitWithin)
	}
	failed := logs.event(t, eventStartFailed)
	assertDiff(t, "failed stage and hint", []any{stageAwaitTokens, hintBucketDrift}, []any{failed["stage"], failed["hint"]})
}

func driftTokenBucket(t *testing.T, url string) {
	t.Helper()
	js := natsjstest.JetStream(t, url)
	stream, err := js.Stream(t.Context(), tokenStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	config := stream.CachedInfo().Config
	config.MaxAge = driftedMaxAge
	if _, err := js.UpdateStream(t.Context(), config); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
}

func TestOnlyAGrantedTokenIsAcceptedByTheIngester(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs := &logBuffer{}
	setup, _, otlpAddress := ingesterOn(t, cluster.URLs())
	setup.Journal.Tenant = shopTenant
	grantToken(t, tokenWith(defaultTenantTokenID), defaultTenant, cluster.URLs()...)
	notify, done, life := serveIngester(t, setup, logs)
	eventually(t, "serving", life.Served)

	got := []codes.Code{
		exportOnce(t, otlpAddress, otlptest.WithTestToken()),
		exportOnce(t, otlpAddress),
		exportOnce(t, otlpAddress, otlptest.WithToken(tokenWith(strangerTokenID))),
		exportOnce(t, otlpAddress, otlptest.WithToken(tokenWith(defaultTenantTokenID))),
	}

	notify.send()
	assertCode(t, <-done, exitOK, logs)
	want := []codes.Code{
		codes.OK,
		codes.Unauthenticated,
		codes.Unauthenticated,
		codes.Unauthenticated,
	}
	assertDiff(t, "granted, missing, unknown, another tenant", want, got)
	final := logs.all(t, eventPipelineStats)
	auth, found := final[len(final)-1]["auth"].(map[string]any)
	if !found {
		t.Fatalf("the final pipeline.stats has no auth group: %v", final[len(final)-1])
	}
	rejected, _ := auth["rejected"].(map[string]any)
	grants, _ := auth["tokens"].(map[string]any)
	assertDiff(t, "accepted, missing, unknown, tenant and grants", []any{
		float64(1),
		float64(1),
		float64(1),
		float64(1),
		float64(2),
	}, []any{
		auth["accepted"],
		auth["missing"],
		rejected["unknown"],
		rejected["tenant"],
		grants["grants"],
	})
}

func tokenWith(id string) string {
	return credential.Prefix + id + otlptest.Token[len(credential.Prefix)+len(id):]
}

func exportOnce(t *testing.T, address string, options ...grpc.DialOption) codes.Code {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address,
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, options...)...,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	}()
	_, err = ptraceotlp.NewGRPCClient(conn).Export(t.Context(), callRequest(1))
	return status.Code(err)
}
