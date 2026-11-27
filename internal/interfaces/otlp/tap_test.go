package otlp_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	rejectedMessage    = "ingest token rejected"
	unavailableTokens  = "ingest token directory unavailable"
	authorization      = "authorization"
	eventAuthRejected  = "otlp.auth.rejected"
	eventAuthFailed    = "otlp.auth.failed"
	authEventPrefix    = "otlp.auth."
	authSecret         = "SECRET-DIRECTORY-DETAIL"
	floodedRequests    = 3
	unknownTokenID     = "unknowntoken"
	mismatchedTokenID  = "mismatchedid"
	foreignTokenID     = "foreigntoken"
	staleTokenID       = "staletokenid"
	brokenTokenID      = "brokentoken2"
	unparsableTokenID  = "unparsable22"
	bareTokenID        = "baresentinel"
	tenantHintHeader   = "x-tenant-hint"
	tenantHint         = "shop"
	bearerPrefix       = "Bearer "
	otherSchemePrefix  = "Basic  "
	expectedRejections = 1
)

var errDirectoryBroken = errors.New(authSecret)

func tokenWith(id string) string {
	return credential.Prefix + id + otlptest.Token[len(credential.Prefix)+credential.IDLength:]
}

func rejection(reason ingestauth.Reason, id string) error {
	return ingestauth.RejectionError{
		Reason:  reason,
		TokenID: credential.ID(id),
	}
}

var tokenOutcomes = map[string]error{
	tokenWith(unparsableTokenID): rejection(ingestauth.ReasonMalformed, ""),
	tokenWith(unknownTokenID):    rejection(ingestauth.ReasonUnknown, unknownTokenID),
	tokenWith(mismatchedTokenID): rejection(ingestauth.ReasonMismatch, mismatchedTokenID),
	tokenWith(foreignTokenID):    rejection(ingestauth.ReasonTenant, foreignTokenID),
	tokenWith(staleTokenID):      fmt.Errorf("snapshot: %w", ingestauth.ErrDirectoryStale),
	tokenWith(brokenTokenID):     errDirectoryBroken,
	tokenWith(bareTokenID):       ingestauth.ErrUnauthenticated,
}

func byToken(ctx context.Context, token string) (ingestauth.Principal, error) {
	if outcome, known := tokenOutcomes[token]; known {
		return ingestauth.Principal{}, outcome
	}
	return otlptest.AdmitToken(ctx, token)
}

func startByToken(t *testing.T, acceptor acceptorFunc) harness {
	t.Helper()
	return startGuarded(t, acceptor, byToken)
}

type sent struct {
	credentials []string
}

func carrying(values ...string) sent {
	return sent{
		credentials: values,
	}
}

func bearing(id string) sent {
	return carrying(bearerPrefix + tokenWith(id))
}

func (s sent) context(ctx context.Context) context.Context {
	if len(s.credentials) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, metadata.MD{
		authorization: s.credentials,
	})
}

func (s sent) export(t *testing.T, h harness) *status.Status {
	t.Helper()
	_, err := h.Client.Export(s.context(t.Context()), requestOf(1))
	return status.Convert(err)
}

func TestAnUnauthenticatedRequestIsRejectedBeforeDecoding(t *testing.T) {
	type want struct {
		Code          codes.Code
		DecodeEvents  int
		AcceptorCalls int64
	}
	tests := []struct {
		name string
		args sent
		want want
	}{
		{
			name: "without a token the body is never decoded",
			args: carrying(),
			want: want{
				Code: codes.Unauthenticated,
			},
		},
		{
			name: "a rejected token never reaches the decoder",
			args: bearing(unknownTokenID),
			want: want{
				Code: codes.Unauthenticated,
			},
		},
		{
			name: "an admitted token reaches the decoder",
			args: carrying(bearerPrefix + otlptest.Token),
			want: want{
				Code:         codes.Internal,
				DecodeEvents: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acceptor := &countingAcceptor{}
			h := startByToken(t, acceptor.accept)
			err := sendRaw(tt.args.context(t.Context()), h.Conn, append(malformed(), 0xff))
			got := want{
				Code:          status.Code(err),
				DecodeEvents:  len(h.Logs.Events(t, "otlp.export.decode_rejected")),
				AcceptorCalls: acceptor.calls.Load(),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEveryRejectionGivesUnauthenticatedWithOneMessage(t *testing.T) {
	tests := []struct {
		name string
		args sent
	}{
		{
			name: "no authorization header",
			args: carrying(),
		},
		{
			name: "two authorization headers",
			args: carrying(bearerPrefix+otlptest.Token, bearerPrefix+otlptest.Token),
		},
		{
			name: "another scheme",
			args: carrying(otherSchemePrefix + otlptest.Token),
		},
		{
			name: "a short credential",
			args: carrying(bearerPrefix + otlptest.Token[1:]),
		},
		{
			name: "a token Parse refuses",
			args: bearing(unparsableTokenID),
		},
		{
			name: "an unknown token id",
			args: bearing(unknownTokenID),
		},
		{
			name: "a secret that does not match",
			args: bearing(mismatchedTokenID),
		},
		{
			name: "a token of another tenant",
			args: bearing(foreignTokenID),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startByToken(t, returning(ingest.AcceptResult{}, nil))
			st := tt.args.export(t, h)
			want := statusWant{
				Code:    codes.Unauthenticated,
				Message: rejectedMessage,
			}
			got := statusWant{
				Code:       st.Code(),
				Message:    st.Message(),
				RetryDelay: retryDelay(st),
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAStaleDirectoryAnswersUnavailableWithRetryInfo(t *testing.T) {
	h := startByToken(t, returning(ingest.AcceptResult{}, nil))
	st := bearing(staleTokenID).export(t, h)
	want := statusWant{
		Code:       codes.Unavailable,
		Message:    unavailableTokens,
		RetryDelay: 5 * time.Second,
	}
	got := statusWant{
		Code:       st.Code(),
		Message:    st.Message(),
		RetryDelay: retryDelay(st),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
	if events := authEvents(t, h); len(events) != 0 {
		t.Errorf("a stale directory logged %v, want no per-request log", events)
	}
}

func authEvents(t *testing.T, h harness) []map[string]any {
	t.Helper()
	var matched []map[string]any
	for _, record := range h.Logs.Records(t) {
		if strings.HasPrefix(fmt.Sprint(record["msg"]), authEventPrefix) {
			matched = append(matched, record)
		}
	}
	return matched
}

func TestAnUnexpectedErrorIsCountedAsFailed(t *testing.T) {
	h := startByToken(t, returning(ingest.AcceptResult{}, nil))
	for range floodedRequests {
		st := bearing(brokenTokenID).export(t, h)
		if st.Code() != codes.Internal || st.Message() != internalMessageText {
			t.Fatalf("unexpected error answered %v %q, want INTERNAL %q", st.Code(), st.Message(), internalMessageText)
		}
	}
	want := []map[string]any{
		{
			"level":               "ERROR",
			"msg":                 eventAuthFailed,
			"error":               authSecret,
			"suppressed_requests": float64(0),
		},
	}
	if diff := cmp.Diff(want, withoutTime(h.Logs.Events(t, eventAuthFailed))); diff != "" {
		t.Errorf("auth failure log (-want +got):\n%s", diff)
	}
	failed := otlp.TapStats{
		Failed: floodedRequests,
	}
	if diff := cmp.Diff(failed, h.Tap.Stats()); diff != "" {
		t.Errorf("tap stats (-want +got):\n%s", diff)
	}
}

const internalMessageText = "internal error"

func TestTheHandlerNeverSeesTheAuthorizationHeader(t *testing.T) {
	var seen atomic.Pointer[metadata.MD]
	h := start(t, func(ctx context.Context, _ ingest.AcceptCommand) (ingest.AcceptResult, error) {
		incoming, _ := metadata.FromIncomingContext(ctx)
		seen.Store(&incoming)
		return ingest.AcceptResult{}, nil
	})
	ctx := metadata.AppendToOutgoingContext(t.Context(), tenantHintHeader, tenantHint)
	exportOK(t, h, ctx)
	got := *seen.Load()
	want := struct {
		Authorization []string
		TenantHint    []string
	}{
		TenantHint: []string{tenantHint},
	}
	gotHeaders := struct {
		Authorization []string
		TenantHint    []string
	}{
		Authorization: got.Get(authorization),
		TenantHint:    got.Get(tenantHintHeader),
	}
	if diff := cmp.Diff(want, gotHeaders); diff != "" {
		t.Errorf("handler metadata (-want +got):\n%s", diff)
	}
}

func withoutPeer(records []map[string]any) []map[string]any {
	for _, record := range records {
		delete(record, "peer")
	}
	return records
}

func TestRejectionFloodWritesOneWarnPerReasonPerInterval(t *testing.T) {
	h := startByToken(t, returning(ingest.AcceptResult{}, nil))
	for range floodedRequests {
		carrying().export(t, h)
		carrying(otherSchemePrefix+otlptest.Token).export(t, h)
		bearing(mismatchedTokenID).export(t, h)
	}
	h.Clock.Advance(otlp.LogInterval)
	carrying().export(t, h)
	warned := func(reason string, suppressed int) map[string]any {
		return map[string]any{
			"level":               "WARN",
			"msg":                 eventAuthRejected,
			"reason":              reason,
			"suppressed_requests": float64(suppressed),
		}
	}
	mismatch := warned("mismatch", 0)
	mismatch["token_id"] = mismatchedTokenID
	want := []map[string]any{
		warned("missing", 0),
		warned("header", 0),
		mismatch,
		warned("missing", floodedRequests-1),
	}
	if diff := cmp.Diff(want, withoutPeer(withoutTime(h.Logs.Events(t, eventAuthRejected)))); diff != "" {
		t.Errorf("rejection log (-want +got):\n%s", diff)
	}
}

func TestARejectionLogNamesItsPeer(t *testing.T) {
	h := startByToken(t, returning(ingest.AcceptResult{}, nil))
	carrying().export(t, h)
	events := h.Logs.Events(t, eventAuthRejected)
	if len(events) != expectedRejections || events[0]["peer"] != "bufconn" {
		t.Errorf("rejection events %v, want one naming the peer bufconn", events)
	}
}

func TestTapCountsEveryOutcome(t *testing.T) {
	h := startByToken(t, returning(ingest.AcceptResult{}, nil))
	outcomes := []struct {
		args  sent
		times int
	}{
		{
			args:  carrying(),
			times: 1,
		},
		{
			args:  carrying(otherSchemePrefix + otlptest.Token),
			times: 2,
		},
		{
			args:  carrying(bearerPrefix+otlptest.Token, bearerPrefix+otlptest.Token),
			times: 1,
		},
		{
			args:  bearing(brokenTokenID),
			times: 4,
		},
		{
			args:  bearing(staleTokenID),
			times: 1,
		},
		{
			args:  bearing(unknownTokenID),
			times: 1,
		},
		{
			args:  carrying(bearerPrefix + otlptest.Token),
			times: 1,
		},
	}
	for _, outcome := range outcomes {
		for range outcome.times {
			outcome.args.export(t, h)
		}
	}
	want := otlp.TapStats{
		Missing: 1,
		Header:  3,
		Failed:  4,
	}
	if diff := cmp.Diff(want, h.Tap.Stats()); diff != "" {
		t.Errorf("tap stats (-want +got):\n%s", diff)
	}
}

func TestAnAdmittedTokenReachesTheAcceptor(t *testing.T) {
	acceptor := &countingAcceptor{}
	h := startGuarded(t, acceptor.accept, otlptest.Admitting())
	carrying(bearerPrefix+otlptest.Token).export(t, h)
	carrying(bearerPrefix+tokenWith(unknownTokenID)).export(t, h)
	if got := acceptor.calls.Load(); got != 1 {
		t.Errorf("acceptor called %d times, want once for the admitted token", got)
	}
}

func TestAnUnclassifiedRejectionIsUnauthenticatedAndCountedAsFailed(t *testing.T) {
	h := startByToken(t, returning(ingest.AcceptResult{}, nil))
	st := bearing(bareTokenID).export(t, h)
	type outcome struct {
		Status   statusWant
		Stats    otlp.TapStats
		Failures int
		Warnings int
	}
	want := outcome{
		Status: statusWant{
			Code:    codes.Unauthenticated,
			Message: rejectedMessage,
		},
		Stats: otlp.TapStats{
			Failed: 1,
		},
		Failures: 1,
	}
	got := outcome{
		Status: statusWant{
			Code:       st.Code(),
			Message:    st.Message(),
			RetryDelay: retryDelay(st),
		},
		Stats:    h.Tap.Stats(),
		Failures: len(h.Logs.Events(t, eventAuthFailed)),
		Warnings: len(h.Logs.Events(t, eventAuthRejected)),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
