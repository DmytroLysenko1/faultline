package natsjs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func apiError(code jetstream.ErrorCode, description string) error {
	return fmt.Errorf("nats: %w", &jetstream.APIError{Code: 503, ErrorCode: code, Description: description})
}

// TestEveryFailureHasOneReasonAndKeepsItsCause: classify never returns nil and names exactly one reason,
// which the journal turns into the answer of the ingest port.
func TestEveryFailureHasOneReasonAndKeepsItsCause(t *testing.T) {
	tests := []struct {
		name       string
		args       error
		wantReason Reason
	}{
		{name: "stream full", args: apiError(codeStoreFailed, storeFullBytes), wantReason: ReasonStreamFull},
		{name: "message count full", args: apiError(codeStoreFailed, storeFullMessages), wantReason: ReasonStreamFull},
		{name: "store too large", args: apiError(codeStoreFailed, storeTooLarge), wantReason: ReasonTooLarge},
		{name: "store closed", args: apiError(codeStoreFailed, "store is closed"), wantReason: ReasonStoreFailed},
		{name: "message over the stream limit", args: apiError(codeMessageTooLarge, "message size exceeds maximum allowed"),
			wantReason: ReasonTooLarge},
		{name: "duplicate in flight", args: apiError(codeDuplicateInProcess, "duplicate message id is in process"),
			wantReason: ReasonInProcess},
		{name: "insufficient resources", args: apiError(codeInsufficientResource, "insufficient resources"),
			wantReason: ReasonInsufficientResources},
		{name: "storage resources", args: apiError(codeStorageResources, "insufficient storage resources available"),
			wantReason: ReasonInsufficientResources},
		{name: "cluster unavailable", args: apiError(codeClusterUnavailable, "JetStream system temporarily unavailable"),
			wantReason: ReasonNoResponders},
		{name: "not the leader", args: apiError(codeNotLeader, "JetStream not in clustered mode or not leader"),
			wantReason: ReasonNoResponders},
		{name: "too many requests", args: apiError(codeTooManyRequests, "too many requests"),
			wantReason: ReasonNoResponders},
		{name: "no peers to place a stream", args: apiError(codeNoSuitablePeers, "no suitable peers for placement"),
			wantReason: ReasonInsufficientResources},
		{name: "unknown code", args: apiError(1, "?"), wantReason: ReasonUnexpected},
		{name: "account limits", args: apiError(codeAccountLimits, "resource limits exceeded for account"),
			wantReason: ReasonAccountLimits},
		{name: "no applicable limits", args: apiError(codeNoLimits, "no JetStream default or applicable tiered limit"),
			wantReason: ReasonAccountLimits},
		{name: "header too large", args: apiError(codeHeaderTooLarge, "header size exceeds maximum allowed of 64k"),
			wantReason: ReasonTooLarge},
		{name: "stream mismatch", args: apiError(codeStreamMismatch, "expected stream does not match"),
			wantReason: ReasonStreamMismatch},
		{name: "sealed", args: apiError(codeStreamSealed, "invalid operation on sealed stream"),
			wantReason: ReasonSealed},
		{name: "the caller cancelled", args: fmt.Errorf("wait: %w", context.Canceled), wantReason: ReasonCanceled},
		{name: "no responders after retries", args: jetstream.ErrNoStreamResponse, wantReason: ReasonNoResponders},
		{name: "invalid ack", args: jetstream.ErrInvalidJSAck, wantReason: ReasonUnexpected},
		{name: "reconnect buffer off", args: nats.ErrReconnectBufExceeded, wantReason: ReasonDisconnected},
		{name: "draining", args: nats.ErrConnectionDraining, wantReason: ReasonClosed},
		{name: "closed by its owner", args: nats.ErrConnectionClosed, wantReason: ReasonClosed},
		{name: "max payload", args: nats.ErrMaxPayload, wantReason: ReasonTooLarge},
		{name: "deadline", args: fmt.Errorf("wait: %w", context.DeadlineExceeded), wantReason: ReasonTimeout},
		{name: "anything else", args: errors.New("surprise"), wantReason: ReasonUnexpected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.args)

			if got.Reason != tt.wantReason {
				t.Errorf("reason = %s, want %s", got.Reason, tt.wantReason)
			}
			if !errors.Is(got, tt.args) {
				t.Errorf("err = %v, want it to keep its cause %v", got, tt.args)
			}
		})
	}
}

func TestEveryReasonHasAName(t *testing.T) {
	for reason := range reasonCount {
		if reason.String() == "" {
			t.Errorf("reason %d has no name", reason)
		}
	}
	if got := (reasonCount + 1).String(); got != ReasonUnexpected.String() {
		t.Errorf("an out-of-range reason is %q, want %q", got, ReasonUnexpected.String())
	}
}
