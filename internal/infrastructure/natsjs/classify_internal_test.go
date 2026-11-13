package natsjs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
)

var sentinels = []error{ingest.ErrOverloaded, ingest.ErrBatchTooLarge, ingest.ErrUnavailable}

func apiError(code jetstream.ErrorCode, description string) error {
	return fmt.Errorf("nats: %w", &jetstream.APIError{Code: 503, ErrorCode: code, Description: description})
}

// TestEveryFailureIsExactlyOneIngestSentinel: classify never returns nil and never two sentinels, so
// no failed publish can reach the client as OK and no answer is both retryable and final.
func TestEveryFailureIsExactlyOneIngestSentinel(t *testing.T) {
	tests := []struct {
		name       string
		args       error
		wantErr    error
		wantReason Reason
	}{
		{name: "stream full", args: apiError(codeStoreFailed, storeFullBytes), wantErr: ingest.ErrOverloaded,
			wantReason: ReasonStreamFull},
		{name: "message count full", args: apiError(codeStoreFailed, storeFullMessages), wantErr: ingest.ErrOverloaded,
			wantReason: ReasonStreamFull},
		{name: "store too large", args: apiError(codeStoreFailed, storeTooLarge), wantErr: ingest.ErrBatchTooLarge,
			wantReason: ReasonTooLarge},
		{name: "store closed", args: apiError(codeStoreFailed, "store is closed"), wantErr: ingest.ErrUnavailable,
			wantReason: ReasonStoreFailed},
		{name: "message over the stream limit", args: apiError(codeMessageTooLarge, "message size exceeds maximum allowed"),
			wantErr: ingest.ErrBatchTooLarge, wantReason: ReasonTooLarge},
		{name: "duplicate in flight", args: apiError(codeDuplicateInProcess, "duplicate message id is in process"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonInProcess},
		{name: "insufficient resources", args: apiError(codeInsufficientResource, "insufficient resources"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonInsufficientResources},
		{name: "storage resources", args: apiError(codeStorageResources, "insufficient storage resources available"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonInsufficientResources},
		{name: "cluster unavailable", args: apiError(codeClusterUnavailable, "JetStream system temporarily unavailable"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonNoResponders},
		{name: "unknown code", args: apiError(1, "?"), wantErr: ingest.ErrUnavailable, wantReason: ReasonUnexpected},
		{name: "account limits", args: apiError(codeAccountLimits, "resource limits exceeded for account"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonAccountLimits},
		{name: "no applicable limits", args: apiError(codeNoLimits, "no JetStream default or applicable tiered limit"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonAccountLimits},
		{name: "header too large", args: apiError(codeHeaderTooLarge, "header size exceeds maximum allowed of 64k"),
			wantErr: ingest.ErrBatchTooLarge, wantReason: ReasonTooLarge},
		{name: "stream mismatch", args: apiError(codeStreamMismatch, "expected stream does not match"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonStreamMismatch},
		{name: "sealed", args: apiError(codeStreamSealed, "invalid operation on sealed stream"),
			wantErr: ingest.ErrUnavailable, wantReason: ReasonSealed},
		{name: "the caller cancelled", args: fmt.Errorf("wait: %w", context.Canceled), wantErr: context.Canceled,
			wantReason: ReasonCanceled},
		{name: "no responders after retries", args: jetstream.ErrNoStreamResponse, wantErr: ingest.ErrUnavailable,
			wantReason: ReasonNoResponders},
		{name: "invalid ack", args: jetstream.ErrInvalidJSAck, wantErr: ingest.ErrUnavailable,
			wantReason: ReasonUnexpected},
		{name: "reconnect buffer off", args: nats.ErrReconnectBufExceeded, wantErr: ingest.ErrUnavailable,
			wantReason: ReasonDisconnected},
		{name: "draining", args: nats.ErrConnectionDraining, wantErr: ingest.ErrUnavailable,
			wantReason: ReasonClosed},
		{name: "closed by its owner", args: nats.ErrConnectionClosed, wantErr: ingest.ErrUnavailable,
			wantReason: ReasonClosed},
		{name: "max payload", args: nats.ErrMaxPayload, wantErr: ingest.ErrBatchTooLarge, wantReason: ReasonTooLarge},
		{name: "deadline", args: fmt.Errorf("wait: %w", context.DeadlineExceeded), wantErr: context.DeadlineExceeded,
			wantReason: ReasonTimeout},
		{name: "anything else", args: errors.New("surprise"), wantErr: ingest.ErrUnavailable,
			wantReason: ReasonUnexpected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.args)

			if got.Reason != tt.wantReason {
				t.Errorf("reason = %s, want %s", got.Reason, tt.wantReason)
			}
			if !errors.Is(got, tt.wantErr) || !errors.Is(got, tt.args) {
				t.Errorf("err = %v, want it to be %v and keep its cause", got, tt.wantErr)
			}
			if matched := countSentinels(got); matched != 1 {
				t.Errorf("err matches %d ingest sentinels, want exactly 1", matched)
			}
		})
	}
}

func countSentinels(err error) int {
	matched := 0
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			matched++
		}
	}
	return matched
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
