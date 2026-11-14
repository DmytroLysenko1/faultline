package otlp_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	journalSecret = "SECRET-JOURNAL-DETAIL"
	reasonBytes   = 400
	longReason    = 2 << 10
)

var errJournalBroken = errors.New(journalSecret)

type statusWant struct {
	Code       codes.Code
	Message    string
	RetryDelay time.Duration
}

func retryDelay(st *status.Status) time.Duration {
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.RetryInfo); ok {
			return info.GetRetryDelay().AsDuration()
		}
	}
	return 0
}

func TestExportMapsEveryAcceptorErrorToItsStatus(t *testing.T) {
	tests := []struct {
		Name string
		Args error
		Want statusWant
	}{
		{
			Name: "overloaded journal asks the client to retry after a second",
			Args: fmt.Errorf("accept batch: %w", ingest.ErrOverloaded),
			Want: statusWant{
				Code:       codes.ResourceExhausted,
				Message:    "span journal overloaded, retry later",
				RetryDelay: otlp.RetryDelay,
			},
		},
		{
			Name: "batch larger than the journal is permanent",
			Args: fmt.Errorf("accept batch: %w", ingest.ErrBatchTooLarge),
			Want: statusWant{
				Code:    codes.InvalidArgument,
				Message: "request exceeds span journal capacity",
			},
		},
		{
			Name: "a journal fault is unavailable",
			Args: ingest.ErrUnavailable,
			Want: statusWant{
				Code:    codes.Unavailable,
				Message: "span journal unavailable",
			},
		},
		{
			Name: "closed journal is unavailable",
			Args: errors.Join(ingest.ErrUnavailable, ingest.ErrNotAttempted),
			Want: statusWant{
				Code:    codes.Unavailable,
				Message: "span journal unavailable",
			},
		},
		{
			Name: "canceled request",
			Args: fmt.Errorf("append: %w", context.Canceled),
			Want: statusWant{
				Code:    codes.Canceled,
				Message: "append: context canceled",
			},
		},
		{
			Name: "deadline",
			Args: context.DeadlineExceeded,
			Want: statusWant{
				Code:    codes.DeadlineExceeded,
				Message: "context deadline exceeded",
			},
		},
		{
			Name: "anything else is internal and hides the cause",
			Args: fmt.Errorf("accept batch: %w", errJournalBroken),
			Want: statusWant{
				Code:    codes.Internal,
				Message: "internal error",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			h := start(t, returning(ingest.AcceptResult{}, tt.Args))
			_, err := h.Client.Export(t.Context(), requestOf(1))
			st := status.Convert(err)
			got := statusWant{
				Code:       st.Code(),
				Message:    st.Message(),
				RetryDelay: retryDelay(st),
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("status (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExportWithoutRejectionsHasNoPartialSuccess(t *testing.T) {
	h := start(t, returning(ingest.AcceptResult{
		Accepted: 1,
	}, nil))
	response, err := h.Client.Export(t.Context(), requestOf(1))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := response.PartialSuccess(); got.RejectedSpans() != 0 || got.ErrorMessage() != "" {
		t.Errorf("partial success = {%d, %q}, want empty", got.RejectedSpans(), got.ErrorMessage())
	}
}

func repeatedReason(letter string) string {
	return strings.Repeat(letter, reasonBytes)
}

func TestPartialSuccessCarriesWholeReasonsUpToTheLimit(t *testing.T) {
	reasons := []string{
		repeatedReason("a"),
		repeatedReason("b"),
		repeatedReason("c"),
	}
	h := start(t, returning(ingest.AcceptResult{
		Rejected: len(reasons),
		Reasons:  reasons,
	}, nil))
	response, err := h.Client.Export(t.Context(), requestOf(1))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	partial := response.PartialSuccess()
	if diff := cmp.Diff(int64(len(reasons)), partial.RejectedSpans()); diff != "" {
		t.Errorf("rejected spans (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(reasons[0]+"; "+reasons[1], partial.ErrorMessage()); diff != "" {
		t.Errorf("error message keeps whole reasons only (-want +got):\n%s", diff)
	}
}

func TestOverlongFirstReasonIsCutAtARuneBoundary(t *testing.T) {
	reason := "x" + strings.Repeat("é", longReason)
	h := start(t, returning(ingest.AcceptResult{
		Rejected: 1,
		Reasons: []string{
			reason,
		},
	}, nil))
	response, err := h.Client.Export(t.Context(), requestOf(1))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	message := response.PartialSuccess().ErrorMessage()
	if len(message) > otlp.MaxErrorMessageBytes || !utf8.ValidString(message) || !strings.HasPrefix(reason, message) {
		t.Errorf("message of %d bytes (valid UTF-8 %v) is not a rune-aligned prefix within %d bytes",
			len(message), utf8.ValidString(message), otlp.MaxErrorMessageBytes)
	}
	if len(message) < otlp.MaxErrorMessageBytes-utf8.UTFMax {
		t.Errorf("message of %d bytes wastes the %d-byte limit", len(message), otlp.MaxErrorMessageBytes)
	}
}
