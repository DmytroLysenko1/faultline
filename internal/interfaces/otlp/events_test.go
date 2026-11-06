package otlp_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	validTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	validTraceparent = "00-" + validTraceID + "-00f067aa0ba902b7-01"
	rejectedSpans    = 3
	overloadRounds   = 6
)

func withTraceparent(ctx context.Context, header string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "traceparent", header)
}

func rejecting(count int) acceptorFunc {
	return returning(ingest.AcceptResult{
		Rejected: count,
		Reasons: []string{
			"invalid span: trace_id: zero",
		},
	}, nil)
}

func exportOK(t *testing.T, h harness, ctx context.Context) {
	t.Helper()
	if _, err := h.Client.Export(ctx, requestOf(1)); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

func TestRejectedSpansAreLoggedWithTheCallerTraceID(t *testing.T) {
	h := start(t, rejecting(rejectedSpans))
	exportOK(t, h, withTraceparent(t.Context(), validTraceparent))
	want := []map[string]any{
		{
			"level":               "WARN",
			"msg":                 "otlp.export.rejected",
			"rejected_spans":      float64(rejectedSpans),
			"reason":              "invalid span: trace_id: zero",
			"suppressed_requests": float64(0),
			"suppressed_spans":    float64(0),
			"trace_id":            validTraceID,
		},
	}
	got := h.Logs.Events(t, "otlp.export.rejected")
	if diff := cmp.Diff(want, withoutTime(got)); diff != "" {
		t.Errorf("rejected events (-want +got):\n%s", diff)
	}
}

func withoutTime(records []map[string]any) []map[string]any {
	for _, record := range records {
		delete(record, "time")
	}
	return records
}

func TestRejectedLogIsThrottledAndCarriesWhatItSkipped(t *testing.T) {
	h := start(t, rejecting(rejectedSpans))
	exportOK(t, h, t.Context())
	exportOK(t, h, t.Context())
	exportOK(t, h, t.Context())
	if got := len(h.Logs.Events(t, "otlp.export.rejected")); got != 1 {
		t.Fatalf("%d rejected events inside one interval, want 1", got)
	}
	h.Clock.Advance(otlp.LogInterval)
	exportOK(t, h, t.Context())
	events := h.Logs.Events(t, "otlp.export.rejected")
	last := events[len(events)-1]
	if last["suppressed_requests"] != float64(2) || last["suppressed_spans"] != float64(2*rejectedSpans) {
		t.Errorf("record after the interval = %v, want 2 suppressed requests with %d spans", last, 2*rejectedSpans)
	}
}

func TestInvalidTraceparentAddsNoTraceID(t *testing.T) {
	for _, header := range []string{
		"01-" + validTraceID + "-00f067aa0ba902b7-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-" + validTraceID + "-0000000000000000-01",
		"00-" + validTraceID + "-00f067aa0ba902b7-1",
		"00-" + validTraceID + "-00f067aa0ba902b7-01-extra",
		"00-" + validTraceID + "-00f067aa0ba902b7-0g",
	} {
		t.Run(fmt.Sprintf("%q", header), func(t *testing.T) {
			h := start(t, rejecting(1))
			exportOK(t, h, withTraceparent(t.Context(), header))
			for _, record := range h.Logs.Events(t, "otlp.export.rejected") {
				if _, found := record["trace_id"]; found {
					t.Errorf("invalid traceparent produced trace_id %v", record["trace_id"])
				}
			}
		})
	}
}

func eventCounts(t *testing.T, h harness) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, record := range h.Logs.Records(t) {
		counts[fmt.Sprint(record["level"], " ", record["msg"])]++
	}
	return counts
}

func TestFailuresLogOnlyWhatAnOperatorMustSee(t *testing.T) {
	tests := []struct {
		Name string
		Args error
		Want map[string]int
	}{
		{
			Name: "overload is throttled to one warning per interval",
			Args: ingest.ErrOverloaded,
			Want: map[string]int{
				"WARN otlp.export.overloaded": 1,
			},
		},
		{
			Name: "too large is an error every time",
			Args: ingest.ErrBatchTooLarge,
			Want: map[string]int{
				"ERROR otlp.export.too_large": 2,
			},
		},
		{
			Name: "internal failure is an error every time",
			Args: errJournalBroken,
			Want: map[string]int{
				"ERROR otlp.export.failed": 2,
			},
		},
		{
			Name: "shutdown is a debug note, not an incident",
			Args: ingest.ErrUnavailable,
			Want: map[string]int{
				"DEBUG otlp.export.interrupted": 2,
			},
		},
		{
			Name: "client cancel is a debug note, not an incident",
			Args: context.Canceled,
			Want: map[string]int{
				"DEBUG otlp.export.interrupted": 2,
			},
		},
		{
			Name: "deadline is a debug note, not an incident",
			Args: context.DeadlineExceeded,
			Want: map[string]int{
				"DEBUG otlp.export.interrupted": 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			h := start(t, returning(ingest.AcceptResult{}, tt.Args))
			for range 2 {
				if _, err := h.Client.Export(t.Context(), requestOf(1)); err == nil {
					t.Fatalf("Export succeeded, want the acceptor error")
				}
			}
			if diff := cmp.Diff(tt.Want, eventCounts(t, h)); diff != "" {
				t.Errorf("logged events (-want +got):\n%s", diff)
			}
		})
	}
}

func exportAllowingOverload(t *testing.T, h harness) {
	t.Helper()
	if _, err := h.Client.Export(t.Context(), requestOf(1)); err != nil && status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Export: %v, want success or RESOURCE_EXHAUSTED", err)
	}
}

func TestRejectedAndOverloadedAreThrottledIndependently(t *testing.T) {
	var calls atomic.Int64
	h := start(t, func(context.Context, ingest.SpanSource) (ingest.AcceptResult, error) {
		if calls.Add(1)%2 == 1 {
			return ingest.AcceptResult{
				Rejected: 1,
				Reasons: []string{
					"invalid span: trace_id: zero",
				},
			}, nil
		}
		return ingest.AcceptResult{}, ingest.ErrOverloaded
	})
	for range overloadRounds {
		exportAllowingOverload(t, h)
	}
	h.Clock.Advance(otlp.LogInterval)
	exportAllowingOverload(t, h)
	exportAllowingOverload(t, h)
	overloads := h.Logs.Events(t, "otlp.export.overloaded")
	if len(h.Logs.Events(t, "otlp.export.rejected")) != 2 || len(overloads) != 2 {
		t.Fatalf("rejected %d, overloaded %d events, want 2 of each: one throttle must not mute the other",
			len(h.Logs.Events(t, "otlp.export.rejected")), len(overloads))
	}
	if got := overloads[1]["suppressed_requests"]; got != float64(overloadRounds/2-1) {
		t.Errorf("second overloaded record suppressed_requests = %v, want %d", got, overloadRounds/2-1)
	}
}
