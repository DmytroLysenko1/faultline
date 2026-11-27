package otlp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const unlimitedRate = 1_000_000_000

func TestAZeroVerdictDoesNotPanic(t *testing.T) {
	settings := benchOptions()
	gate := otlptest.Gate(t, admission.Limits{
		Slots:             RequestSlots,
		MinShare:          MinTokenShare,
		ActiveWindow:      ActiveTokenWindow,
		WaitingWindow:     WaitingTokenWindow,
		RequestsPerSecond: unlimitedRate,
	})
	export := guardedExport{
		gate:      NewTap(otlptest.Admitting(), settings),
		admission: gate,
		events:    newEvents(settings),
		holdLimit: settings.SlotHoldLimit,
	}
	decoded := 0
	_, err := export.handle(nil, t.Context(), func(any) error {
		decoded++
		return nil
	}, func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		t.Errorf("a stream without a verdict reached the interceptor")
		return nil, nil
	})
	type outcome struct {
		Code     codes.Code
		Decoded  int
		Admitted uint64
	}
	want := outcome{
		Code: codes.Internal,
	}
	got := outcome{
		Code:     status.Code(err),
		Decoded:  decoded,
		Admitted: gate.Admission.Stats().Admitted,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("a stream without a verdict (-want +got):\n%s", diff)
	}
}

type countingExporter struct {
	exported int
}

func (c *countingExporter) Export(context.Context, ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	c.exported++
	return ptraceotlp.NewExportResponse(), nil
}

func passThrough(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(ctx, request)
}

func TestABodyDecodedAfterTheHoldLimitIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settings := benchOptions()
		gate := otlptest.Gate(t, AdmissionLimits(unlimitedRate))
		tap := NewTap(otlptest.Admitting(), settings)
		receiver := &countingExporter{}
		export := guardedExport{
			gate:      tap,
			admission: gate,
			receiver:  receiver,
			events:    newEvents(settings),
			holdLimit: settings.SlotHoldLimit,
		}
		ctx := tap.decide(t.Context(), metadata.Pairs(authorizationHeader, "Bearer "+otlptest.Token))
		_, err := export.handle(nil, ctx, func(any) error {
			time.Sleep(settings.SlotHoldLimit + time.Second)
			return nil
		}, passThrough)
		type outcome struct {
			Code     codes.Code
			Exported int
			TooSlow  uint64
			InUse    int
		}
		want := outcome{
			Code:    codes.Canceled,
			TooSlow: 1,
		}
		stats := gate.Admission.Stats()
		got := outcome{
			Code:     status.Code(err),
			Exported: receiver.exported,
			TooSlow:  stats.TooSlow,
			InUse:    stats.InUse,
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("a body decoded without error 1 s after the hold limit (-want +got):\n%s", diff)
		}
	})
}

type steppedClock struct {
	now time.Time
}

func (c *steppedClock) Now() time.Time {
	return c.now
}

type tooSlowRecord struct {
	Level      string  `json:"level"`
	Message    string  `json:"msg"`
	Suppressed float64 `json:"suppressed_requests"`
}

func tooSlowRecords(t *testing.T, logs *bytes.Buffer) []tooSlowRecord {
	t.Helper()
	var records []tooSlowRecord
	for line := range bytes.Lines(logs.Bytes()) {
		var record tooSlowRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		records = append(records, record)
	}
	return records
}

func TestSlowBodiesWriteOneWarnPerInterval(t *testing.T) {
	logs := &bytes.Buffer{}
	clock := &steppedClock{
		now: time.Unix(0, 0),
	}
	shared := newEvents(DefaultOptions(slog.New(slog.NewJSONHandler(logs, nil)), clock))
	shared.bodyTooSlow(t.Context(), grantedTokenID, admission.ErrSlotHeldTooLong)
	clock.now = clock.now.Add(500 * time.Millisecond)
	shared.bodyTooSlow(t.Context(), grantedTokenID, admission.ErrSlotHeldTooLong)
	withinASecond := tooSlowRecords(t, logs)
	clock.now = clock.now.Add(LogInterval)
	shared.bodyTooSlow(t.Context(), grantedTokenID, admission.ErrSlotHeldTooLong)
	type outcome struct {
		WithinASecond []tooSlowRecord
		AfterInterval []tooSlowRecord
	}
	first := tooSlowRecord{
		Level:   "WARN",
		Message: "otlp.export.too_slow",
	}
	want := outcome{
		WithinASecond: []tooSlowRecord{first},
		AfterInterval: []tooSlowRecord{
			first,
			{
				Level:      "WARN",
				Message:    "otlp.export.too_slow",
				Suppressed: 1,
			},
		},
	}
	got := outcome{
		WithinASecond: withinASecond,
		AfterInterval: tooSlowRecords(t, logs),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("two slow bodies 500 ms apart, then one after the log interval (-want +got):\n%s", diff)
	}
}

func TestARefusalForAnUnknownReasonIsInternal(t *testing.T) {
	logs := &bytes.Buffer{}
	clock := &steppedClock{
		now: time.Unix(0, 0),
	}
	shared := newEvents(DefaultOptions(slog.New(slog.NewJSONHandler(logs, nil)), clock))
	refusal := admission.RejectionError{
		Reason: admission.ReasonCount,
	}
	failure := admissionFailureOf(refusal)
	failure.report(shared, t.Context(), grantedTokenID, refusal)
	type outcome struct {
		Code    codes.Code
		Records []logLine
	}
	want := outcome{
		Code: codes.Internal,
		Records: []logLine{
			{
				Level:   "ERROR",
				Message: "otlp.export.failed",
			},
		},
	}
	got := outcome{
		Code:    failure.status.Code(),
		Records: logLines(t, logs),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("a refusal past the last reason (-want +got):\n%s", diff)
	}
}

type logLine struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
}

func logLines(t *testing.T, logs *bytes.Buffer) []logLine {
	t.Helper()
	var lines []logLine
	decoder := json.NewDecoder(logs)
	for decoder.More() {
		var line logLine
		if err := decoder.Decode(&line); err != nil {
			t.Fatalf("decode a log line: %v", err)
		}
		lines = append(lines, line)
	}
	return lines
}

func TestARefusalWithoutItsDetailsIsLoggedAsAnInternalFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	clock := &steppedClock{
		now: time.Unix(0, 0),
	}
	shared := newEvents(DefaultOptions(slog.New(slog.NewJSONHandler(logs, nil)), clock))
	shared.admissionRejected(t.Context(), grantedTokenID, admission.ErrShareExceeded)
	want := []logLine{
		{
			Level:   "ERROR",
			Message: "otlp.export.failed",
		},
	}
	if diff := cmp.Diff(want, logLines(t, logs)); diff != "" {
		t.Errorf("a share sentinel without its rejection details (-want +got):\n%s", diff)
	}
}
