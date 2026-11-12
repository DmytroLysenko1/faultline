package record_test

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

// maxRequestBytes is the OTLP gRPC receive limit (otlp.MaxRecvMsgBytes); the record bound is derived from it.
const maxRequestBytes = 4 << 20

// minWireBytesPerSpan: an accepted span costs at least this on the wire — ids with their tags and lengths (design step 3).
const minWireBytesPerSpan = 30

func TestARecordOverTheGuardIsTooLarge(t *testing.T) {
	distinctLongNames := make([]span.Span, record.MaxRecordBytes/operation.MaxNameBytes+1)
	for i := range distinctLongNames {
		distinctLongNames[i] = minimalSpan(uint64(i))
		distinctLongNames[i].Operation = operation.Operation{
			Protocol: operation.ProtocolHTTP,
			Name:     fmt.Sprintf("%0*d", operation.MaxNameBytes, i),
		}
	}
	manySpans := make([]span.Span, record.MaxRecordBytes/minWireBytesPerSpan+1)
	for i := range manySpans {
		manySpans[i] = minimalSpan(uint64(i))
	}
	tests := []struct {
		name  string
		spans []span.Span
	}{
		{name: "the string table pushes it over", spans: distinctLongNames},
		{name: "just past the guard by one span", spans: manySpans[:record.MaxRecordBytes/minWireBytesPerSpan-1]},
		{name: "the spans alone push it over", spans: manySpans},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			published := false

			err := new(record.Encoder).Encode(testTenant, 0, tt.spans, writtenAt, 0, func([]byte, string) error {
				published = true
				return nil
			})

			if !errors.Is(err, record.ErrTooLarge) || published {
				t.Errorf("err = %v, published = %t; want ErrTooLarge and nothing published", err, published)
			}
		})
	}
}

// TestWorstRecordStaysUnderTheGuard: the largest request the receiver accepts, all spans in one partition,
// still encodes under MaxRecordBytes — the guard only catches broken invariants, never valid traffic.
func TestWorstRecordStaysUnderTheGuard(t *testing.T) {
	allOptional := func(n uint64) span.Span {
		observed := fullSpan(n)
		observed.Service.Namespace, observed.Service.Name, observed.Operation.Name = "n", "s", "o"
		observed.Interval = eventtime.FromUnixNanos(endAt(-time.Millisecond), endAt(0))
		return observed
	}
	distinctNames := func(n uint64) span.Span {
		observed := minimalSpan(n)
		observed.Service.Name = fmt.Sprintf("s%d", n)
		return observed
	}
	tests := []struct {
		name      string
		wireBytes int
		build     func(uint64) span.Span
	}{
		{name: "minimal spans, one service", wireBytes: minWireBytesPerSpan, build: minimalSpan},
		{name: "every optional field", wireBytes: minWireBytesPerSpan + 10 + 9 + 9 + 4, build: allOptional},
		{name: "a distinct service name per span", wireBytes: minWireBytesPerSpan + 2 + 7, build: distinctNames},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spans := make([]span.Span, maxRequestBytes/tt.wireBytes)
			for i := range spans {
				spans[i] = tt.build(uint64(i))
			}
			size := 0

			err := new(record.Encoder).Encode(testTenant, 0, spans, writtenAt, 0, func(data []byte, _ string) error {
				size = len(data)
				return nil
			})

			if err != nil || size > record.MaxRecordBytes {
				t.Errorf("%d spans: err = %v, %d bytes; want a record under %d", len(spans), err, size, record.MaxRecordBytes)
			}
		})
	}
}

// TestAnOversizedBatchStopsEncodingAtTheGuard: a batch twice the guard is refused without first encoding all of
// it — otherwise one hostile request would make the ingester allocate its whole encoded size.
func TestAnOversizedBatchStopsEncodingAtTheGuard(t *testing.T) {
	spans := make([]span.Span, 2*record.MaxRecordBytes/minWireBytesPerSpan)
	for i := range spans {
		spans[i] = minimalSpan(uint64(i))
	}
	encoder := new(record.Encoder)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	err := encoder.Encode(testTenant, 0, spans, writtenAt, 0, func([]byte, string) error { return nil })

	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if !errors.Is(err, record.ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if allocated > 3*record.MaxRecordBytes {
		t.Errorf("refusing an oversized batch allocated %d MiB, want under %d MiB", allocated>>20, 3*record.MaxRecordBytes>>20)
	}
}
