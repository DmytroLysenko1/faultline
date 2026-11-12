package record_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

// repeatedNames builds count spans over a fixed set of 5 services and 3 operations, so a bigger record has
// more spans but not more distinct strings.
func repeatedNames(count int) []span.Span {
	shapes := syntheticSpans(15, 5, 3, 21)
	spans := make([]span.Span, count)
	for i := range spans {
		spans[i] = shapes[i%len(shapes)]
		spans[i].TraceID = traceOf(uint64(i + 1))
	}
	return spans
}

// TestDecodeAllocatesNothingPerSpan: allocations depend on the distinct strings of a record, not on how many
// spans it carries — the decode cost the processor pays on its single hot goroutine.
func TestDecodeAllocatesNothingPerSpan(t *testing.T) {
	allocations := func(count int) float64 {
		data, _ := encoded(t, repeatedNames(count))
		header, err := record.ReadHeader(data, testPartition, noWeightLimit)
		if err != nil {
			t.Fatalf("ReadHeader: %v", err)
		}
		return testing.AllocsPerRun(50, func() {
			if _, err := record.Decode(data, header); err != nil {
				t.Fatalf("Decode: %v", err)
			}
		})
	}

	small, large := allocations(64), allocations(4096)

	if large != small {
		t.Errorf("decode allocates %.1f times for 64 spans and %.1f for 4096; want no per-span allocation", small, large)
	}
}
