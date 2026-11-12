//go:build !race

package record_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

// TestEncodeAllocatesNothingPerSpan: a warm encoder reuses its pooled buffers; what remains per call (the
// footprint map, the message id) does not grow with the span count. Not under -race: sync.Pool drops items there.
func TestEncodeAllocatesNothingPerSpan(t *testing.T) {
	encoder := new(record.Encoder)
	allocations := func(count int) float64 {
		spans := repeatedNames(count)
		encode := func() {
			err := encoder.Encode(testTenant, testPartition, spans, writtenAt, 0, func([]byte, string) error { return nil })
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
		}
		encode()
		return testing.AllocsPerRun(50, encode)
	}

	small, large := allocations(64), allocations(4096)

	if large != small {
		t.Errorf("encode allocates %.1f times for 64 spans and %.1f for 4096; want no per-span allocation", small, large)
	}
}
