package record_test

import (
	"fmt"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

// Record sizes: 1 span, 64 (a 512-span collector batch split over P = 8), 512, 2048 (one batch, P = 1);
// the E0 load shape — 500 services × 200 operations.
var benchSizes = []int{1, 64, 512, 2048}

func BenchmarkEncode(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("spans=%d", size), func(b *testing.B) {
			spans := syntheticSpans(size, 500, 200, 5)
			encoder := new(record.Encoder)
			recordBytes := 0
			b.ReportAllocs()
			for b.Loop() {
				err := encoder.Encode(testTenant, 0, spans, writtenAt, 0, func(data []byte, _ string) error {
					recordBytes = len(data)
					return nil
				})
				if err != nil {
					b.Fatalf("Encode: %v", err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/span")
			b.ReportMetric(float64(recordBytes)/float64(size), "B/span")
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("spans=%d", size), func(b *testing.B) {
			data, _ := encoded(b, syntheticSpans(size, 500, 200, 5))
			header, err := record.ReadHeader(data, testPartition, noWeightLimit)
			if err != nil {
				b.Fatalf("ReadHeader: %v", err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := record.Decode(data, header); err != nil {
					b.Fatalf("Decode: %v", err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/span")
		})
	}
}

func BenchmarkReadHeader(b *testing.B) {
	data, _ := encoded(b, syntheticSpans(512, 500, 200, 5))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := record.ReadHeader(data, testPartition, noWeightLimit); err != nil {
			b.Fatalf("ReadHeader: %v", err)
		}
	}
}
