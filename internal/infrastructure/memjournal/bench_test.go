package memjournal_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const benchSpans = 512

// The journal only reads the spans it owns, so one slice serves every iteration once its
// delivery is released.
func BenchmarkAppendReceiveRelease512(b *testing.B) {
	spans := spansOf(b, 0, benchSpans)
	journal, err := memjournal.New(memjournal.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
		IdleAfter:  quietJournal,
	}, wallClock{})
	if err != nil {
		b.Fatalf("memjournal.New: %v", err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := journal.Append(b.Context(), spans); err != nil {
			b.Fatalf("Append: %v", err)
		}
		deliveries, err := journal.Receive(b.Context(), receiveAll, noWait)
		if err != nil {
			b.Fatalf("Receive: %v", err)
		}
		releaseAll(deliveries)
	}
}
