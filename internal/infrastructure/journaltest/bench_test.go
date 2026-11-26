package journaltest_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/journaltest"
)

const benchSpans = 512

func BenchmarkAppendReceiveRelease512(b *testing.B) {
	spans := spansOf(b, 0, benchSpans)
	journal, err := journaltest.NewMemory(journaltest.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
		IdleAfter:  quietJournal,
	}, wallClock{})
	if err != nil {
		b.Fatalf("journaltest.NewMemory: %v", err)
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
