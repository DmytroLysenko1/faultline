//go:build !race

package journaltest_test

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/journaltest"
)

const largestQueue = 1_000_000

func TestWorstCaseHeapCoversTheQueueAllocatedUpFront(t *testing.T) {
	limits := journaltest.Limits{
		MaxBytes:   1,
		MaxBatches: largestQueue,
		IdleAfter:  time.Minute,
	}
	grown := telemetrytest.HeapGrowth(func() any {
		journal, err := journaltest.NewMemory(limits, wallClock{})
		if err != nil {
			t.Fatalf("journaltest.NewMemory: %v", err)
		}
		return journal
	})
	if model := uint64(limits.WorstCaseHeapBytes()); grown > model {
		t.Errorf("an empty journal took %d bytes, model %d", grown, model)
	}
}
