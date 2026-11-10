//go:build !race

package memjournal_test

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const largestQueue = 1_000_000

func TestWorstCaseHeapCoversTheQueueAllocatedUpFront(t *testing.T) {
	limits := memjournal.Limits{
		MaxBytes:   1,
		MaxBatches: largestQueue,
		IdleAfter:  time.Minute,
	}
	grown := telemetrytest.HeapGrowth(func() any {
		journal, err := memjournal.New(limits, wallClock{})
		if err != nil {
			t.Fatalf("memjournal.New: %v", err)
		}
		return journal
	})
	if model := uint64(limits.WorstCaseHeapBytes()); grown > model {
		t.Errorf("an empty journal took %d bytes, model %d", grown, model)
	}
}
