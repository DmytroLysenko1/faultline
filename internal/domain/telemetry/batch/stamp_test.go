package batch_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

func TestNewOwningLeavesTheBatchUnstamped(t *testing.T) {
	unstamped, err := batch.NewOwning([]span.Span{spanEndingAt(t, 1, 50*time.Second)})
	if err != nil {
		t.Fatalf("NewOwning: %v", err)
	}
	if !unstamped.ReceivedAt.IsZero() {
		t.Errorf("ReceivedAt = %v, want the zero time until the journal stamps it", unstamped.ReceivedAt)
	}
}

func TestCandidateUsesTheLatestEndKnownAtConstruction(t *testing.T) {
	spans := []span.Span{spanEndingAt(t, 1, 50*time.Second), spanEndingAt(t, 2, 80*time.Second)}
	unstamped, err := batch.NewOwning(spans)
	if err != nil {
		t.Fatalf("NewOwning: %v", err)
	}
	later, earlier := unstamped, unstamped
	later.ReceivedAt, earlier.ReceivedAt = time.Unix(100, 0), time.Unix(70, 0)

	laterCandidate, _ := later.Candidate()
	earlierCandidate, _ := earlier.Candidate()

	want := [2]time.Time{time.Unix(80, 0), time.Unix(70, 0)}
	if diff := cmp.Diff(want, [2]time.Time{laterCandidate, earlierCandidate}); diff != "" {
		t.Errorf("[received at 100 s, received at 70 s] candidate mismatch (-want +got):\n%s", diff)
	}
}

func TestCandidateDoesNotAllocate(t *testing.T) {
	const runs = 100
	received := mustBatch(t, time.Unix(100, 0), spanEndingAt(t, 1, 50*time.Second), spanEndingAt(t, 2, 80*time.Second))
	allocations := testing.AllocsPerRun(runs, func() {
		received.Candidate()
	})
	if allocations != 0 {
		t.Errorf("Candidate allocates %.0f times, want 0: the journal calls it under its lock", allocations)
	}
}
