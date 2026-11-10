package batch_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

type stampView struct {
	ReceivedAt time.Time
	Spans      int
	Candidate  time.Time
	Known      bool
}

func viewOf(received batch.Batch) stampView {
	candidate, known := received.Candidate()
	return stampView{
		ReceivedAt: received.ReceivedAt(),
		Spans:      len(received.Spans()),
		Candidate:  candidate,
		Known:      known,
	}
}

func TestStampedSetsTheReceiveTimeAndLeavesTheOriginalUnstamped(t *testing.T) {
	spans := []span.Span{spanEndingAt(t, 1, 50*time.Second), spanEndingAt(t, 2, 80*time.Second)}
	unstamped, err := batch.NewOwning(spans)
	if err != nil {
		t.Fatalf("NewOwning: %v", err)
	}

	stamped := unstamped.Stamped(time.Unix(60, 0))

	want := stampView{ReceivedAt: time.Unix(60, 0), Spans: 2, Candidate: time.Unix(60, 0), Known: true}
	if diff := cmp.Diff(want, viewOf(stamped)); diff != "" {
		t.Errorf("stamped batch mismatch (-want +got):\n%s", diff)
	}
	if !unstamped.ReceivedAt().IsZero() {
		t.Errorf("Stamped changed the original: ReceivedAt() = %v, want the zero time", unstamped.ReceivedAt())
	}
}

func TestCandidateUsesTheLatestEndKnownAtConstruction(t *testing.T) {
	spans := []span.Span{spanEndingAt(t, 1, 50*time.Second), spanEndingAt(t, 2, 80*time.Second)}
	unstamped, err := batch.NewOwning(spans)
	if err != nil {
		t.Fatalf("NewOwning: %v", err)
	}

	later := unstamped.Stamped(time.Unix(100, 0))
	earlier := unstamped.Stamped(time.Unix(70, 0))

	want := [2]time.Time{time.Unix(80, 0), time.Unix(70, 0)}
	laterCandidate, _ := later.Candidate()
	earlierCandidate, _ := earlier.Candidate()
	if diff := cmp.Diff(want, [2]time.Time{laterCandidate, earlierCandidate}); diff != "" {
		t.Errorf("[stamped at 100 s, stamped at 70 s] candidate mismatch (-want +got):\n%s", diff)
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
