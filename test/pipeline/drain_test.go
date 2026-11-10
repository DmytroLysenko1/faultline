//go:build !race

package pipeline_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	journalMaxBytes      = 64 << 20
	journalMaxBatches    = 4096
	largestRequestSpans  = 123_360
	largestRequestsCount = 2
	drainBudget          = 2500 * time.Millisecond
	journalIdleAfter     = 35 * time.Second
)

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func singlePairBatches(t *testing.T) []batch.Batch {
	t.Helper()
	return processingtest.SinglePairsAfterPrefill(t, 0, journalMaxBatches)
}

func largestRequests(t *testing.T) []batch.Batch {
	t.Helper()
	batches := make([]batch.Batch, 0, largestRequestsCount)
	for request := range largestRequestsCount {
		first := uint64(request) * largestRequestSpans
		batches = append(batches, processingtest.CallerHalvesAfterPrefill(t, first, largestRequestSpans))
	}
	return batches
}

func fullJournal(t *testing.T, batches []batch.Batch) *memjournal.Journal {
	t.Helper()
	journal, err := memjournal.New(memjournal.Limits{
		MaxBytes:   journalMaxBytes,
		MaxBatches: journalMaxBatches,
		IdleAfter:  journalIdleAfter,
	}, systemClock{})
	if err != nil {
		t.Fatalf("memjournal.New: %v", err)
	}
	for index, appended := range batches {
		if err := journal.Append(t.Context(), slices.Clone(appended.Spans)); err != nil {
			t.Fatalf("Append batch %d: %v", index, err)
		}
	}
	if err := journal.Append(t.Context(), slices.Clone(batches[0].Spans)); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("Append into the filled journal = %v, want ErrOverloaded", err)
	}
	journal.Close()
	return journal
}

func TestDrainFullJournal(t *testing.T) {
	tests := []struct {
		Name string
		Args func(t *testing.T) []batch.Batch
	}{
		{
			Name: "full by batch count: 4096 single pairs",
			Args: singlePairBatches,
		},
		{
			Name: "full by bytes: two largest requests of caller halves",
			Args: largestRequests,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			builder := processingtest.PrefilledBuilder(t)
			store, err := memgraph.New(builder.Snapshot())
			if err != nil {
				t.Fatalf("memgraph.New: %v", err)
			}
			journal := fullJournal(t, tt.Args(t))
			started := time.Now()
			if err := processing.NewPipeline(journal, builder, store).Run(t.Context()); err != nil {
				t.Fatalf("Run: %v", err)
			}
			elapsed := time.Since(started)
			t.Logf("drained in %v (budget %v)", elapsed, drainBudget)
			if elapsed > drainBudget {
				t.Errorf("drain took %v, budget %v", elapsed, drainBudget)
			}
			if diff := cmp.Diff(memjournal.Stats{}, journal.Stats()); diff != "" {
				t.Errorf("journal after drain (-want +got):\n%s", diff)
			}
		})
	}
}
