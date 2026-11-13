package memjournal_test

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const appendsEach = 200

type positioned struct {
	Partition  uint16
	Sequence   uint64
	Heartbeat  bool
	ReceivedAt time.Time
}

func positionsOf(deliveries []processing.Delivery) []positioned {
	var got []positioned
	for _, delivery := range deliveries {
		position := delivery.Position()
		got = append(got, positioned{
			Partition:  position.Partition,
			Sequence:   position.Sequence,
			Heartbeat:  delivery.Batch().Heartbeat,
			ReceivedAt: delivery.Batch().ReceivedAt,
		})
	}
	return got
}

// TestPositionsCountEveryRecordInJournalOrder: heartbeats are records too, so a checkpoint position
// names one place in the journal whatever mix of batches and heartbeats led to it.
func TestPositionsCountEveryRecordInJournalOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		start := time.Now()
		mustAppend(t, journal, spansOf(t, 0, smallBatch))
		first := mustReceive(t, journal, receiveAll)
		time.Sleep(idleAfter)
		pulse := mustReceive(t, journal, receiveAll)
		mustAppend(t, journal, spansOf(t, 1, smallBatch))
		last := mustReceive(t, journal, receiveAll)
		deliveries := slices.Concat(first, pulse, last)
		defer releaseAll(deliveries)

		want := []positioned{
			{Sequence: 1, ReceivedAt: start},
			{Sequence: 2, Heartbeat: true, ReceivedAt: start.Add(idleAfter)},
			{Sequence: 3, ReceivedAt: start.Add(idleAfter)},
		}
		if diff := cmp.Diff(want, positionsOf(deliveries)); diff != "" {
			t.Errorf("positions (-want +got):\n%s", diff)
		}
	})
}

// TestConcurrentAppendsGetConsecutivePositionsInReceivedAtOrder: writers race, yet positions are
// 1..n without gaps or repeats, and a later position never has an earlier ReceivedAt.
func TestConcurrentAppendsGetConsecutivePositionsInReceivedAtOrder(t *testing.T) {
	journal := newJournal(t, memjournal.Limits{MaxBytes: roomyBytes, MaxBatches: concurrentWriters * appendsEach})
	batches := make([][]span.Span, 0, concurrentWriters*appendsEach)
	for index := range concurrentWriters * appendsEach {
		batches = append(batches, spansOf(t, index, smallBatch))
	}
	var writers sync.WaitGroup
	for writer := range concurrentWriters {
		writers.Go(func() {
			for index := range appendsEach {
				if err := journal.Append(t.Context(), batches[writer*appendsEach+index]); err != nil {
					t.Errorf("writer %d, append %d: %v", writer, index, err)
					return
				}
			}
		})
	}
	writers.Wait()
	deliveries := mustReceive(t, journal, concurrentWriters*appendsEach)
	defer releaseAll(deliveries)

	got := positionsOf(deliveries)
	if len(got) != concurrentWriters*appendsEach {
		t.Fatalf("received %d positioned records, want %d", len(got), concurrentWriters*appendsEach)
	}
	for index, record := range got {
		if record.Sequence != uint64(index+1) {
			t.Fatalf("record %d has position %d, want %d", index, record.Sequence, index+1)
		}
		if index > 0 && record.ReceivedAt.Before(got[index-1].ReceivedAt) {
			t.Fatalf("position %d received at %s, before position %d at %s", record.Sequence, record.ReceivedAt,
				got[index-1].Sequence, got[index-1].ReceivedAt)
		}
	}
}
