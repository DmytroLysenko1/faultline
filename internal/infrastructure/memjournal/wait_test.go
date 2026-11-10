package memjournal_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	longWait       = 10 * time.Second
	arrivalDelay   = 2 * time.Second
	writtenBatches = 2_000
)

type waited struct {
	Views   []view
	Elapsed time.Duration
}

func TestReceiveWaitsAtMostMaxWait(t *testing.T) {
	tests := []struct {
		Name string
		Args time.Duration
		Want waited
	}{
		{Name: "empty journal returns nothing after the wait", Args: longWait, Want: waited{Elapsed: longWait}},
		{Name: "zero wait polls", Args: noWait},
		{Name: "negative wait polls", Args: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				journal := roomyJournal(t)
				start := time.Now()

				views := receiveViews(t, journal, tt.Args)

				if diff := cmp.Diff(tt.Want, waited{Views: views, Elapsed: time.Since(start)}); diff != "" {
					t.Errorf("receive (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestReceiveReturnsABatchAsSoonAsItArrives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		start := time.Now()
		arriving := spansOf(t, 0, smallBatch)
		var writer sync.WaitGroup
		writer.Go(func() {
			time.Sleep(arrivalDelay)
			mustAppend(t, journal, arriving)
		})

		views := receiveViews(t, journal, longWait)
		writer.Wait()

		want := waited{Views: []view{dataView(t, start.Add(arrivalDelay), arriving)}, Elapsed: arrivalDelay}
		if diff := cmp.Diff(want, waited{Views: views, Elapsed: time.Since(start)}); diff != "" {
			t.Errorf("receive (-want +got):\n%s", diff)
		}
	})
}

func TestReceiveWithACanceledContextFailsEvenWhenBatchesWait(t *testing.T) {
	journal := roomyJournal(t)
	queued := spansOf(t, 0, smallBatch)
	mustAppend(t, journal, queued)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := journal.Receive(ctx, receiveAll, noWait); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
	assertStats(t, journal, statsOf(t, queued))
}

func TestDeliveryBytesIsTheFootprintTheJournalCharged(t *testing.T) {
	journal := roomyJournal(t)
	small, large := spansOf(t, 0, smallBatch), spansOf(t, smallBatch, largeBatch)
	mustAppend(t, journal, small, large)
	deliveries := mustReceive(t, journal, receiveAll)
	defer releaseAll(deliveries)

	got := make([]int64, 0, len(deliveries))
	for _, delivery := range deliveries {
		got = append(got, delivery.Bytes())
	}
	if diff := cmp.Diff([]int64{footprintOf(t, small), footprintOf(t, large)}, got); diff != "" {
		t.Errorf("Bytes() per delivery (-want +got):\n%s", diff)
	}
}

type steppedClock struct {
	readings []time.Time
}

func (c *steppedClock) Now() time.Time {
	next := c.readings[0]
	if len(c.readings) > 1 {
		c.readings = c.readings[1:]
	}
	return next
}

func TestReceivedAtNeverStepsBackWithTheClock(t *testing.T) {
	at := func(seconds int64) time.Time { return time.Unix(seconds, 0) }
	journal, err := memjournal.New(memjournal.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
		IdleAfter:  quietJournal,
	}, &steppedClock{readings: []time.Time{at(100), at(90), at(120)}})
	if err != nil {
		t.Fatalf("memjournal.New: %v", err)
	}
	for first := range 3 {
		if err := journal.Append(t.Context(), spansOf(t, first, smallBatch)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	deliveries := mustReceive(t, journal, receiveAll)
	defer releaseAll(deliveries)

	got := make([]time.Time, 0, len(deliveries))
	for _, delivery := range deliveries {
		got = append(got, delivery.Batch().ReceivedAt)
	}
	if diff := cmp.Diff([]time.Time{at(100), at(100), at(120)}, got); diff != "" {
		t.Errorf("ReceivedAt in journal order (-want +got):\n%s", diff)
	}
}

func TestReceivedAtFollowsJournalOrderUnderConcurrentWriters(t *testing.T) {
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   roomyBytes * roomyBytes,
		MaxBatches: writtenBatches,
	})
	appended := spansOf(t, 0, smallBatch)
	var writers sync.WaitGroup
	for range concurrentWriters {
		writers.Go(func() {
			for range writtenBatches / concurrentWriters {
				mustAppend(t, journal, appended)
			}
		})
	}
	writers.Wait()
	journal.Close()

	var stamps []time.Time
	for {
		deliveries, err := journal.Receive(t.Context(), receiveAll, noWait)
		if errors.Is(err, processing.ErrFeedClosed) {
			break
		}
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		for _, delivery := range deliveries {
			stamps = append(stamps, delivery.Batch().ReceivedAt)
		}
		releaseAll(deliveries)
	}

	if len(stamps) != writtenBatches {
		t.Fatalf("received %d batches, want %d", len(stamps), writtenBatches)
	}
	if !slices.IsSortedFunc(stamps, time.Time.Compare) {
		t.Errorf("ReceivedAt is not sorted in delivery order: journal order is not ReceivedAt order")
	}
}
