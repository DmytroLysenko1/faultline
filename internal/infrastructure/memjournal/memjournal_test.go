package memjournal_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	roomyBytes   = 1 << 20
	roomyBatches = 64
	receiveAll   = 64
	smallBatch   = 1
	largeBatch   = 3

	concurrentWriters = 8
	maxAppendAttempts = 1_000_000
	releaseRounds     = 1000
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newJournal(t *testing.T, limits memjournal.Limits) *memjournal.Journal {
	t.Helper()
	journal, err := memjournal.New(limits)
	if err != nil {
		t.Fatalf("memjournal.New(%+v): %v", limits, err)
	}
	return journal
}

func roomyJournal(t *testing.T) *memjournal.Journal {
	t.Helper()
	return newJournal(t, memjournal.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
	})
}

func batchOf(t *testing.T, first, count int) batch.Batch {
	t.Helper()
	spans := make([]span.Span, 0, count)
	for offset := range count {
		spans = append(spans, telemetrytest.NumberedSpan(t, kind.Client, uint64(first+offset), "checkout"))
	}
	created, err := batch.New(spans, telemetrytest.Epoch())
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	return created
}

func mustAppend(t *testing.T, journal *memjournal.Journal, appended ...batch.Batch) {
	t.Helper()
	for _, next := range appended {
		if err := journal.Append(t.Context(), next); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func mustReceive(t *testing.T, journal *memjournal.Journal, limit int) []processing.Delivery {
	t.Helper()
	deliveries, err := journal.Receive(t.Context(), limit)
	if err != nil {
		t.Fatalf("Receive(%d): %v", limit, err)
	}
	return deliveries
}

func batchesOf(deliveries []processing.Delivery) []batch.Batch {
	received := make([]batch.Batch, 0, len(deliveries))
	for _, delivery := range deliveries {
		received = append(received, delivery.Batch())
	}
	return received
}

func releaseAll(deliveries []processing.Delivery) {
	for _, delivery := range deliveries {
		delivery.Release()
	}
}

func statsOf(batches ...batch.Batch) memjournal.Stats {
	var stats memjournal.Stats
	for _, counted := range batches {
		stats.Bytes += counted.Footprint()
		stats.Batches++
		stats.Spans += len(counted.Spans)
	}
	return stats
}

func assertStats(t *testing.T, journal *memjournal.Journal, want memjournal.Stats) {
	t.Helper()
	if diff := cmp.Diff(want, journal.Stats()); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

func TestNewRejectsNonPositiveLimits(t *testing.T) {
	tests := []struct {
		Name    string
		Args    memjournal.Limits
		WantErr error
	}{
		{
			Name: "zero bytes",
			Args: memjournal.Limits{
				MaxBatches: 1,
			},
			WantErr: memjournal.ErrInvalidLimits,
		},
		{
			Name: "negative bytes",
			Args: memjournal.Limits{
				MaxBytes:   -1,
				MaxBatches: 1,
			},
			WantErr: memjournal.ErrInvalidLimits,
		},
		{
			Name: "negative batches",
			Args: memjournal.Limits{
				MaxBytes:   1,
				MaxBatches: -1,
			},
			WantErr: memjournal.ErrInvalidLimits,
		},
		{
			Name: "zero batches",
			Args: memjournal.Limits{
				MaxBytes: 1,
			},
			WantErr: memjournal.ErrInvalidLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if _, err := memjournal.New(tt.Args); !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestReceiveReturnsBatchesInAppendOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		appended := []batch.Batch{
			batchOf(t, 0, smallBatch),
			batchOf(t, 1, largeBatch),
			batchOf(t, smallBatch+largeBatch, smallBatch),
		}
		mustAppend(t, journal, appended...)
		deliveries := mustReceive(t, journal, receiveAll)
		defer releaseAll(deliveries)
		if diff := cmp.Diff(appended, batchesOf(deliveries)); diff != "" {
			t.Errorf("received batches (-want +got):\n%s", diff)
		}
	})
}

func TestReceiveTakesAtMostMaxAndLeavesTheRest(t *testing.T) {
	const firstTake = 2
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		appended := []batch.Batch{
			batchOf(t, 0, smallBatch),
			batchOf(t, 1, smallBatch),
			batchOf(t, 2, smallBatch),
		}
		mustAppend(t, journal, appended...)
		first := mustReceive(t, journal, firstTake)
		second := mustReceive(t, journal, receiveAll)
		releaseAll(append(first, second...))
		if diff := cmp.Diff(appended[:firstTake], batchesOf(first)); diff != "" {
			t.Errorf("first receive (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(appended[firstTake:], batchesOf(second)); diff != "" {
			t.Errorf("second receive (-want +got):\n%s", diff)
		}
	})
}

func TestReceiveRejectsNonPositiveMax(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		queued := []batch.Batch{
			batchOf(t, 0, smallBatch),
			batchOf(t, 1, smallBatch),
		}
		mustAppend(t, journal, queued...)
		for _, limit := range []int{
			0,
			-1,
		} {
			if _, err := journal.Receive(t.Context(), limit); !errors.Is(err, memjournal.ErrInvalidMax) {
				t.Errorf("Receive(%d) err = %v, want %v", limit, err, memjournal.ErrInvalidMax)
			}
		}
		assertStats(t, journal, statsOf(queued...))
	})
}

func TestAppendBatchAtExactCapacityIsAccepted(t *testing.T) {
	exact := batchOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   exact.Footprint(),
		MaxBatches: 1,
	})
	mustAppend(t, journal, exact)
	assertStats(t, journal, statsOf(exact))
}

func TestAppendBatchLargerThanFullCapacityIsTooLarge(t *testing.T) {
	oversized := batchOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   oversized.Footprint() - 1,
		MaxBatches: roomyBatches,
	})
	if err := journal.Append(t.Context(), oversized); !errors.Is(err, ingest.ErrBatchTooLarge) {
		t.Fatalf("err = %v, want %v", err, ingest.ErrBatchTooLarge)
	}
	assertStats(t, journal, memjournal.Stats{})
}

func TestByteCreditReturnsOnlyAfterRelease(t *testing.T) {
	first := batchOf(t, 0, largeBatch)
	second := batchOf(t, largeBatch, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   first.Footprint() + second.Footprint() - 1,
		MaxBatches: roomyBatches,
	})
	mustAppend(t, journal, first)
	if err := journal.Append(t.Context(), second); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("append without credit: err = %v, want %v", err, ingest.ErrOverloaded)
	}
	held := mustReceive(t, journal, receiveAll)
	if err := journal.Append(t.Context(), second); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("append while the first batch is unreleased: err = %v, want %v", err, ingest.ErrOverloaded)
	}
	assertStats(t, journal, statsOf(first))
	releaseAll(held)
	mustAppend(t, journal, second)
	assertStats(t, journal, statsOf(second))
}

func TestFullQueueRejectsAndReturnsTheByteCredit(t *testing.T) {
	queued := batchOf(t, 0, smallBatch)
	rejected := batchOf(t, 1, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   queued.Footprint() + rejected.Footprint(),
		MaxBatches: 1,
	})
	mustAppend(t, journal, queued)
	if err := journal.Append(t.Context(), rejected); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("append into a full queue: err = %v, want %v", err, ingest.ErrOverloaded)
	}
	assertStats(t, journal, statsOf(queued))
	held := mustReceive(t, journal, receiveAll)
	defer releaseAll(held)
	if err := journal.Append(t.Context(), rejected); err != nil {
		t.Fatalf("append after the queue drained: %v, the rejected append kept its byte credit", err)
	}
	assertStats(t, journal, statsOf(queued, rejected))
}

func TestRepeatedReleaseDoesNotReturnCreditTwice(t *testing.T) {
	sized := batchOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   sized.Footprint(),
		MaxBatches: roomyBatches,
	})
	mustAppend(t, journal, sized)
	deliveries := mustReceive(t, journal, receiveAll)
	releaseAll(deliveries)
	releaseAll(deliveries)
	assertStats(t, journal, memjournal.Stats{})
	mustAppend(t, journal, sized)
	if err := journal.Append(t.Context(), sized); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("second release doubled the credit: err = %v, want %v", err, ingest.ErrOverloaded)
	}
}

func TestStatsCountUnreleasedBatchesUntilRelease(t *testing.T) {
	small := batchOf(t, 0, smallBatch)
	large := batchOf(t, smallBatch, largeBatch)
	journal := roomyJournal(t)
	mustAppend(t, journal, small, large)
	assertStats(t, journal, statsOf(small, large))
	deliveries := mustReceive(t, journal, receiveAll)
	assertStats(t, journal, statsOf(small, large))
	deliveries[0].Release()
	assertStats(t, journal, statsOf(large))
	deliveries[1].Release()
	assertStats(t, journal, memjournal.Stats{})
}

func TestAppendWithCanceledContextKeepsTheByteCredit(t *testing.T) {
	appended := batchOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   appended.Footprint(),
		MaxBatches: roomyBatches,
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := journal.Append(ctx, appended); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
	assertStats(t, journal, memjournal.Stats{})
	if err := journal.Append(t.Context(), appended); err != nil {
		t.Fatalf("append after a canceled one: %v, the canceled append kept its byte credit", err)
	}
}

type appendCondition struct {
	Oversized bool
	Closed    bool
	Canceled  bool
	FullQueue bool
	NoCredit  bool
}

func limitsUnder(appended batch.Batch, condition appendCondition) memjournal.Limits {
	switch {
	case condition.Oversized:
		return memjournal.Limits{
			MaxBytes:   appended.Footprint() - 1,
			MaxBatches: roomyBatches,
		}
	case condition.NoCredit:
		return memjournal.Limits{
			MaxBytes:   appended.Footprint(),
			MaxBatches: roomyBatches,
		}
	default:
		return memjournal.Limits{
			MaxBytes:   appended.Footprint() * roomyBatches,
			MaxBatches: 1,
		}
	}
}

func journalUnder(t *testing.T, appended batch.Batch, condition appendCondition) *memjournal.Journal {
	t.Helper()
	journal := newJournal(t, limitsUnder(appended, condition))
	if condition.FullQueue || condition.NoCredit {
		mustAppend(t, journal, batchOf(t, largeBatch, largeBatch))
	}
	if condition.Closed {
		journal.Close()
	}
	return journal
}

func contextUnder(t *testing.T, condition appendCondition) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	if condition.Canceled {
		cancel()
		return ctx
	}
	t.Cleanup(cancel)
	return ctx
}

func TestAppendReportsTheMostFinalFailureFirst(t *testing.T) {
	tests := []struct {
		Name    string
		Args    appendCondition
		WantErr error
	}{
		{
			Name: "oversized batch on a closed journal is too large",
			Args: appendCondition{
				Oversized: true,
				Closed:    true,
			},
			WantErr: ingest.ErrBatchTooLarge,
		},
		{
			Name: "oversized batch with a canceled context is too large",
			Args: appendCondition{
				Oversized: true,
				Canceled:  true,
			},
			WantErr: ingest.ErrBatchTooLarge,
		},
		{
			Name: "closed journal with a canceled context is unavailable",
			Args: appendCondition{
				Closed:   true,
				Canceled: true,
			},
			WantErr: ingest.ErrUnavailable,
		},
		{
			Name: "canceled context on a full queue is canceled",
			Args: appendCondition{
				Canceled:  true,
				FullQueue: true,
			},
			WantErr: context.Canceled,
		},
		{
			Name: "canceled context without byte credit is canceled",
			Args: appendCondition{
				Canceled: true,
				NoCredit: true,
			},
			WantErr: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			appended := batchOf(t, 0, largeBatch)
			journal := journalUnder(t, appended, tt.Args)
			if err := journal.Append(contextUnder(t, tt.Args), appended); !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestClosedJournalRejectsAppendsAndDrainsTheRest(t *testing.T) {
	journal := roomyJournal(t)
	queued := batchOf(t, 0, smallBatch)
	mustAppend(t, journal, queued)
	journal.Close()
	journal.Close()
	if err := journal.Append(t.Context(), batchOf(t, 1, smallBatch)); !errors.Is(err, ingest.ErrUnavailable) {
		t.Fatalf("append after close: err = %v, want %v", err, ingest.ErrUnavailable)
	}
	rest := mustReceive(t, journal, receiveAll)
	releaseAll(rest)
	if diff := cmp.Diff([]batch.Batch{
		queued,
	}, batchesOf(rest)); diff != "" {
		t.Errorf("drained batches (-want +got):\n%s", diff)
	}
	if _, err := journal.Receive(t.Context(), receiveAll); !errors.Is(err, processing.ErrFeedClosed) {
		t.Fatalf("receive from closed empty journal: err = %v, want %v", err, processing.ErrFeedClosed)
	}
	assertStats(t, journal, memjournal.Stats{})
}

type receiveResult struct {
	Deliveries []processing.Delivery
	Err        error
}

func receiveAsync(ctx context.Context, journal *memjournal.Journal) <-chan receiveResult {
	results := make(chan receiveResult, 1)
	go func() {
		deliveries, err := journal.Receive(ctx, receiveAll)
		results <- receiveResult{
			Deliveries: deliveries,
			Err:        err,
		}
	}()
	return results
}

func TestReceiveWaitsForTheFirstBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		results := receiveAsync(t.Context(), journal)
		synctest.Wait()
		if len(results) != 0 {
			t.Fatalf("Receive returned before any batch was appended")
		}
		appended := batchOf(t, 0, smallBatch)
		mustAppend(t, journal, appended)
		got := <-results
		if got.Err != nil {
			t.Fatalf("Receive: %v", got.Err)
		}
		releaseAll(got.Deliveries)
		if diff := cmp.Diff([]batch.Batch{
			appended,
		}, batchesOf(got.Deliveries)); diff != "" {
			t.Errorf("received batches (-want +got):\n%s", diff)
		}
	})
}

func TestBlockedReceiveEnds(t *testing.T) {
	tests := []struct {
		Name    string
		Args    func(journal *memjournal.Journal, cancel context.CancelFunc)
		WantErr error
	}{
		{
			Name: "on context cancel",
			Args: func(_ *memjournal.Journal, cancel context.CancelFunc) {
				cancel()
			},
			WantErr: context.Canceled,
		},
		{
			Name: "on close with ErrFeedClosed",
			Args: func(journal *memjournal.Journal, _ context.CancelFunc) {
				journal.Close()
			},
			WantErr: processing.ErrFeedClosed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				journal := roomyJournal(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				results := receiveAsync(ctx, journal)
				synctest.Wait()
				tt.Args(journal, cancel)
				if got := <-results; !errors.Is(got.Err, tt.WantErr) {
					t.Fatalf("err = %v, want %v", got.Err, tt.WantErr)
				}
			})
		})
	}
}

type writerTally struct {
	Accepted   atomic.Int64
	SawClosure atomic.Int64
}

func appendUntilClosed(t *testing.T, journal *memjournal.Journal, appended batch.Batch, tally *writerTally,
	started func(),
) {
	t.Helper()
	for range maxAppendAttempts {
		err := journal.Append(t.Context(), appended)
		switch {
		case err == nil:
			tally.Accepted.Add(1)
			started()
		case errors.Is(err, ingest.ErrOverloaded):
		case errors.Is(err, ingest.ErrUnavailable):
			tally.SawClosure.Add(1)
			return
		default:
			t.Errorf("Append: unexpected error %v", err)
			return
		}
	}
}

func startWriters(t *testing.T, journal *memjournal.Journal, tally *writerTally, started func()) <-chan struct{} {
	t.Helper()
	appended := batchOf(t, 0, smallBatch)
	var writers sync.WaitGroup
	for range concurrentWriters {
		writers.Go(func() {
			appendUntilClosed(t, journal, appended, tally, started)
		})
	}
	done := make(chan struct{})
	go func() {
		writers.Wait()
		close(done)
	}()
	return done
}

func drainUntilClosed(t *testing.T, journal *memjournal.Journal) int64 {
	t.Helper()
	var received int64
	for {
		deliveries, err := journal.Receive(t.Context(), receiveAll)
		if errors.Is(err, processing.ErrFeedClosed) {
			return received
		}
		if err != nil {
			t.Errorf("Receive: %v", err)
			return received
		}
		received += int64(len(deliveries))
		releaseAll(deliveries)
	}
}

func TestCloseDuringAppendsLosesNoAcceptedBatch(t *testing.T) {
	journal := roomyJournal(t)
	var tally writerTally
	var received atomic.Int64
	var consumer sync.WaitGroup
	consumer.Go(func() {
		received.Store(drainUntilClosed(t, journal))
	})
	firstAccepted := make(chan struct{})
	writersDone := startWriters(t, journal, &tally, sync.OnceFunc(func() {
		close(firstAccepted)
	}))
	select {
	case <-firstAccepted:
	case <-writersDone:
	}
	journal.Close()
	<-writersDone
	consumer.Wait()
	if got := tally.SawClosure.Load(); got != concurrentWriters {
		t.Fatalf("%d of %d writers were still appending when Close landed, the race was not exercised", got,
			concurrentWriters)
	}
	if got, want := received.Load(), tally.Accepted.Load(); got != want {
		t.Errorf("received %d batches, want every accepted one: %d", got, want)
	}
	assertStats(t, journal, memjournal.Stats{})
}

func TestReleaseConcurrentWithAppendKeepsStatsExact(t *testing.T) {
	journal := roomyJournal(t)
	appended := batchOf(t, 0, smallBatch)
	var releasers sync.WaitGroup
	for range releaseRounds {
		mustAppend(t, journal, appended)
		held := mustReceive(t, journal, receiveAll)
		releasers.Go(func() {
			releaseAll(held)
		})
		mustAppend(t, journal, appended)
		releaseAll(mustReceive(t, journal, receiveAll))
	}
	releasers.Wait()
	assertStats(t, journal, memjournal.Stats{})
}
