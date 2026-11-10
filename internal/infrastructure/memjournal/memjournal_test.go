package memjournal_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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

	noWait       = time.Duration(0)
	waitForever  = time.Hour
	quietJournal = 24 * time.Hour

	concurrentWriters = 8
	maxAppendAttempts = 1_000_000
	releaseRounds     = 1000
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type wallClock struct{}

func (wallClock) Now() time.Time {
	return time.Now()
}

func newJournal(t *testing.T, limits memjournal.Limits) *memjournal.Journal {
	t.Helper()
	if limits.IdleAfter == 0 {
		limits.IdleAfter = quietJournal
	}
	journal, err := memjournal.New(limits, wallClock{})
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

func spansOf(t testing.TB, first, count int) []span.Span {
	t.Helper()
	spans := make([]span.Span, 0, count)
	for offset := range count {
		spans = append(spans, telemetrytest.NumberedSpan(t, kind.Client, uint64(first+offset), "checkout"))
	}
	return spans
}

func footprintOf(t *testing.T, spans []span.Span) int64 {
	t.Helper()
	measured, err := batch.New(spans, time.Time{})
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	return measured.Footprint()
}

func mustAppend(t *testing.T, journal *memjournal.Journal, appended ...[]span.Span) {
	t.Helper()
	for _, next := range appended {
		if err := journal.Append(t.Context(), slices.Clone(next)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func mustReceive(t *testing.T, journal *memjournal.Journal, limit int) []processing.Delivery {
	t.Helper()
	deliveries, err := journal.Receive(t.Context(), limit, noWait)
	if err != nil {
		t.Fatalf("Receive(%d): %v", limit, err)
	}
	return deliveries
}

func spansOfDeliveries(deliveries []processing.Delivery) [][]span.Span {
	received := make([][]span.Span, 0, len(deliveries))
	for _, delivery := range deliveries {
		received = append(received, delivery.Batch().Spans())
	}
	return received
}

func releaseAll(deliveries []processing.Delivery) {
	for _, delivery := range deliveries {
		delivery.Release()
	}
}

func statsOf(t *testing.T, batches ...[]span.Span) memjournal.Stats {
	t.Helper()
	var stats memjournal.Stats
	for _, counted := range batches {
		stats.Bytes += footprintOf(t, counted)
		stats.Batches++
		stats.Spans += len(counted)
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
	valid := memjournal.Limits{MaxBytes: 1, MaxBatches: 1, IdleAfter: time.Second}
	with := func(edit func(*memjournal.Limits)) memjournal.Limits {
		limits := valid
		edit(&limits)
		return limits
	}
	tests := []struct {
		Name    string
		Args    memjournal.Limits
		WantErr error
	}{
		{Name: "valid limits", Args: valid},
		{Name: "zero bytes", Args: with(func(l *memjournal.Limits) { l.MaxBytes = 0 }), WantErr: memjournal.ErrInvalidLimits},
		{Name: "negative bytes", Args: with(func(l *memjournal.Limits) { l.MaxBytes = -1 }),
			WantErr: memjournal.ErrInvalidLimits},
		{Name: "zero batches", Args: with(func(l *memjournal.Limits) { l.MaxBatches = 0 }),
			WantErr: memjournal.ErrInvalidLimits},
		{Name: "negative batches", Args: with(func(l *memjournal.Limits) { l.MaxBatches = -1 }),
			WantErr: memjournal.ErrInvalidLimits},
		{Name: "zero idle period", Args: with(func(l *memjournal.Limits) { l.IdleAfter = 0 }),
			WantErr: memjournal.ErrInvalidLimits},
		{Name: "negative idle period", Args: with(func(l *memjournal.Limits) { l.IdleAfter = -time.Second }),
			WantErr: memjournal.ErrInvalidLimits},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if _, err := memjournal.New(tt.Args, wallClock{}); !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestNewWithoutAClockPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("New(limits, nil) did not panic, want a wiring panic")
		}
	}()
	journal, err := memjournal.New(memjournal.Limits{MaxBytes: 1, MaxBatches: 1, IdleAfter: time.Second}, nil)
	t.Errorf("New(limits, nil) = %v, %v; want a wiring panic", journal, err)
}

func TestReceiveReturnsBatchesInAppendOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		appended := [][]span.Span{
			spansOf(t, 0, smallBatch),
			spansOf(t, 1, largeBatch),
			spansOf(t, smallBatch+largeBatch, smallBatch),
		}
		mustAppend(t, journal, appended...)
		deliveries := mustReceive(t, journal, receiveAll)
		defer releaseAll(deliveries)
		if diff := cmp.Diff(appended, spansOfDeliveries(deliveries)); diff != "" {
			t.Errorf("received batches (-want +got):\n%s", diff)
		}
	})
}

func TestReceiveTakesAtMostMaxAndLeavesTheRest(t *testing.T) {
	const firstTake = 2
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		appended := [][]span.Span{
			spansOf(t, 0, smallBatch),
			spansOf(t, 1, smallBatch),
			spansOf(t, 2, smallBatch),
		}
		mustAppend(t, journal, appended...)
		first := mustReceive(t, journal, firstTake)
		second := mustReceive(t, journal, receiveAll)
		releaseAll(append(first, second...))
		if diff := cmp.Diff(appended[:firstTake], spansOfDeliveries(first)); diff != "" {
			t.Errorf("first receive (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(appended[firstTake:], spansOfDeliveries(second)); diff != "" {
			t.Errorf("second receive (-want +got):\n%s", diff)
		}
	})
}

func TestReceiveRejectsNonPositiveMax(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := roomyJournal(t)
		queued := [][]span.Span{
			spansOf(t, 0, smallBatch),
			spansOf(t, 1, smallBatch),
		}
		mustAppend(t, journal, queued...)
		for _, limit := range []int{
			0,
			-1,
		} {
			if _, err := journal.Receive(t.Context(), limit, noWait); !errors.Is(err, memjournal.ErrInvalidMax) {
				t.Errorf("Receive(%d) err = %v, want %v", limit, err, memjournal.ErrInvalidMax)
			}
		}
		assertStats(t, journal, statsOf(t, queued...))
	})
}

func TestAppendBatchAtExactCapacityIsAccepted(t *testing.T) {
	exact := spansOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, exact),
		MaxBatches: 1,
	})
	mustAppend(t, journal, exact)
	assertStats(t, journal, statsOf(t, exact))
}

func TestAppendBatchLargerThanFullCapacityIsTooLarge(t *testing.T) {
	oversized := spansOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, oversized) - 1,
		MaxBatches: roomyBatches,
	})
	if err := journal.Append(t.Context(), slices.Clone(oversized)); !errors.Is(err, ingest.ErrBatchTooLarge) {
		t.Fatalf("err = %v, want %v", err, ingest.ErrBatchTooLarge)
	}
	assertStats(t, journal, memjournal.Stats{})
}

func TestByteCreditReturnsOnlyAfterRelease(t *testing.T) {
	first := spansOf(t, 0, largeBatch)
	second := spansOf(t, largeBatch, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, first) + footprintOf(t, second) - 1,
		MaxBatches: roomyBatches,
	})
	mustAppend(t, journal, first)
	if err := journal.Append(t.Context(), slices.Clone(second)); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("append without credit: err = %v, want %v", err, ingest.ErrOverloaded)
	}
	held := mustReceive(t, journal, receiveAll)
	if err := journal.Append(t.Context(), slices.Clone(second)); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("append while the first batch is unreleased: err = %v, want %v", err, ingest.ErrOverloaded)
	}
	assertStats(t, journal, statsOf(t, first))
	releaseAll(held)
	mustAppend(t, journal, second)
	assertStats(t, journal, statsOf(t, second))
}

func TestFullQueueRejectsAndReturnsTheByteCredit(t *testing.T) {
	queued := spansOf(t, 0, smallBatch)
	rejected := spansOf(t, 1, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, queued) + footprintOf(t, rejected),
		MaxBatches: 1,
	})
	mustAppend(t, journal, queued)
	if err := journal.Append(t.Context(), slices.Clone(rejected)); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("append into a full queue: err = %v, want %v", err, ingest.ErrOverloaded)
	}
	assertStats(t, journal, statsOf(t, queued))
	held := mustReceive(t, journal, receiveAll)
	defer releaseAll(held)
	if err := journal.Append(t.Context(), slices.Clone(rejected)); err != nil {
		t.Fatalf("append after the queue drained: %v, the rejected append kept its byte credit", err)
	}
	assertStats(t, journal, statsOf(t, queued, rejected))
}

func TestRepeatedReleaseDoesNotReturnCreditTwice(t *testing.T) {
	sized := spansOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, sized),
		MaxBatches: roomyBatches,
	})
	mustAppend(t, journal, sized)
	deliveries := mustReceive(t, journal, receiveAll)
	releaseAll(deliveries)
	releaseAll(deliveries)
	assertStats(t, journal, memjournal.Stats{})
	mustAppend(t, journal, sized)
	if err := journal.Append(t.Context(), slices.Clone(sized)); !errors.Is(err, ingest.ErrOverloaded) {
		t.Fatalf("second release doubled the credit: err = %v, want %v", err, ingest.ErrOverloaded)
	}
}

func TestStatsCountUnreleasedBatchesUntilRelease(t *testing.T) {
	small := spansOf(t, 0, smallBatch)
	large := spansOf(t, smallBatch, largeBatch)
	journal := roomyJournal(t)
	mustAppend(t, journal, small, large)
	assertStats(t, journal, statsOf(t, small, large))
	deliveries := mustReceive(t, journal, receiveAll)
	assertStats(t, journal, statsOf(t, small, large))
	deliveries[0].Release()
	assertStats(t, journal, statsOf(t, large))
	deliveries[1].Release()
	assertStats(t, journal, memjournal.Stats{})
}

func TestAppendWithCanceledContextKeepsTheByteCredit(t *testing.T) {
	appended := spansOf(t, 0, largeBatch)
	journal := newJournal(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, appended),
		MaxBatches: roomyBatches,
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := journal.Append(ctx, slices.Clone(appended)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
	assertStats(t, journal, memjournal.Stats{})
	if err := journal.Append(t.Context(), slices.Clone(appended)); err != nil {
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

func limitsUnder(t *testing.T, appended []span.Span, condition appendCondition) memjournal.Limits {
	switch {
	case condition.Oversized:
		return memjournal.Limits{
			MaxBytes:   footprintOf(t, appended) - 1,
			MaxBatches: roomyBatches,
		}
	case condition.NoCredit:
		return memjournal.Limits{
			MaxBytes:   footprintOf(t, appended),
			MaxBatches: roomyBatches,
		}
	default:
		return memjournal.Limits{
			MaxBytes:   footprintOf(t, appended) * roomyBatches,
			MaxBatches: 1,
		}
	}
}

func journalUnder(t *testing.T, appended []span.Span, condition appendCondition) *memjournal.Journal {
	t.Helper()
	journal := newJournal(t, limitsUnder(t, appended, condition))
	if condition.FullQueue || condition.NoCredit {
		mustAppend(t, journal, spansOf(t, largeBatch, largeBatch))
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
			appended := spansOf(t, 0, largeBatch)
			journal := journalUnder(t, appended, tt.Args)
			if err := journal.Append(contextUnder(t, tt.Args), slices.Clone(appended)); !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestClosedJournalRejectsAppendsAndDrainsTheRest(t *testing.T) {
	journal := roomyJournal(t)
	queued := spansOf(t, 0, smallBatch)
	mustAppend(t, journal, queued)
	journal.Close()
	journal.Close()
	if err := journal.Append(t.Context(), spansOf(t, 1, smallBatch)); !errors.Is(err, ingest.ErrUnavailable) {
		t.Fatalf("append after close: err = %v, want %v", err, ingest.ErrUnavailable)
	}
	rest := mustReceive(t, journal, receiveAll)
	releaseAll(rest)
	if diff := cmp.Diff([][]span.Span{
		queued,
	}, spansOfDeliveries(rest)); diff != "" {
		t.Errorf("drained batches (-want +got):\n%s", diff)
	}
	if _, err := journal.Receive(t.Context(), receiveAll, waitForever); !errors.Is(err, processing.ErrFeedClosed) {
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
		deliveries, err := journal.Receive(ctx, receiveAll, waitForever)
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
		appended := spansOf(t, 0, smallBatch)
		mustAppend(t, journal, appended)
		got := <-results
		if got.Err != nil {
			t.Fatalf("Receive: %v", got.Err)
		}
		releaseAll(got.Deliveries)
		if diff := cmp.Diff([][]span.Span{
			appended,
		}, spansOfDeliveries(got.Deliveries)); diff != "" {
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

func appendUntilClosed(t *testing.T, journal *memjournal.Journal, appended []span.Span, tally *writerTally,
	started func(),
) {
	t.Helper()
	for range maxAppendAttempts {
		err := journal.Append(t.Context(), slices.Clone(appended))
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
	appended := spansOf(t, 0, smallBatch)
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
		deliveries, err := journal.Receive(t.Context(), receiveAll, waitForever)
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
	appended := spansOf(t, 0, smallBatch)
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
