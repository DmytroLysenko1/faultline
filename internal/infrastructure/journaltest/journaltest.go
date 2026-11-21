package journaltest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const (
	shortWait      = 50 * time.Millisecond
	settleWait     = 5 * time.Second
	smallLimit     = 2
	batchesWritten = 5
	spansPerBatch  = 3
	traceStride    = 100
	maxReceives    = 50
	serviceName    = "checkout"
	Credit         = 64 << 20
	creditBatches  = 2
)

type Feed interface {
	Receive(ctx context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error)
}

type Journal struct {
	Feed     Feed
	Append   func(t *testing.T, spans []span.Span)
	Close    func(t *testing.T)
	LimitErr error
}

type Open func(t *testing.T, credit int64) *Journal

func RunFeedContract(t *testing.T, open Open) {
	t.Helper()
	t.Run("an empty feed waits maxWait and hands out nothing", func(t *testing.T) { emptyWaits(t, open(t, Credit)) })
	t.Run("no wait answers at once", func(t *testing.T) { noWait(t, open(t, Credit)) })
	t.Run("batches come in append order, limit at a time", func(t *testing.T) { inOrder(t, open(t, Credit)) })
	t.Run("a batch appended after a timeout comes next", func(t *testing.T) { afterTimeout(t, open(t, Credit)) })
	t.Run("a canceled receive is the caller's cancel", func(t *testing.T) { canceled(t, open(t, Credit)) })
	t.Run("a limit below one is refused", func(t *testing.T) { belowOne(t, open(t, Credit)) })
	t.Run("released credit lets the next batch through", func(t *testing.T) { creditBack(t, open) })
	t.Run("close hands out an in-order prefix, then closed for good", func(t *testing.T) { closing(t, open(t, Credit)) })
}

func emptyWaits(t *testing.T, journal *Journal) {
	started := time.Now()
	got, err := journal.Feed.Receive(t.Context(), smallLimit, shortWait)
	if err != nil || len(got) != 0 {
		t.Fatalf("Receive = %d deliveries, %v; want none and no error", len(got), err)
	}
	if waited := time.Since(started); waited < shortWait {
		t.Errorf("returned after %s, before maxWait %s", waited, shortWait)
	}
}

func noWait(t *testing.T, journal *Journal) {
	got, err := journal.Feed.Receive(t.Context(), smallLimit, 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("Receive = %d deliveries, %v; want none and no error", len(got), err)
	}
}

func inOrder(t *testing.T, journal *Journal) {
	written := appendBatches(t, journal, 0, batchesWritten)
	got := receiveBatches(t, journal.Feed, len(written))
	checkInOrder(t, got, written)
}

func afterTimeout(t *testing.T, journal *Journal) {
	if got, err := journal.Feed.Receive(t.Context(), smallLimit, shortWait); err != nil || len(got) != 0 {
		t.Fatalf("Receive before the append = %d deliveries, %v; want none", len(got), err)
	}
	written := appendBatches(t, journal, 0, 1)
	checkInOrder(t, receiveBatches(t, journal.Feed, 1), written)
}

func canceled(t *testing.T, journal *Journal) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := journal.Feed.Receive(ctx, smallLimit, shortWait); !errors.Is(err, context.Canceled) {
		t.Errorf("Receive with a canceled context = %v, want context.Canceled", err)
	}
}

func belowOne(t *testing.T, journal *Journal) {
	if _, err := journal.Feed.Receive(t.Context(), 0, 0); !errors.Is(err, journal.LimitErr) {
		t.Errorf("Receive with limit 0 = %v, want %v", err, journal.LimitErr)
	}
}

func creditBack(t *testing.T, open Open) {
	weight := footprintOf(t, batchOf(t, 0))
	journal := open(t, creditBatches*weight)
	written := appendBatches(t, journal, 0, creditBatches)
	held := receiveHeld(t, journal.Feed, creditBatches)
	checkInOrder(t, held, written)
	if total := bytesOf(held); total > creditBatches*weight {
		t.Fatalf("%d bytes held, credit %d", total, creditBatches*weight)
	}
	for _, delivery := range held {
		delivery.Release()
	}
	next := appendBatches(t, journal, creditBatches, 1)
	checkInOrder(t, receiveBatches(t, journal.Feed, 1), next)
}

func footprintOf(t *testing.T, spans []span.Span) int64 {
	t.Helper()
	measured, err := batch.NewOwning(spans)
	if err != nil {
		t.Fatalf("NewOwning: %v", err)
	}
	return measured.Footprint()
}

func bytesOf(deliveries []processing.Delivery) int64 {
	var total int64
	for _, delivery := range deliveries {
		total += delivery.Bytes()
	}
	return total
}

func closing(t *testing.T, journal *Journal) {
	written := appendBatches(t, journal, 0, batchesWritten)
	journal.Close(t)
	got, err := receiveUntilClosed(t, journal.Feed)
	if !errors.Is(err, processing.ErrFeedClosed) {
		t.Fatalf("Receive after Close = %v, want ErrFeedClosed", err)
	}
	checkInOrder(t, got, written[:len(got)])
	if _, err := journal.Feed.Receive(t.Context(), smallLimit, shortWait); !errors.Is(err, processing.ErrFeedClosed) {
		t.Errorf("Receive once closed = %v, want ErrFeedClosed again", err)
	}
}

func appendBatches(t *testing.T, journal *Journal, first, count uint64) [][]span.Span {
	t.Helper()
	written := make([][]span.Span, 0, count)
	for index := range count {
		spans := batchOf(t, (first+index)*traceStride)
		journal.Append(t, slices.Clone(spans))
		written = append(written, spans)
	}
	return written
}

func batchOf(t *testing.T, firstTrace uint64) []span.Span {
	t.Helper()
	spans := make([]span.Span, 0, spansPerBatch)
	for offset := range uint64(spansPerBatch) {
		spans = append(spans, telemetrytest.NumberedSpan(t, kind.Client, firstTrace+offset, serviceName))
	}
	return spans
}

func receiveBatches(t *testing.T, feed Feed, count int) []processing.Delivery {
	t.Helper()
	got := receiveHeld(t, feed, count)
	for _, delivery := range got {
		delivery.Release()
	}
	return got
}

func receiveHeld(t *testing.T, feed Feed, count int) []processing.Delivery {
	t.Helper()
	var got []processing.Delivery
	for range maxReceives {
		deliveries, err := feed.Receive(t.Context(), smallLimit, settleWait)
		if err != nil {
			t.Fatalf("Receive after %d of %d batches: %v", len(got), count, err)
		}
		got = append(got, limited(t, deliveries)...)
		if len(got) >= count {
			return got
		}
	}
	t.Fatalf("%d of %d batches after %d receives", len(got), count, maxReceives)
	return nil
}

func receiveUntilClosed(t *testing.T, feed Feed) ([]processing.Delivery, error) {
	t.Helper()
	var got []processing.Delivery
	for range maxReceives {
		deliveries, err := feed.Receive(t.Context(), smallLimit, shortWait)
		if err != nil {
			return got, err
		}
		for _, delivery := range deliveries {
			delivery.Release()
		}
		got = append(got, limited(t, deliveries)...)
	}
	t.Fatalf("still open after %d receives", maxReceives)
	return nil, nil
}

func limited(t *testing.T, deliveries []processing.Delivery) []processing.Delivery {
	t.Helper()
	if len(deliveries) > smallLimit {
		t.Fatalf("Receive handed out %d deliveries, limit %d", len(deliveries), smallLimit)
	}
	return slices.DeleteFunc(deliveries, func(delivery processing.Delivery) bool {
		return delivery.Batch().Heartbeat
	})
}

func checkInOrder(t *testing.T, got []processing.Delivery, written [][]span.Span) {
	t.Helper()
	if len(got) != len(written) {
		t.Fatalf("%d batches, want %d", len(got), len(written))
	}
	for index, delivery := range got {
		checkDelivery(t, index, delivery, written[index])
		if index > 0 {
			checkAfter(t, index, got[index-1], delivery)
		}
	}
}

func checkDelivery(t *testing.T, index int, delivery processing.Delivery, written []span.Span) {
	t.Helper()
	if !slices.Equal(delivery.Batch().Spans, written) {
		t.Errorf("batch %d: spans %v, want %v", index, delivery.Batch().Spans, written)
	}
	if delivery.Bytes() <= 0 {
		t.Errorf("batch %d: %d credit bytes, want a positive weight", index, delivery.Bytes())
	}
}

func checkAfter(t *testing.T, index int, previous, next processing.Delivery) {
	t.Helper()
	if next.Position().Sequence <= previous.Position().Sequence {
		t.Errorf("batch %d: position %d after %d", index, next.Position().Sequence, previous.Position().Sequence)
	}
	if next.Batch().ReceivedAt.Before(previous.Batch().ReceivedAt) {
		t.Errorf("batch %d: ReceivedAt %v before %v", index, next.Batch().ReceivedAt, previous.Batch().ReceivedAt)
	}
}
