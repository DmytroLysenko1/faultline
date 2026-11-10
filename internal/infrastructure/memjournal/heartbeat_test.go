package memjournal_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const (
	idleAfter     = 35 * time.Second
	catchUpAge    = 2 * time.Minute
	shortWait     = time.Second
	spanDuration  = time.Millisecond
	tightQueue    = 2
	roomForPulse  = 3
	appendSpacing = 500 * time.Millisecond
)

func idleJournal(t *testing.T, maxBatches int) *memjournal.Journal {
	t.Helper()
	return newJournal(t, memjournal.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: maxBatches,
		IdleAfter:  idleAfter,
	})
}

func endingAt(t *testing.T, id byte, end time.Time) []span.Span {
	t.Helper()
	return []span.Span{
		telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
			f.SpanID = ids.SpanID{id}
			f.Start = uint64(end.Add(-spanDuration).UnixNano())
			f.End = uint64(end.UnixNano())
		}))),
	}
}

type view struct {
	Heartbeat  bool
	ReceivedAt time.Time
	Spans      int
	Bytes      int64
}

func viewsOf(deliveries []processing.Delivery) []view {
	var views []view
	for _, delivery := range deliveries {
		views = append(views, view{
			Heartbeat:  delivery.Batch().Heartbeat,
			ReceivedAt: delivery.Batch().ReceivedAt,
			Spans:      len(delivery.Batch().Spans),
			Bytes:      delivery.Bytes(),
		})
	}
	return views
}

func dataView(t *testing.T, receivedAt time.Time, spans []span.Span) view {
	t.Helper()
	return view{ReceivedAt: receivedAt, Spans: len(spans), Bytes: footprintOf(t, spans)}
}

func pulseView(receivedAt time.Time) view {
	return view{Heartbeat: true, ReceivedAt: receivedAt}
}

func receiveViews(t *testing.T, journal *memjournal.Journal, maxWait time.Duration) []view {
	t.Helper()
	deliveries, err := journal.Receive(t.Context(), receiveAll, maxWait)
	if err != nil {
		t.Fatalf("Receive(maxWait %v): %v", maxWait, err)
	}
	defer releaseAll(deliveries)
	return viewsOf(deliveries)
}

func assertViews(t *testing.T, want, got []view) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("deliveries (-want +got):\n%s", diff)
	}
}

func TestReceiveDeliversOneHeartbeatOnceTheCandidateStalls(t *testing.T) {
	tests := []struct {
		Name      string
		Args      time.Duration
		WantPulse time.Duration
	}{
		{Name: "stalled before the call", Args: idleAfter, WantPulse: idleAfter},
		{Name: "stalls during the wait", Args: idleAfter - appendSpacing, WantPulse: idleAfter - appendSpacing + shortWait},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				journal := idleJournal(t, roomyBatches)
				start := time.Now()
				mustAppend(t, journal, endingAt(t, 1, start.Add(-time.Second)))
				releaseAll(mustReceive(t, journal, receiveAll))
				time.Sleep(tt.Args)

				got := receiveViews(t, journal, shortWait)

				assertViews(t, []view{pulseView(start.Add(tt.WantPulse))}, got)
				assertStats(t, journal, memjournal.Stats{})
			})
		})
	}
}

func TestNoHeartbeatBeforeTheFirstDataBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		start := time.Now()
		time.Sleep(2 * idleAfter)

		quiet := receiveViews(t, journal, shortWait)
		catchUp := endingAt(t, 1, start.Add(-catchUpAge))
		mustAppend(t, journal, catchUp)
		delivered := receiveViews(t, journal, noWait)

		assertViews(t, nil, quiet)
		assertViews(t, []view{dataView(t, start.Add(2*idleAfter+shortWait), catchUp)}, delivered)
	})
}

func TestAFirstBatchWithoutTimesStartsTheIdleClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		start := time.Now()
		mustAppend(t, journal, spansOf(t, 0, smallBatch))
		releaseAll(mustReceive(t, journal, receiveAll))
		time.Sleep(idleAfter)

		got := receiveViews(t, journal, noWait)

		assertViews(t, []view{pulseView(start.Add(idleAfter))}, got)
	})
}

func TestHeartbeatCountsAsARecordForTheNextOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		start := time.Now()
		mustAppend(t, journal, endingAt(t, 1, start))
		releaseAll(mustReceive(t, journal, receiveAll))
		time.Sleep(idleAfter)

		first := receiveViews(t, journal, noWait)
		tooSoon := receiveViews(t, journal, idleAfter-shortWait)
		second := receiveViews(t, journal, shortWait)

		assertViews(t, []view{pulseView(start.Add(idleAfter))}, first)
		assertViews(t, nil, tooSoon)
		assertViews(t, []view{pulseView(start.Add(2 * idleAfter))}, second)
	})
}

func TestAppendPrependsAHeartbeatOnlyToABatchThatDoesNotRaiseTheCandidate(t *testing.T) {
	tests := []struct {
		Name      string
		Args      time.Duration
		WantPulse bool
	}{
		{Name: "older events keep the candidate and get a heartbeat first", Args: -catchUpAge, WantPulse: true},
		{Name: "fresh events raise the candidate and get no heartbeat", Args: idleAfter - time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				journal := idleJournal(t, roomForPulse)
				start := time.Now()
				first := endingAt(t, 1, start.Add(-time.Second))
				next := endingAt(t, 2, start.Add(tt.Args))
				mustAppend(t, journal, first)
				time.Sleep(idleAfter)

				mustAppend(t, journal, next)

				want := []view{dataView(t, start, first)}
				if tt.WantPulse {
					want = append(want, pulseView(start.Add(idleAfter)))
				}
				want = append(want, dataView(t, start.Add(idleAfter), next))
				assertViews(t, want, receiveViews(t, journal, noWait))
			})
		})
	}
}

func TestAStreamOfOldEventsDoesNotSuppressTheHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		start := time.Now()
		mustAppend(t, journal, endingAt(t, 1, start.Add(-catchUpAge)))
		var pulses []time.Time
		for elapsed := appendSpacing; elapsed <= 2*idleAfter; elapsed += appendSpacing {
			time.Sleep(appendSpacing)
			mustAppend(t, journal, endingAt(t, 2, start.Add(-catchUpAge)))
			for _, delivered := range receiveViews(t, journal, noWait) {
				if delivered.Heartbeat {
					pulses = append(pulses, delivered.ReceivedAt)
				}
			}
		}

		want := []time.Time{start.Add(idleAfter), start.Add(2 * idleAfter)}
		if diff := cmp.Diff(want, pulses); diff != "" {
			t.Errorf("heartbeats (-want +got):\n%s", diff)
		}
	})
}

func TestHeartbeatYieldsTheLastQueueSlotToData(t *testing.T) {
	tests := []struct {
		Name      string
		Args      int
		WantPulse bool
	}{
		{Name: "one free slot goes to the data batch", Args: tightQueue},
		{Name: "two free slots fit the heartbeat and the batch", Args: roomForPulse, WantPulse: true},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				journal := idleJournal(t, tt.Args)
				start := time.Now()
				first := endingAt(t, 1, start)
				old := endingAt(t, 2, start.Add(-catchUpAge))
				mustAppend(t, journal, first)
				time.Sleep(idleAfter)

				if err := journal.Append(t.Context(), old); err != nil {
					t.Fatalf("Append into the last free slot: %v, want the data batch accepted", err)
				}

				want := []view{dataView(t, start, first)}
				if tt.WantPulse {
					want = append(want, pulseView(start.Add(idleAfter)))
				}
				want = append(want, dataView(t, start.Add(idleAfter), old))
				assertViews(t, want, receiveViews(t, journal, noWait))
			})
		})
	}
}

func TestReceiveLeavesTheLastFreeSlotToData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, tightQueue)
		start := time.Now()
		queued := endingAt(t, 1, start)
		mustAppend(t, journal, queued)
		time.Sleep(idleAfter)

		lastSlotFree := receiveViews(t, journal, noWait)
		drained := receiveViews(t, journal, noWait)

		assertViews(t, []view{dataView(t, start, queued)}, lastSlotFree)
		assertViews(t, []view{pulseView(start.Add(idleAfter))}, drained)
	})
}

func TestOneSlotJournalWritesNoHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, 1)
		mustAppend(t, journal, endingAt(t, 1, time.Now()))
		releaseAll(mustReceive(t, journal, receiveAll))
		time.Sleep(idleAfter)

		assertViews(t, nil, receiveViews(t, journal, shortWait))
		if err := journal.Append(t.Context(), endingAt(t, 2, time.Now())); err != nil {
			t.Fatalf("Append after an idle period: %v, want the only slot free for data", err)
		}
	})
}

func TestHeartbeatHoldsNoCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		start := time.Now()
		held := endingAt(t, 1, start)
		mustAppend(t, journal, held)
		data := mustReceive(t, journal, receiveAll)
		defer releaseAll(data)
		time.Sleep(idleAfter)

		pulses := mustReceive(t, journal, receiveAll)
		statsWhileHeld := journal.Stats()
		releaseAll(pulses)

		assertViews(t, []view{pulseView(start.Add(idleAfter))}, viewsOf(pulses))
		if diff := cmp.Diff(statsOf(t, held), statsWhileHeld); diff != "" {
			t.Errorf("stats while the heartbeat is unreleased (-want +got):\n%s", diff)
		}
		assertStats(t, journal, statsOf(t, held))
	})
}

func TestClosedJournalWritesNoHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journal := idleJournal(t, roomyBatches)
		mustAppend(t, journal, endingAt(t, 1, time.Now()))
		releaseAll(mustReceive(t, journal, receiveAll))
		journal.Close()
		time.Sleep(idleAfter)

		deliveries, err := journal.Receive(t.Context(), receiveAll, shortWait)

		if !errors.Is(err, processing.ErrFeedClosed) {
			t.Fatalf("Receive after close and idle: %d deliveries, err = %v, want %v",
				len(deliveries), err, processing.ErrFeedClosed)
		}
	})
}
