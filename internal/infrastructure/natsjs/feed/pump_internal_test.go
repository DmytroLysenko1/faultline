package feed

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	receiveLimit = 64
	longWait     = time.Hour
	shortWait    = time.Second
	waitBudget   = 500 * time.Millisecond
	resetStop    = 20 * time.Second
	resetLinger  = time.Second
)

func receiveNow(t *testing.T, feed *Feed) ([]processing.Delivery, error) {
	t.Helper()
	synctest.Wait()
	return feed.Receive(t.Context(), receiveLimit, 0)
}

func TestAPartitionIsHandedOutInOrderWithPositionsAndHeartbeats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		first, second := spansOf(t, 0), spansOf(t, 10)
		h.reader.push(
			step{next: batchAt(t, 1, first, at(0), at(time.Second))},
			step{next: batchAt(t, 2, second, at(2*time.Second), at(2*time.Second))},
			step{next: heartbeatAt(t, 3, at(3*time.Second), at(4*time.Second))},
		)

		got, err := receiveNow(t, h.feed)

		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		want := []received{
			{Sequence: 1, ReceivedAt: at(0), Spans: spansPerRecord},
			{Sequence: 2, ReceivedAt: at(2 * time.Second), Spans: spansPerRecord},
			{Sequence: 3, Heartbeat: true, ReceivedAt: at(3 * time.Second)},
		}
		if diff := cmp.Diff(want, summarize(got)); diff != "" {
			t.Errorf("deliveries mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(first, got[0].Batch().Spans); diff != "" {
			t.Errorf("first batch spans mismatch (-want +got):\n%s", diff)
		}
		if got[0].Position().Partition != 0 || got[2].Bytes() != 0 {
			t.Errorf("first position %+v, heartbeat credit %d; want partition 0 and no credit",
				got[0].Position(), got[2].Bytes())
		}
	})
}

func TestStatsKeepEveryReadRecordAccountedFor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.reader.push(
			step{next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))},
			step{next: heartbeatAt(t, 2, at(time.Second), at(time.Second))},
		)
		got, err := receiveNow(t, h.feed)
		if err != nil || len(got) != 2 {
			t.Fatalf("Receive = %d deliveries, %v; want 2", len(got), err)
		}

		got[0].Release()
		got[0].Release()
		got[1].Release()

		stats := h.feed.Stats()
		if stats.Read != stats.DeliveredBatches+stats.Heartbeats+stats.Held {
			t.Errorf("read %d != delivered %d + heartbeats %d + held %d", stats.Read, stats.DeliveredBatches,
				stats.Heartbeats, stats.Held)
		}
		want := Stats{Read: 2, DeliveredBatches: 1, Heartbeats: 1, Released: 2, Lag: stats.Lag}
		if diff := cmp.Diff(want, stats); diff != "" {
			t.Errorf("stats mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestPendingAndLagFollowTheLastRecordRead(t *testing.T) {
	tests := []struct {
		name    string
		pending uint64
		wantLag time.Duration
	}{
		{name: "records wait behind the last one read", pending: 41, wantLag: 3 * time.Second},
		{name: "a partition read to its end has no lag", pending: 0, wantLag: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				stamped := time.Now().Add(-3 * time.Second)
				next := batchAt(t, 1, spansOf(t, 0), stamped, stamped)
				next.pending = tt.pending
				h.reader.push(step{next: next})
				if _, err := receiveNow(t, h.feed); err != nil {
					t.Fatalf("Receive: %v", err)
				}

				stats := h.feed.Stats()

				if stats.PendingRecords != tt.pending || stats.Lag != tt.wantLag {
					t.Errorf("pending %d and lag %s, want %d and %s", stats.PendingRecords, stats.Lag, tt.pending,
						tt.wantLag)
				}
			})
		})
	}
}

func TestReceivedAtTakesTheEarlierClockAndNeverGoesBack(t *testing.T) {
	type stamps struct {
		leader    time.Time
		writer    time.Time
		heartbeat bool
	}
	tests := []struct {
		name        string
		resumedAt   time.Time
		records     []stamps
		want        []time.Time
		wantClamped uint64
		wantSkew    time.Duration
	}{
		{
			name:     "a writer behind the leader gives the writer's time",
			records:  []stamps{{leader: at(5 * time.Second), writer: at(2 * time.Second)}},
			want:     []time.Time{at(2 * time.Second)},
			wantSkew: 3 * time.Second,
		},
		{
			name:     "a leader whose clock runs ahead does not pass the min",
			records:  []stamps{{leader: at(time.Hour), writer: at(time.Second)}},
			want:     []time.Time{at(time.Second)},
			wantSkew: time.Hour - time.Second,
		},
		{
			name: "a step back keeps the previous time",
			records: []stamps{
				{leader: at(10 * time.Second), writer: at(10 * time.Second)},
				{leader: at(4 * time.Second), writer: at(4 * time.Second)},
				{leader: at(12 * time.Second), writer: at(12 * time.Second)},
			},
			want:        []time.Time{at(10 * time.Second), at(10 * time.Second), at(12 * time.Second)},
			wantClamped: 1,
		},
		{
			name:    "a heartbeat carries journal time, so its skew is not counted",
			records: []stamps{{leader: at(5 * time.Second), writer: at(2 * time.Second), heartbeat: true}},
			want:    []time.Time{at(2 * time.Second)},
		},
		{
			name:        "after a restore the checkpoint's time is the floor",
			resumedAt:   at(time.Minute),
			records:     []stamps{{leader: at(30 * time.Second), writer: at(30 * time.Second)}},
			want:        []time.Time{at(time.Minute)},
			wantClamped: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := open(t, &Start{ReceivedAt: tt.resumedAt}, testOptions(), answering())
				for index, stamp := range tt.records {
					next := batchAt(t, uint64(index+1), spansOf(t, 0), stamp.writer, stamp.leader)
					if stamp.heartbeat {
						next = heartbeatAt(t, uint64(index+1), stamp.writer, stamp.leader)
					}
					h.reader.push(step{next: next})
				}

				got, err := receiveNow(t, h.feed)

				if err != nil {
					t.Fatalf("Receive: %v", err)
				}
				stamped := make([]time.Time, 0, len(got))
				for _, delivery := range got {
					stamped = append(stamped, delivery.Batch().ReceivedAt)
				}
				if diff := cmp.Diff(tt.want, stamped); diff != "" {
					t.Errorf("ReceivedAt mismatch (-want +got):\n%s", diff)
				}
				stats := h.feed.Stats()
				if stats.ReceivedAtClamped != tt.wantClamped || stats.ClockSkewMax != tt.wantSkew {
					t.Errorf("clamped %d and skew %s, want %d and %s", stats.ReceivedAtClamped, stats.ClockSkewMax,
						tt.wantClamped, tt.wantSkew)
				}
			})
		})
	}
}

func TestPositionsOnlyGrowAndARemovedRangeIsCountedAsAGap(t *testing.T) {
	tests := []struct {
		name      string
		after     uint64
		sequences []uint64
		wantGaps  uint64
		wantErr   error
	}{
		{name: "a first start takes the first record it finds", sequences: []uint64{7, 8}},
		{name: "a hole in the middle", sequences: []uint64{1, 2, 5}, wantGaps: 2},
		{name: "a hole right after the checkpoint", after: 10, sequences: []uint64{13}, wantGaps: 2},
		{name: "a record read twice stops the feed", sequences: []uint64{1, 2, 2}, wantErr: ErrConsumerLost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := open(t, &Start{After: processing.Position{Sequence: tt.after}}, testOptions(), answering())
				for _, sequence := range tt.sequences {
					h.reader.push(step{next: heartbeatAt(t, sequence, at(0), at(0))})
				}

				_, err := receiveNow(t, h.feed)
				if err == nil {
					_, err = receiveNow(t, h.feed)
				}

				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Receive = %v, want %v", err, tt.wantErr)
				}
				if gaps := h.feed.Stats().SequenceGapRecords; gaps != tt.wantGaps {
					t.Errorf("gap of %d records, want %d", gaps, tt.wantGaps)
				}
			})
		})
	}
}

func TestTheCreditHoldsThePumpUntilADeliveryIsReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := batchAt(t, 1, spansOf(t, 0), at(0), at(0))
		options := testOptions()
		options.MaxBytes, options.MaxFootprint = weightOf(t, first), weightOf(t, first)
		h := open(t, &Start{}, options, answering())
		h.reader.push(step{next: first}, step{next: batchAt(t, 2, spansOf(t, 10), at(0), at(0))})

		got, err := receiveNow(t, h.feed)
		if err != nil || len(got) != 1 {
			t.Fatalf("first Receive = %d deliveries, %v; want 1", len(got), err)
		}
		held, err := receiveNow(t, h.feed)
		if err != nil || len(held) != 0 {
			t.Fatalf("Receive with the credit spent = %d deliveries, %v; want none", len(held), err)
		}
		if stats := h.feed.Stats(); stats.Read != 2 || stats.Held != 1 {
			t.Fatalf("read %d, held %d; want the second record read and held", stats.Read, stats.Held)
		}

		got[0].Release()
		next, err := receiveNow(t, h.feed)

		if err != nil || len(next) != 1 || next[0].Position().Sequence != 2 {
			t.Errorf("Receive after Release = %v, %v; want position 2", summarize(next), err)
		}
	})
}

func TestNothingFromARecreatedConsumerIsHandedOutBeforeTheStreamIsChecked(t *testing.T) {
	timeout := answer{err: context.DeadlineExceeded}
	tests := []struct {
		name          string
		steps         func(t *testing.T) []step
		stream        *fakeStream
		wantErr       error
		wantDelivered uint64
		wantFailures  uint64
		wantTook      time.Duration
	}{
		{
			name: "a record of a recreated stream",
			steps: func(t *testing.T) []step {
				return []step{{rename: secondConsumer, next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))}}
			},
			stream:  answering(answer{found: identity{Created: recreated()}}),
			wantErr: ErrStreamIdentity,
		},
		{
			name:    "silence from a stream recreated empty",
			steps:   func(*testing.T) []step { return []step{{rename: secondConsumer}} },
			stream:  answering(answer{found: identity{Created: recreated()}}),
			wantErr: ErrStreamIdentity,
		},
		{
			name:    "a deleted stream",
			steps:   func(*testing.T) []step { return []step{{rename: secondConsumer}} },
			stream:  answering(answer{err: jetstream.ErrStreamNotFound}),
			wantErr: ErrStreamIdentity,
		},
		{
			name:    "a stream that refuses to answer",
			steps:   func(*testing.T) []step { return []step{{rename: secondConsumer}} },
			stream:  answering(answer{err: jetstream.ErrJetStreamNotEnabled}),
			wantErr: ErrConsumerLost,
		},
		{
			name: "a stream that does not answer for the whole budget",
			steps: func(t *testing.T) []step {
				return []step{{rename: secondConsumer, next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))}}
			},
			stream:       answering(answer{takes: longWait}),
			wantErr:      ErrConsumerLost,
			wantFailures: 12,
			wantTook:     identityBudget,
		},
		{
			name: "a stream that answers on the third try",
			steps: func(t *testing.T) []step {
				return []step{{rename: secondConsumer, next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))}}
			},
			stream: answering(timeout, answer{err: nats.ErrNoResponders},
				answer{found: identity{Created: streamCreated()}}),
			wantDelivered: 1,
			wantFailures:  2,
			wantTook:      2 * identityRetryGap,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := open(t, &Start{}, testOptions(), tt.stream)
				started := time.Now()
				h.reader.push(tt.steps(t)...)

				got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Receive = %v, want %v", err, tt.wantErr)
				}
				if took := time.Since(started); took != tt.wantTook {
					t.Errorf("answered after %s, want %s", took, tt.wantTook)
				}
				synctest.Wait()
				stats := h.feed.Stats()
				if uint64(len(got)) != tt.wantDelivered || stats.DeliveredBatches != tt.wantDelivered {
					t.Errorf("%d handed out, %d delivered; want %d", len(got), stats.DeliveredBatches, tt.wantDelivered)
				}
				if stats.ConsumerResets != 1 || stats.IdentityCheckFailures != tt.wantFailures {
					t.Errorf("resets %d, identity failures %d; want 1 and %d", stats.ConsumerResets,
						stats.IdentityCheckFailures, tt.wantFailures)
				}
			})
		})
	}
}

func TestAConsumerCheckedOnceIsNotCheckedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := answering()
		h := open(t, &Start{}, testOptions(), stream)
		h.reader.push(
			step{rename: secondConsumer, next: heartbeatAt(t, 1, at(0), at(0))},
			step{next: heartbeatAt(t, 2, at(0), at(0))},
			step{},
		)

		got, err := receiveNow(t, h.feed)

		if err != nil || len(got) != 2 {
			t.Fatalf("Receive = %d deliveries, %v; want 2", len(got), err)
		}
		if calls := stream.described(); calls != 1 {
			t.Errorf("the stream was read %d times, want once for the one new consumer", calls)
		}
	})
}

func TestTheFeedStopsWithItsCauseAndHandsOutNothingMore(t *testing.T) {
	tests := []struct {
		name     string
		failing  func(t *testing.T, h *harness) step
		wantErr  error
		wantStop StopReason
	}{
		{
			name:     "a stream deleted while nats.go recreated the consumer",
			failing:  func(*testing.T, *harness) step { return step{err: jetstream.ErrStreamNotFound} },
			wantErr:  ErrStreamIdentity,
			wantStop: StopIdentity,
		},
		{
			name:     "a consumer nats.go could not recreate",
			failing:  func(*testing.T, *harness) step { return step{err: errors.New("reset attempts exhausted")} },
			wantErr:  ErrConsumerLost,
			wantStop: StopConsumerLost,
		},
		{
			name:     "a record of a version this processor does not know",
			failing:  func(t *testing.T, _ *harness) step { return step{next: unknownVersionAt(t, 2)} },
			wantErr:  ErrUnknownRecordVersion,
			wantStop: StopUnknownVersion,
		},
		{
			name: "a corrupt record the DLQ refuses for good",
			failing: func(_ *testing.T, h *harness) step {
				h.letters.refuse(&jetstream.APIError{ErrorCode: codeSealed})
				return step{next: &message{data: []byte("garbage"), sequence: 2}}
			},
			wantErr:  ErrDeadLetterRejected,
			wantStop: StopDeadLetterRejected,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				h.reader.push(step{next: heartbeatAt(t, 1, at(0), at(0))}, tt.failing(t, h))
				synctest.Wait()

				got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

				if !errors.Is(err, tt.wantErr) || len(got) != 0 {
					t.Fatalf("Receive = %d deliveries, %v; want none and %v, the queued heartbeat withheld", len(got),
						err, tt.wantErr)
				}
				if again, err := h.feed.Receive(t.Context(), receiveLimit, 0); !errors.Is(err, tt.wantErr) {
					t.Errorf("Receive again = %d deliveries, %v; want %v", len(again), err, tt.wantErr)
				}
				if stop := h.feed.Stats().Stop; stop != tt.wantStop {
					t.Errorf("stop reason %s, want %s", stop, tt.wantStop)
				}
			})
		})
	}
}

func TestALostLeaseAnswersAtOnceEvenWithDeliveriesQueued(t *testing.T) {
	tests := []struct {
		name    string
		maxWait time.Duration
		queued  bool
		closed  bool
	}{
		{name: "lost before the receive", queued: true},
		{name: "lost while the receive waits", maxWait: longWait},
		{name: "lost after Close, while the queue drains", queued: true, closed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				if tt.queued {
					h.reader.push(step{next: heartbeatAt(t, 1, at(0), at(0))})
					synctest.Wait()
					if tt.closed {
						h.feed.Close()
					}
					h.lease.lose()
				}
				if !tt.queued {
					time.AfterFunc(time.Second, h.lease.lose)
				}

				got, err := h.feed.Receive(t.Context(), receiveLimit, tt.maxWait)

				if !errors.Is(err, processing.ErrLeaseLost) || len(got) != 0 {
					t.Fatalf("Receive = %d deliveries, %v; want none and processing.ErrLeaseLost", len(got), err)
				}
				if stop := h.feed.Stats().Stop; stop != StopLeaseLost && !tt.closed {
					t.Errorf("stop reason %s, want lease_lost", stop)
				}
			})
		})
	}
}

func TestCloseHandsOutWhatIsQueuedThenErrFeedClosedForGood(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.reader.push(step{next: heartbeatAt(t, 1, at(0), at(0))}, step{next: heartbeatAt(t, 2, at(0), at(0))})
		synctest.Wait()

		h.feed.Close()
		got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)
		_, closed := h.feed.Receive(t.Context(), receiveLimit, longWait)

		if err != nil || len(got) != 2 {
			t.Fatalf("Receive after Close = %d deliveries, %v; want the 2 queued", len(got), err)
		}
		if !errors.Is(closed, processing.ErrFeedClosed) {
			t.Errorf("Receive once drained = %v, want ErrFeedClosed", closed)
		}
		if err := h.feed.Wait(t.Context()); err != nil {
			t.Errorf("Wait: %v", err)
		}
		if stop := h.feed.Stats().Stop; stop != StopClosed {
			t.Errorf("stop reason %s, want closed", stop)
		}
	})
}

func TestCloseDuringAConsumerResetAnswersAtOnceAndWaitKeepsItsBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.reader.stopTakes, h.reader.lingers = resetStop, resetLinger
		h.reader.push(step{hang: true})
		synctest.Wait()
		started := time.Now()

		h.feed.Close()
		_, err := h.feed.Receive(t.Context(), receiveLimit, longWait)
		answered := time.Since(started)
		budget, cancel := context.WithTimeout(t.Context(), waitBudget)
		defer cancel()
		abandoned := h.feed.Wait(budget)

		if !errors.Is(err, processing.ErrFeedClosed) || answered != 0 {
			t.Errorf("Receive after Close = %v after %s, want ErrFeedClosed at once", err, answered)
		}
		if !errors.Is(abandoned, context.DeadlineExceeded) {
			t.Errorf("Wait within %s = %v, want DeadlineExceeded while nats.go stops the reset", waitBudget, abandoned)
		}
		if err := h.feed.Wait(t.Context()); err != nil || time.Since(started) != resetStop+resetLinger {
			t.Errorf("Wait = %v after %s, want nil once the consumer and the pump stopped at %s", err,
				time.Since(started), resetStop+resetLinger)
		}
	})
}

func TestReceiveWaitsForTheFirstDeliveryAndTakesTheReadyOnesUpToLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		later := []step{
			{next: heartbeatAt(t, 1, at(0), at(0))},
			{next: heartbeatAt(t, 2, at(0), at(0))},
			{next: heartbeatAt(t, 3, at(0), at(0))},
		}
		time.AfterFunc(shortWait, func() { h.reader.push(later...) })
		started := time.Now()

		first, err := h.feed.Receive(t.Context(), 2, longWait)

		if err != nil || len(first) == 0 || time.Since(started) != shortWait {
			t.Fatalf("Receive = %d deliveries, %v after %s; want the first after %s", len(first), err,
				time.Since(started), shortWait)
		}
		synctest.Wait()
		rest, err := h.feed.Receive(t.Context(), 2, 0)
		if err != nil || len(first)+len(rest) != 3 || len(rest) > 2 {
			t.Errorf("then %d deliveries, %v; want the remaining of 3, at most 2", len(rest), err)
		}
	})
}

func TestReceiveRefusesABadLimitAndACanceledContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		canceled, cancel := context.WithCancel(t.Context())
		cancel()

		if _, err := h.feed.Receive(t.Context(), 0, 0); !errors.Is(err, ErrInvalidLimit) {
			t.Errorf("limit 0 = %v, want ErrInvalidLimit", err)
		}
		if _, err := h.feed.Receive(canceled, receiveLimit, 0); !errors.Is(err, context.Canceled) {
			t.Errorf("canceled = %v, want context.Canceled", err)
		}
		waiting, stop := context.WithCancel(t.Context())
		time.AfterFunc(shortWait, stop)
		if _, err := h.feed.Receive(waiting, receiveLimit, longWait); !errors.Is(err, context.Canceled) {
			t.Errorf("canceled while waiting = %v, want context.Canceled", err)
		}
	})
}

func TestCloseStopsAPumpThatWaitsForCreditOrForReceive(t *testing.T) {
	tests := []struct {
		name       string
		oneBatch   bool
		heartbeats int
	}{
		{name: "waiting for credit", oneBatch: true},
		{name: "waiting for a full queue to drain", heartbeats: queuedDeliveries + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				first, second := batchAt(t, 1, spansOf(t, 0), at(0), at(0)), batchAt(t, 2, spansOf(t, 10), at(0), at(0))
				options := testOptions()
				if tt.oneBatch {
					options.MaxBytes, options.MaxFootprint = weightOf(t, first), weightOf(t, first)
				}
				h := open(t, &Start{}, options, answering())
				h.reader.push(step{next: first}, step{next: second})
				for index := range tt.heartbeats {
					h.reader.push(step{next: heartbeatAt(t, uint64(index+3), at(0), at(0))})
				}
				synctest.Wait()

				h.feed.Close()

				if err := h.feed.Wait(t.Context()); err != nil {
					t.Errorf("Wait after Close = %v, want the pump stopped", err)
				}
				if read := h.feed.Stats().Read; read == 0 {
					t.Error("the pump read nothing before Close")
				}
			})
		})
	}
}

func TestAWaitingReceiveIsAnsweredByAHaltAtOnce(t *testing.T) {
	tests := []struct {
		name    string
		halt    func(t *testing.T, h *harness) func()
		wantErr error
	}{
		{
			name:    "Close",
			halt:    func(_ *testing.T, h *harness) func() { return h.feed.Close },
			wantErr: processing.ErrFeedClosed,
		},
		{
			name: "a record of an unknown version",
			halt: func(t *testing.T, h *harness) func() {
				return func() { h.reader.push(step{next: unknownVersionAt(t, 1)}) }
			},
			wantErr: ErrUnknownRecordVersion,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				time.AfterFunc(shortWait, tt.halt(t, h))
				started := time.Now()

				got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

				if !errors.Is(err, tt.wantErr) || len(got) != 0 || time.Since(started) != shortWait {
					t.Errorf("Receive = %d deliveries, %v after %s; want %v at %s", len(got), err,
						time.Since(started), tt.wantErr, shortWait)
				}
			})
		})
	}
}

func TestEveryStopReasonHasItsMetricName(t *testing.T) {
	got := make([]string, 0, stopReasonCount+1)
	for reason := range stopReasonCount + 1 {
		got = append(got, reason.String())
	}

	want := []string{"none", "closed", "lease_lost", "identity", "unknown_version", "systemic_corruption",
		"dead_letter_rejected", "consumer_lost", "unknown"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("stop reason names mismatch (-want +got):\n%s", diff)
	}
}

func TestADeliveryQueuedAfterTheClosedAnswerIsNotHandedOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.feed.Close()
		if _, err := h.feed.Receive(t.Context(), receiveLimit, 0); !errors.Is(err, processing.ErrFeedClosed) {
			t.Fatalf("Receive after Close = %v, want ErrFeedClosed", err)
		}

		h.feed.deliveries <- &delivery{position: processing.Position{Sequence: 1}}
		got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

		if !errors.Is(err, processing.ErrFeedClosed) || len(got) != 0 {
			t.Errorf("Receive once closed = %d deliveries, %v; want ErrFeedClosed for good", len(got), err)
		}
	})
}

func TestALeaseLostWhileReceiveReadsWinsOverWhatItRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.reader.push(step{next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))})
		synctest.Wait()
		h.lease.lock.Lock()
		h.lease.lostAt = h.lease.looks + 2
		h.lease.lock.Unlock()

		got, err := h.feed.Receive(t.Context(), receiveLimit, 0)

		if !errors.Is(err, processing.ErrLeaseLost) || len(got) != 0 {
			t.Fatalf("Receive = %d deliveries, %v; want none and processing.ErrLeaseLost", len(got), err)
		}
		if released := h.feed.Stats().Released; released != 1 {
			t.Errorf("%d deliveries released, want the one read and dropped", released)
		}
	})
}
