package journal_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	requestSpans    = 40
	concurrentCalls = 32
	answerDelay     = time.Second
	callerBudget    = 10 * time.Second
	lastOfFour      = uint16(3)
)

func TestNewRefusesOptionsItCannotKeep(t *testing.T) {
	tests := []struct {
		name string
		args func(*journal.Options)
	}{
		{name: "no clock", args: func(o *journal.Options) { o.Clock = nil }},
		{name: "no tenant", args: func(o *journal.Options) { o.Tenant = "" }},
		{name: "a tenant that is not a DNS label", args: func(o *journal.Options) { o.Tenant = "Tenant_A" }},
		{name: "no partitions", args: func(o *journal.Options) { o.Topology.Partitions = 0 }},
		{name: "more partitions than streams", args: func(o *journal.Options) {
			o.Topology.Partitions = natsjs.MaxPartitions + 1
		}},
		{name: "a publish timeout that leaves no attempt after the reserve", args: func(o *journal.Options) {
			o.PublishTimeout = doomedAt
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := optionsOf(topologyOf(fourPartitions, oneReplica))
			tt.args(options)

			_, err := journal.New(acking(), options)

			if !errors.Is(err, journal.ErrInvalidOptions) {
				t.Errorf("New = %v, want ErrInvalidOptions", err)
			}
		})
	}
	if _, err := journal.New(acking(), nil); !errors.Is(err, journal.ErrInvalidOptions) {
		t.Errorf("New(nil options) = %v, want ErrInvalidOptions", err)
	}
	valid := optionsOf(topologyOf(fourPartitions, oneReplica))
	if _, err := journal.New(nil, valid); !errors.Is(err, journal.ErrInvalidOptions) {
		t.Errorf("New(nil publisher) = %v, want ErrInvalidOptions", err)
	}
}

// TestEveryPartitionGetsOneRecordOfItsOwnSpansInRequestOrder: the processor of a partition must see
// every span of its traces and none of another's, in the order the client sent them.
func TestEveryPartitionGetsOneRecordOfItsOwnSpansInRequestOrder(t *testing.T) {
	publisher := acking()
	spans := spansAcross(requestSpans, 1)

	if err := appendWithin(t, newJournal(t, publisher, fourPartitions), callerBudget, spans); err != nil {
		t.Fatalf("Append: %v", err)
	}

	calls := publisher.published()
	if diff := cmp.Diff([]uint16{0, 1, 2, 3}, publisher.partitions()); diff != "" {
		t.Fatalf("published partitions (-want +got):\n%s", diff)
	}
	for _, made := range calls {
		header, got := decode(t, made)
		want := record.Header{Version: record.Version, Kind: record.KindBatch, Tenant: testTenant,
			Partition: made.partition, RulesVersion: rulesVersion}
		if diff := cmp.Diff(want, header, cmpopts.IgnoreFields(record.Header{}, "Weight", "WrittenAt")); diff != "" {
			t.Errorf("partition %d header (-want +got):\n%s", made.partition, diff)
		}
		if diff := cmp.Diff(spansOfPartition(spans, made.partition, fourPartitions), got); diff != "" {
			t.Errorf("partition %d spans (-want +got):\n%s", made.partition, diff)
		}
	}
}

func TestARequestOnOnePartitionOfManyMakesOneRecordThere(t *testing.T) {
	publisher := acking()
	spans := spansOfPartition(spansAcross(requestSpans, 1), lastOfFour, fourPartitions)

	if err := appendWithin(t, newJournal(t, publisher, fourPartitions), callerBudget, spans); err != nil {
		t.Fatalf("Append: %v", err)
	}

	calls := publisher.published()
	if len(calls) != 1 || calls[0].partition != lastOfFour {
		t.Fatalf("published partitions %v, want only %d", publisher.partitions(), lastOfFour)
	}
	if _, got := decode(t, calls[0]); !cmp.Equal(spans, got) {
		t.Errorf("partition %d spans (-want +got):\n%s", lastOfFour, cmp.Diff(spans, got))
	}
}

// TestTheSameRequestMakesTheSameMessageIDs: a client retry, written seconds later, must hit the
// dedup window of the records the first attempt stored.
func TestTheSameRequestMakesTheSameMessageIDs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publisher := acking()
		journalOf := newJournal(t, publisher, fourPartitions)
		spans := spansAcross(requestSpans, 1)

		first := appendWithin(t, journalOf, callerBudget, spans)
		time.Sleep(time.Minute)
		again := appendWithin(t, journalOf, callerBudget, spans)
		other := appendWithin(t, journalOf, callerBudget, spansAcross(requestSpans, requestSpans+1))

		if err := errors.Join(first, again, other); err != nil {
			t.Fatalf("Append: %v", err)
		}
		ids := idsByPartition(publisher.published())
		for partition, got := range ids {
			if len(got) != 3 || got[0] != got[1] || got[0] == got[2] {
				t.Errorf("partition %d message ids %q, want the retry's equal and another request's different",
					partition, got)
			}
		}
	})
}

func idsByPartition(calls []call) map[uint16][]string {
	ids := make(map[uint16][]string)
	for _, made := range calls {
		ids[made.partition] = append(ids[made.partition], made.msgID)
	}
	return ids
}

// TestAnEmptyRequestPublishesNothing: nothing to store is stored, whatever the caller's context.
func TestAnEmptyRequestPublishesNothing(t *testing.T) {
	publisher := acking()
	ctx, cancel := cancelled(t.Context())
	defer cancel()

	err := newJournal(t, publisher, fourPartitions).Append(ctx, nil)

	if err != nil || len(publisher.published()) != 0 {
		t.Errorf("Append(nil) = %v after %d publishes, want nil after none", err, len(publisher.published()))
	}
}

// TestNoPathAnswersOKWithoutAPubAck (I13): whichever partition fails, for whatever reason, the
// request is not acknowledged, and the failure keeps its ingest meaning.
func TestNoPathAnswersOKWithoutAPubAck(t *testing.T) {
	for reason := range natsjs.Reason(natsjs.ReasonCount) {
		for failing := range uint16(fourPartitions) {
			t.Run(fmt.Sprintf("%s on partition %d", reason, failing), func(t *testing.T) {
				publisher := answering(map[uint16]answer{failing: {fails: true, reason: reason}})
				journalOf := newJournal(t, publisher, fourPartitions)

				err := appendWithin(t, journalOf, callerBudget, spansAcross(requestSpans, 1))

				if diff := cmp.Diff(sentinelsOf(reason), matchedSentinels(err)); diff != "" {
					t.Errorf("partition %d failing: Append = %v, sentinels (-want +got):\n%s", failing, err, diff)
				}
				if errors.Is(err, ingest.ErrNotAttempted) {
					t.Errorf("partition %d failing: Append = %v says not attempted, but the write was tried", failing, err)
				}
				if got := journalOf.Stats().Failed[reason]; got != 1 {
					t.Errorf("failed[%s] = %d, want 1", reason, got)
				}
			})
		}
	}
}

var ingestSentinels = []error{ingest.ErrOverloaded, ingest.ErrBatchTooLarge, ingest.ErrUnavailable}

// sentinelsOf is the one ingest meaning a failure may carry: the OTLP handler maps the first it finds.
func sentinelsOf(reason natsjs.Reason) []string {
	if reason == natsjs.ReasonStreamFull {
		return []string{ingest.ErrOverloaded.Error()}
	}
	if reason == natsjs.ReasonTooLarge {
		return []string{ingest.ErrBatchTooLarge.Error()}
	}
	return []string{ingest.ErrUnavailable.Error()}
}

func matchedSentinels(err error) []string {
	var matched []string
	for _, sentinel := range ingestSentinels {
		if errors.Is(err, sentinel) {
			matched = append(matched, sentinel.Error())
		}
	}
	return matched
}

// TestTheMostFinalFailureDecidesTheAnswer: a retry cannot fix a request too large, and an overloaded
// journal asks the client to back off — both say more than unavailable, even when they come last.
func TestTheMostFinalFailureDecidesTheAnswer(t *testing.T) {
	unavailable := answer{fails: true, reason: natsjs.ReasonNoResponders}
	full := answer{fails: true, reason: natsjs.ReasonStreamFull}
	tooLarge := answer{fails: true, reason: natsjs.ReasonTooLarge, after: answerDelay}
	tests := []struct {
		name    string
		args    map[uint16]answer
		wantErr error
	}{
		{name: "every record acknowledged, one a duplicate", args: map[uint16]answer{1: {duplicate: true}}},
		{name: "unavailable only", args: map[uint16]answer{0: unavailable, 2: unavailable},
			wantErr: ingest.ErrUnavailable},
		{name: "overloaded over unavailable", args: map[uint16]answer{0: unavailable, 3: full},
			wantErr: ingest.ErrOverloaded},
		{name: "too large, though it answers last", args: map[uint16]answer{0: full, 1: unavailable, 3: tooLarge},
			wantErr: ingest.ErrBatchTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				err := appendWithin(t, newJournal(t, answering(tt.args), fourPartitions), callerBudget,
					spansAcross(requestSpans, 1))

				if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
					t.Errorf("Append = %v, want %v", err, tt.wantErr)
				}
			})
		})
	}
}

// TestACallerWhoGivesUpGetsItsOwnCancelNotUnavailable: a client that cancelled must hear CANCELLED;
// mapped to unavailable it would be counted as a journal outage and retried.
func TestACallerWhoGivesUpGetsItsOwnCancelNotUnavailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journalOf := newJournal(t, answering(map[uint16]answer{0: {hang: true}}), onePartition)
		ctx, cancel := context.WithCancel(t.Context())
		stop := time.AfterFunc(answerDelay/2, cancel)
		defer stop.Stop()

		err := journalOf.Append(ctx, spansAcross(requestSpans, 1))

		if !errors.Is(err, context.Canceled) || errors.Is(err, ingest.ErrUnavailable) {
			t.Errorf("Append = %v, want the plain cancel", err)
		}
		if got := journalOf.Stats().Interrupted; got != 1 {
			t.Errorf("interrupted = %d, want 1", got)
		}
	})
}

// TestEveryRecordAcknowledgedIsOKThoughTheCallerGaveUpMeanwhile: the records are stored; answering
// the cancel instead would break "nil exactly when every record has its PubAck".
func TestEveryRecordAcknowledgedIsOKThoughTheCallerGaveUpMeanwhile(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	journalOf := newJournal(t, answering(map[uint16]answer{0: {then: cancel}}), onePartition)

	err := journalOf.Append(ctx, spansAcross(requestSpans, 1))

	if err != nil || journalOf.Stats().Interrupted != 0 {
		t.Errorf("Append = %v, interrupted %d; want nil and none", err, journalOf.Stats().Interrupted)
	}
}

func TestACallerAlreadyGoneIsAnsweredWithItsOwnErrorWithoutAPublish(t *testing.T) {
	tests := []struct {
		name    string
		args    func(context.Context) (context.Context, context.CancelFunc)
		wantErr error
	}{
		{name: "cancelled", args: cancelled, wantErr: context.Canceled},
		{name: "deadline passed", args: func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithDeadline(parent, time.Now().Add(-time.Second))
		}, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := acking()
			journalOf := newJournal(t, publisher, onePartition)
			ctx, cancel := tt.args(t.Context())
			defer cancel()

			err := journalOf.Append(ctx, spansAcross(requestSpans, 1))

			if !errors.Is(err, tt.wantErr) || errors.Is(err, ingest.ErrUnavailable) || len(publisher.published()) != 0 {
				t.Errorf("Append = %v after %d publishes, want %v after none", err, len(publisher.published()),
					tt.wantErr)
			}
		})
	}
}

func cancelled(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	cancel()
	return ctx, cancel
}

// TestThePublishDeadlineLeavesTheCallerTimeToHearTheAnswer: the publish ends 100 ms before the
// caller's deadline, so the caller hears why instead of its own deadline; without a deadline, or with
// a long one, a publish gets PublishTimeout. The journal's clock is frozen in the past: the caller's
// deadline is wall time and must be measured as such.
func TestThePublishDeadlineLeavesTheCallerTimeToHearTheAnswer(t *testing.T) {
	tests := []struct {
		name string
		args time.Duration
		want time.Duration
	}{
		{name: "no deadline", want: publishTimeout},
		{name: "a long deadline", args: callerBudget, want: publishTimeout},
		{name: "a second left", args: time.Second, want: time.Second - reserve},
		{name: "just enough left", args: doomedAt + time.Millisecond, want: doomedAt + time.Millisecond - reserve},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				publisher := acking()
				ctx, cancel := withBudget(t.Context(), tt.args)
				defer cancel()
				options := optionsOf(topologyOf(onePartition, oneReplica))
				options.Clock = frozenClock{}
				journalOf, err := journal.New(publisher, options)
				if err != nil {
					t.Fatalf("New: %v", err)
				}

				if err := journalOf.Append(ctx, spansAcross(1, 1)); err != nil {
					t.Fatalf("Append: %v", err)
				}

				if made := publisher.published()[0]; !made.bounded || made.budget != tt.want {
					t.Errorf("publish budget %s (bounded %t), want %s", made.budget, made.bounded, tt.want)
				}
			})
		})
	}
}

func withBudget(parent context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if budget == 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, budget)
}

// TestAPublishThatRunsOutOfItsBudgetIsUnavailableBeforeTheCallersDeadline: a lost quorum must be
// answered as retryable while the client still listens, not as the client's own deadline.
func TestAPublishThatRunsOutOfItsBudgetIsUnavailableBeforeTheCallersDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journalOf := newJournal(t, answering(map[uint16]answer{0: {hang: true}}), onePartition)
		started := time.Now()

		err := appendWithin(t, journalOf, time.Second, spansAcross(1, 1))

		if !errors.Is(err, ingest.ErrUnavailable) {
			t.Errorf("Append = %v, want ErrUnavailable", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Append = %v also matches the caller's own deadline, which has not passed", err)
		}
		if took := time.Since(started); took != time.Second-reserve {
			t.Errorf("answered after %s, want %s of a 1s deadline", took, time.Second-reserve)
		}
	})
}

// TestAFullPartitionOutranksAHungOneAtThePublishDeadline: the answer waits for the hung partition's
// budget, so the full one is not hidden behind a timeout, and still comes before the caller's
// deadline.
func TestAFullPartitionOutranksAHungOneAtThePublishDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journalOf := newJournal(t, answering(map[uint16]answer{
			0: {fails: true, reason: natsjs.ReasonStreamFull},
			3: {hang: true},
		}), fourPartitions)
		started := time.Now()

		err := appendWithin(t, journalOf, time.Second, spansAcross(requestSpans, 1))

		if !errors.Is(err, ingest.ErrOverloaded) {
			t.Errorf("Append = %v, want ErrOverloaded", err)
		}
		if took := time.Since(started); took != time.Second-reserve {
			t.Errorf("answered after %s, want %s", took, time.Second-reserve)
		}
	})
}

// TestACallerWhoGivesUpOnManyPartitionsGetsItsOwnCancel: the parallel path answers the cancel too.
func TestACallerWhoGivesUpOnManyPartitionsGetsItsOwnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hung := answer{hang: true}
		journalOf := newJournal(t, answering(map[uint16]answer{0: hung, 1: hung, 2: hung, 3: hung}), fourPartitions)
		ctx, cancel := context.WithCancel(t.Context())
		stop := time.AfterFunc(answerDelay/2, cancel)
		defer stop.Stop()

		err := journalOf.Append(ctx, spansAcross(requestSpans, 1))

		if !errors.Is(err, context.Canceled) || errors.Is(err, ingest.ErrUnavailable) {
			t.Errorf("Append = %v, want the plain cancel", err)
		}
	})
}

// TestTooLittleTimeLeftIsRefusedBeforeAnythingIsPublished: a publish that cannot finish might still
// be stored after the client heard a failure, so it is not started.
func TestTooLittleTimeLeftIsRefusedBeforeAnythingIsPublished(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publisher := acking()
		journalOf := newJournal(t, publisher, onePartition)

		err := appendWithin(t, journalOf, doomedAt, spansAcross(1, 1))

		if !errors.Is(err, ingest.ErrUnavailable) || !errors.Is(err, ingest.ErrNotAttempted) ||
			len(publisher.published()) != 0 {
			t.Errorf("Append = %v after %d publishes, want ErrUnavailable, not attempted, after none", err,
				len(publisher.published()))
		}
		if stats := journalOf.Stats(); stats.Doomed != 1 || stats.Append != (journal.Histogram{}) {
			t.Errorf("doomed %d, append latencies %v; want 1 and none: a refused request is not a fast one",
				stats.Doomed, stats.Append)
		}
	})
}

// TestPartitionsArePublishedInParallel: four partitions answering in a second each take one second,
// not four, or a request over many partitions would outlive its deadline.
func TestPartitionsArePublishedInParallel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := answer{after: answerDelay}
		journalOf := newJournal(t, answering(map[uint16]answer{0: slow, 1: slow, 2: slow, 3: slow}), fourPartitions)
		started := time.Now()

		if err := appendWithin(t, journalOf, callerBudget, spansAcross(requestSpans, 1)); err != nil {
			t.Fatalf("Append: %v", err)
		}

		if took := time.Since(started); took != answerDelay {
			t.Errorf("four partitions took %s, want %s", took, answerDelay)
		}
	})
}

// TestABatchTooLargeForOneRecordIsRefusedWithoutItsPublish: a retry would be just as large, so the
// client hears too large even though another partition was stored.
func TestABatchTooLargeForOneRecordIsRefusedWithoutItsPublish(t *testing.T) {
	publisher := acking()
	huge := spanOn(1)
	huge.Operation.Name = strings.Repeat("x", record.MaxRecordBytes)
	spans := []span.Span{spanOn(0), huge}

	journalOf := newJournal(t, publisher, twoPartitions)

	err := appendWithin(t, journalOf, callerBudget, spans)

	if !errors.Is(err, ingest.ErrBatchTooLarge) || !errors.Is(err, record.ErrTooLarge) {
		t.Errorf("Append = %v, want ErrBatchTooLarge", err)
	}
	if diff := cmp.Diff([]uint16{0}, publisher.partitions()); diff != "" {
		t.Errorf("published partitions (-want +got):\n%s", diff)
	}
	if got := journalOf.Stats().EncodeTooLarge; got != 1 {
		t.Errorf("encode too large = %d, want 1: a batch refused before its publish must still be counted", got)
	}
}

// TestCloseWaitsForAppendsInFlightAndRefusesNewOnes: the connection must not close under a publish
// awaiting its PubAck, and nothing may start after it.
func TestCloseWaitsForAppendsInFlightAndRefusesNewOnes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publisher := answering(map[uint16]answer{0: {after: answerDelay}})
		journalOf := newJournal(t, publisher, onePartition)
		var appends sync.WaitGroup
		var inFlight error
		appends.Go(func() { inFlight = appendWithin(t, journalOf, callerBudget, spansAcross(1, 1)) })
		synctest.Wait()
		started := time.Now()

		closeErr := journalOf.Close(t.Context())
		appends.Wait()

		if closeErr != nil || inFlight != nil {
			t.Fatalf("Close = %v, the append in flight = %v; want both nil", closeErr, inFlight)
		}
		if took := time.Since(started); took != answerDelay {
			t.Errorf("Close returned after %s, the append in flight needed %s", took, answerDelay)
		}
		late := appendWithin(t, journalOf, callerBudget, spansAcross(1, 2))
		if !errors.Is(late, ingest.ErrUnavailable) || !errors.Is(late, ingest.ErrNotAttempted) ||
			len(publisher.published()) != 1 || journalOf.Stats().Closing != 1 {
			t.Errorf("Append after Close = %v after %d publishes, want ErrUnavailable and no publish", late,
				len(publisher.published()))
		}
	})
}

func TestCloseGivesUpWhenItsContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journalOf := newJournal(t, answering(map[uint16]answer{0: {hang: true}}), onePartition)
		var appends sync.WaitGroup
		appends.Go(func() {
			if err := appendWithin(t, journalOf, callerBudget, spansAcross(1, 1)); !errors.Is(err, ingest.ErrUnavailable) {
				t.Errorf("the hung append = %v, want ErrUnavailable at its publish timeout", err)
			}
		})
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), answerDelay)
		defer cancel()

		err := journalOf.Close(ctx)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close = %v, want DeadlineExceeded", err)
		}
		appends.Wait()
		if err := journalOf.Close(t.Context()); err != nil {
			t.Errorf("Close again after the append ended = %v", err)
		}
	})
}

// TestStatsCountEveryPublishAndAppend: what operators alert on — failures by reason, duplicates
// from retries, and how long PubAcks and whole requests take.
func TestStatsCountEveryPublishAndAppend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journalOf := newJournal(t, answering(map[uint16]answer{
			0: {after: 3 * time.Millisecond},
			1: {after: 30 * time.Millisecond, duplicate: true},
			2: {after: 300 * time.Millisecond, fails: true, reason: natsjs.ReasonStreamFull},
		}), fourPartitions)

		if err := appendWithin(t, journalOf, callerBudget, spansAcross(requestSpans, 1)); err == nil {
			t.Fatal("Append with a full partition succeeded")
		}

		want := journal.Stats{Published: 2, Duplicates: 1}
		want.Failed[natsjs.ReasonStreamFull] = 1
		want.PubAck[bucketOf(t, 0)] = 1
		want.PubAck[bucketOf(t, 3*time.Millisecond)] = 1
		want.PubAck[bucketOf(t, 30*time.Millisecond)] = 1
		want.PubAck[bucketOf(t, 300*time.Millisecond)] = 1
		want.Append[bucketOf(t, 300*time.Millisecond)] = 1
		if diff := cmp.Diff(want, journalOf.Stats()); diff != "" {
			t.Errorf("stats (-want +got):\n%s", diff)
		}
	})
}

// TestAFailureThePublisherDidNotClassifyIsCountedAsUnexpected: a decorated or future publisher may
// return any error; it must show up as a failure, never as a published record.
func TestAFailureThePublisherDidNotClassifyIsCountedAsUnexpected(t *testing.T) {
	tests := []struct {
		name string
		args answer
		want []string
	}{
		{name: "a plain error", args: answer{plain: true}, want: []string{ingest.ErrUnavailable.Error()}},
		{name: "a plain error that has its ingest meaning", args: answer{plain: true, meaning: ingest.ErrOverloaded},
			want: []string{ingest.ErrOverloaded.Error()}},
		{name: "a reason out of range", args: answer{fails: true, reason: natsjs.Reason(natsjs.ReasonCount)},
			want: []string{ingest.ErrUnavailable.Error()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			journalOf := newJournal(t, answering(map[uint16]answer{0: tt.args}), onePartition)

			err := appendWithin(t, journalOf, callerBudget, spansAcross(1, 1))

			if diff := cmp.Diff(tt.want, matchedSentinels(err)); diff != "" {
				t.Errorf("Append = %v, sentinels (-want +got):\n%s", err, diff)
			}
			stats := journalOf.Stats()
			if stats.Published != 0 || stats.Failed[natsjs.ReasonUnexpected] != 1 {
				t.Errorf("published %d, unexpected %d; want 0 and 1", stats.Published, stats.Failed[natsjs.ReasonUnexpected])
			}
		})
	}
}

// TestCloseCalledAgainShareOneWaiterAndReportsADrainedJournal: retried Closes add no goroutines, and
// once the appends are done every Close says so, even with its context already over.
func TestCloseCalledAgainShareOneWaiterAndReportsADrainedJournal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		journalOf := newJournal(t, answering(map[uint16]answer{0: {after: answerDelay}}), onePartition)
		var appends sync.WaitGroup
		appends.Go(func() {
			if err := appendWithin(t, journalOf, callerBudget, spansAcross(1, 1)); err != nil {
				t.Errorf("the append in flight = %v", err)
			}
		})
		synctest.Wait()
		for range concurrentCalls {
			expired, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			if err := journalOf.Close(expired); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Close while the append runs = %v, want DeadlineExceeded", err)
			}
			cancel()
		}
		appends.Wait()
		synctest.Wait()
		over, cancel := context.WithCancel(t.Context())
		cancel()

		if err := journalOf.Close(over); err != nil {
			t.Errorf("Close of a drained journal with its context over = %v, want nil", err)
		}
	})
}

// TestLatencyBoundsStrictlyIncrease: a bound dropped from the fixed-size literal would compile as a
// zero bucket and file every slow publish in the wrong place.
func TestLatencyBoundsStrictlyIncrease(t *testing.T) {
	bounds := journal.LatencyBounds()
	previous := time.Duration(0)
	for index, bound := range bounds {
		if bound <= previous {
			t.Errorf("bound %d = %s after %s, want strictly increasing and above zero", index, bound, previous)
		}
		previous = bound
	}
}

func bucketOf(t *testing.T, took time.Duration) int {
	t.Helper()
	bounds := journal.LatencyBounds()
	for bucket, bound := range bounds {
		if took <= bound {
			return bucket
		}
	}
	return len(bounds)
}

// TestConcurrentAppendsAreEachAnsweredOnTheirOwn: the ingester calls Append from every gRPC
// handler at once.
func TestConcurrentAppendsAreEachAnsweredOnTheirOwn(t *testing.T) {
	publisher := acking()
	journalOf := newJournal(t, publisher, fourPartitions)
	errs := make([]error, concurrentCalls)
	var appends sync.WaitGroup
	for index := range concurrentCalls {
		appends.Go(func() {
			errs[index] = appendWithin(t, journalOf, callerBudget, spansAcross(requestSpans, uint64(index*requestSpans+1)))
		})
	}
	appends.Wait()

	if err := errors.Join(errs...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	calls := publisher.published()
	if got := journalOf.Stats().Published; got != uint64(len(calls)) {
		t.Errorf("published = %d, the stream got %d", got, len(calls))
	}
	for _, made := range calls {
		if _, spans := decode(t, made); len(spans) == 0 {
			t.Errorf("partition %d got an empty record", made.partition)
		}
	}
}
