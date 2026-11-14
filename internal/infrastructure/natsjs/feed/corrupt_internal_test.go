package feed

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	codeStreamFull     = 10077
	codeTooLarge       = 10054
	codeStreamMismatch = 10060
	codeAccountLimits  = 10002
	codeInProcess      = 10158
	firstBodyByte      = record.HeaderBytes + len(testTenant)
)

const garbage = "garbage"

func garbageAt(sequence uint64) *message {
	return &message{data: []byte(garbage), sequence: sequence}
}

func corruptBodyAt(t *testing.T, sequence uint64) *message {
	t.Helper()
	next := batchAt(t, sequence, spansOf(t, 0), at(0), at(0))
	next.data[firstBodyByte] ^= 0xff
	reseal(next.data)
	return next
}

// receiveData hands out what the feed has read by now, releases it and keeps the data positions.
func receiveData(t *testing.T, feed *Feed) []uint64 {
	t.Helper()
	got, err := receiveNow(t, feed)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	sequences := make([]uint64, 0, len(got))
	for _, delivery := range got {
		delivery.Release()
		if !delivery.Batch().Heartbeat {
			sequences = append(sequences, delivery.Position().Sequence)
		}
	}
	return sequences
}

func TestCorruptRecordGoesToTheDLQAndIsSkipped(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(t *testing.T) *message
		want    storedLetter
	}{
		{name: "too short for a header", corrupt: func(*testing.T) *message { return garbageAt(2) },
			want: storedLetter{Sequence: 2, Reason: "length", High: 2, Bytes: len("garbage")}},
		{name: "a bad magic", corrupt: func(t *testing.T) *message {
			next := batchAt(t, 2, spansOf(t, 0), at(0), at(0))
			next.data[0] = 'X'
			return next
		}, want: storedLetter{Sequence: 2, Reason: "magic", High: 2}},
		{name: "a flipped bit", corrupt: func(t *testing.T) *message {
			next := batchAt(t, 2, spansOf(t, 0), at(0), at(0))
			next.data[firstBodyByte] ^= 1
			return next
		}, want: storedLetter{Sequence: 2, Reason: "crc", High: 2}},
		{name: "a record of another partition", corrupt: func(t *testing.T) *message {
			return batchOf(t, 1, 2, spansOf(t, 0), at(0), at(0))
		}, want: storedLetter{Sequence: 2, Reason: "partition", High: 2}},
		{name: "a body that does not decode", corrupt: func(t *testing.T) *message { return corruptBodyAt(t, 2) },
			want: storedLetter{Sequence: 2, Reason: "body", High: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				corrupt := tt.corrupt(t)
				h.reader.push(
					step{next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))},
					step{next: corrupt},
					step{next: batchAt(t, 3, spansOf(t, 10), at(0), at(0))},
				)

				got := receiveData(t, h.feed)

				if diff := cmp.Diff([]uint64{1, 3}, got); diff != "" {
					t.Errorf("delivered positions mismatch (-want +got):\n%s", diff)
				}
				want := tt.want
				want.Bytes = len(corrupt.data)
				if diff := cmp.Diff([]storedLetter{want}, h.letters.copies()); diff != "" {
					t.Errorf("DLQ mismatch (-want +got):\n%s", diff)
				}
				checkAccounted(t, new(h.feed.Stats()))
			})
		})
	}
}

// checkAccounted holds I18 at rest with one corrupt record: every read record was delivered, a heartbeat or
// dead-lettered.
func checkAccounted(t *testing.T, stats *Stats) {
	t.Helper()
	if stats.DeadLettered != 1 || stats.Held != 0 {
		t.Errorf("%d dead-lettered, %d held; want the one corrupt record and none", stats.DeadLettered, stats.Held)
	}
	if sum := stats.DeliveredBatches + stats.Heartbeats + stats.DeadLettered + stats.Held; sum != stats.Read {
		t.Errorf("read %d, accounted for %d: %+v", stats.Read, sum, stats)
	}
}

func TestARestartedFeedDoesNotCopyACorruptRecordTwice(t *testing.T) {
	letters := newFakeLetters()
	synctest.Test(t, func(t *testing.T) {
		first := reopen(t, &Start{}, testOptions(), answering(), letters)
		first.reader.push(step{next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))}, step{next: garbageAt(2)})
		receiveData(t, first.feed)
		first.shut(t)

		again := reopen(t, &Start{After: processing.Position{Sequence: 1}}, testOptions(), answering(), letters)
		again.reader.push(step{next: garbageAt(2)}, step{next: batchAt(t, 3, spansOf(t, 0), at(0), at(0))})

		got := receiveData(t, again.feed)

		if diff := cmp.Diff([]uint64{3}, got); diff != "" {
			t.Errorf("delivered after the restart mismatch (-want +got):\n%s", diff)
		}
		if copies := letters.copies(); len(copies) != 1 {
			t.Errorf("DLQ holds %d copies, want the one kept by its Msg-ID: %+v", len(copies), copies)
		}
		checkAccounted(t, new(again.feed.Stats()))
	})
}

func TestTheCorruptionWindowCountsOnlyEarlierPositionsOfTheLast100(t *testing.T) {
	tests := []struct {
		name     string
		window   window
		sequence uint64
		want     bool
	}{
		{name: "no corruption yet", window: window{}, sequence: 5},
		{name: "one before", window: window{high: 3}, sequence: 5},
		{name: "two within 99", window: window{low: 1, high: 50}, sequence: 100, want: true},
		{name: "the oldest 100 back", window: window{low: 1, high: 50}, sequence: 101},
		{name: "positions below 100", window: window{low: 2, high: 5}, sequence: 9, want: true},
		{name: "a replay of the newest", window: window{low: 2, high: 5}, sequence: 5},
		{name: "a replay of the oldest", window: window{low: 2, high: 5}, sequence: 2},
		{name: "a replay before both", window: window{low: 2, high: 5}, sequence: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.window.systemic(tt.sequence); got != tt.want {
				t.Errorf("%s systemic at %d = %t, want %t", tt.window, tt.sequence, got, tt.want)
			}
		})
	}
}

func TestTheWindowMovesOnlyForANewerCorruption(t *testing.T) {
	tests := []struct {
		name     string
		window   window
		sequence uint64
		want     window
	}{
		{name: "the first", window: window{}, sequence: 7, want: window{high: 7}},
		{name: "a newer one", window: window{high: 7}, sequence: 9, want: window{low: 7, high: 9}},
		{name: "a replayed one", window: window{low: 7, high: 9}, sequence: 7, want: window{low: 7, high: 9}},
		{name: "an older one", window: window{low: 7, high: 9}, sequence: 3, want: window{low: 7, high: 9}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.window.after(tt.sequence), cmp.AllowUnexported(window{})); diff != "" {
				t.Errorf("window after %d mismatch (-want +got):\n%s", tt.sequence, diff)
			}
		})
	}
}

func TestSystemicCorruptionHaltsInsteadOfDraining(t *testing.T) {
	letters := newFakeLetters()
	synctest.Test(t, func(t *testing.T) {
		first := reopen(t, &Start{}, testOptions(), answering(), letters)
		first.reader.push(step{next: garbageAt(2)}, step{next: garbageAt(5)}, step{next: garbageAt(9)})

		_, err := first.feed.Receive(t.Context(), receiveLimit, longWait)

		if !errors.Is(err, ErrSystemicCorruption) || first.feed.Stats().Stop != StopSystemicCorruption {
			t.Fatalf("Receive = %v, stop %s; want ErrSystemicCorruption", err, first.feed.Stats().Stop)
		}
		first.shut(t)
		again := reopen(t, &Start{After: processing.Position{Sequence: 8}}, testOptions(), answering(), letters)
		again.reader.push(step{next: garbageAt(9)})

		if _, err := again.feed.Receive(t.Context(), receiveLimit, longWait); !errors.Is(err, ErrSystemicCorruption) {
			t.Errorf("Receive after the restart = %v, want the same stop on the same record", err)
		}
		want := []storedLetter{{Sequence: 2, Reason: "length", High: 2, Bytes: 7},
			{Sequence: 5, Reason: "length", Low: 2, High: 5, Bytes: 7}}
		if diff := cmp.Diff(want, letters.copies()); diff != "" {
			t.Errorf("DLQ mismatch, no third copy (-want +got):\n%s", diff)
		}
	})
}

func TestAReplayedTailNeitherStopsNorLosesTheWindow(t *testing.T) {
	tests := []struct {
		name   string
		expire []uint64
	}{
		{name: "both copies still deduplicated"},
		{name: "the older copy's duplicate window lapsed", expire: []uint64{2}},
		{name: "both lapsed", expire: []uint64{2, 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			letters := newFakeLetters()
			synctest.Test(t, func(t *testing.T) {
				readTwoCorruptions(t, letters)
				for _, sequence := range tt.expire {
					letters.expire(sequence)
				}
				replay := reopen(t, &Start{After: processing.Position{Sequence: 1}}, testOptions(), answering(), letters)
				replay.reader.push(step{next: garbageAt(2)}, step{next: batchAt(t, 3, spansOf(t, 0), at(0), at(0))},
					step{next: garbageAt(5)})

				if got := receiveData(t, replay.feed); len(got) != 1 {
					t.Fatalf("delivered %v on the replay, want 3 and no stop", got)
				}
				replay.shut(t)
				last := reopen(t, &Start{After: processing.Position{Sequence: 5}}, testOptions(), answering(), letters)
				last.reader.push(step{next: garbageAt(7)})

				if _, err := last.feed.Receive(t.Context(), receiveLimit, longWait); !errors.Is(err, ErrSystemicCorruption) {
					t.Errorf("Receive = %v, want the third corruption within 100 to stop the feed", err)
				}
			})
		})
	}
}

func readTwoCorruptions(t *testing.T, letters *fakeLetters) {
	t.Helper()
	first := reopen(t, &Start{}, testOptions(), answering(), letters)
	first.reader.push(step{next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))}, step{next: garbageAt(2)},
		step{next: batchAt(t, 3, spansOf(t, 0), at(0), at(0))}, step{next: garbageAt(5)})
	if got := receiveData(t, first.feed); len(got) != 2 {
		t.Fatalf("delivered %v, want 1 and 3", got)
	}
	first.shut(t)
}

func TestTheSeedTakesOnlyAWindowItCanTrust(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(letters *fakeLetters)
		wantLow   uint64
		wantError DeadLetterClass
	}{
		{name: "a copy of a recreated stream", prepare: func(letters *fakeLetters) {
			letters.stored = append(letters.stored, deadLetter{created: recreated(), window: window{low: 2, high: 5}})
		}},
		{name: "headers that do not parse", prepare: func(letters *fakeLetters) { letters.malformed = true },
			wantError: seedMalformed},
		{name: "an empty window that is not zero", prepare: func(letters *fakeLetters) {
			letters.stored = append(letters.stored, deadLetter{created: streamCreated(), window: window{low: 7, high: 7}})
		}, wantError: seedMalformed},
		{name: "a window past the stream's end at Open", prepare: func(letters *fakeLetters) {
			letters.stored = append(letters.stored, deadLetter{created: streamCreated(),
				window: window{low: 2, high: lastStreamRecord + 1}})
		}, wantError: seedMalformed},
		{name: "a trusted window", prepare: func(letters *fakeLetters) {
			letters.stored = append(letters.stored, deadLetter{created: streamCreated(), window: window{high: 5}})
		}, wantLow: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				letters := newFakeLetters()
				tt.prepare(letters)
				h := reopen(t, &Start{After: processing.Position{Sequence: 8}}, testOptions(), answering(), letters)
				h.reader.push(step{next: garbageAt(9)}, step{next: batchAt(t, 10, spansOf(t, 0), at(0), at(0))})

				if got := receiveData(t, h.feed); len(got) != 1 {
					t.Fatalf("delivered %v, want 10 after the copy of 9", got)
				}
				copies := letters.copies()
				if newest := copies[len(copies)-1]; newest.Sequence != 9 || newest.Low != tt.wantLow {
					t.Errorf("copy of 9 is %+v, want the window to start at %d", newest, tt.wantLow)
				}
				if got := h.feed.Stats().DeadLetterLastError; got != tt.wantError {
					t.Errorf("last DLQ error %q, want %q", got, tt.wantError)
				}
			})
		})
	}
}

func TestDLQOutageStallsVisibly(t *testing.T) {
	tests := []struct {
		name      string
		refusal   error
		wantClass DeadLetterClass
	}{
		{name: "no responders", refusal: jetstream.ErrNoStreamResponse, wantClass: "no_responders"},
		{name: "a full DLQ", refusal: &jetstream.APIError{ErrorCode: codeStreamFull,
			Description: "maximum bytes exceeded"}, wantClass: "stream_full"},
		{name: "a duplicate still in process", refusal: &jetstream.APIError{ErrorCode: codeInProcess},
			wantClass: "in_process"},
		{name: "account storage used up", refusal: &jetstream.APIError{ErrorCode: codeAccountLimits},
			wantClass: "account_limits"},
		{name: "a timeout", refusal: context.DeadlineExceeded, wantClass: "timeout"},
		{name: "an unknown server error", refusal: &jetstream.APIError{ErrorCode: 1}, wantClass: "unexpected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				h.letters.refuse(tt.refusal, tt.refusal)
				h.reader.push(step{next: batchAt(t, 1, spansOf(t, 0), at(0), at(0))}, step{next: garbageAt(2)},
					step{next: batchAt(t, 3, spansOf(t, 0), at(0), at(0))})

				held := receiveData(t, h.feed)
				stats := h.feed.Stats()

				if len(held) != 1 || stats.DeadLetterStalled.IsZero() || stats.DeadLetterLastError != tt.wantClass {
					t.Fatalf("delivered %v, stalled at %v by %q; want only 1, a stall and %q", held,
						stats.DeadLetterStalled, stats.DeadLetterLastError, tt.wantClass)
				}
				time.Sleep(2 * deadLetterRetryGap)
				if got := receiveData(t, h.feed); len(got) != 1 || got[0] != 3 {
					t.Errorf("delivered %v once the DLQ answers, want 3", got)
				}
				if stats := h.feed.Stats(); !stats.DeadLetterStalled.IsZero() || stats.Stop != StopNone {
					t.Errorf("stall %v, stop %s after the copy; want cleared and running", stats.DeadLetterStalled, stats.Stop)
				}
				checkAccounted(t, new(h.feed.Stats()))
			})
		})
	}
}

func TestAPermanentDLQRefusalStopsTheFeedWithoutSkipping(t *testing.T) {
	tests := []struct {
		name     string
		refusal  error
		wantErr  error
		wantStop StopReason
	}{
		{name: "a sealed DLQ", refusal: &jetstream.APIError{ErrorCode: codeSealed},
			wantErr: ErrDeadLetterRejected, wantStop: StopDeadLetterRejected},
		{name: "another stream on the DLQ subjects", refusal: &jetstream.APIError{ErrorCode: codeStreamMismatch},
			wantErr: ErrDeadLetterRejected, wantStop: StopDeadLetterRejected},
		{name: "a copy above the limit", refusal: &jetstream.APIError{ErrorCode: codeTooLarge},
			wantErr: ErrDeadLetterRejected, wantStop: StopDeadLetterRejected},
		{name: "a copy above the store's limit", refusal: &jetstream.APIError{ErrorCode: codeStreamFull,
			Description: "message too large"}, wantErr: ErrDeadLetterRejected, wantStop: StopDeadLetterRejected},
		{name: "the feed connection closed under a live lease", refusal: nats.ErrConnectionClosed,
			wantErr: ErrConsumerLost, wantStop: StopConsumerLost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				h.letters.refuse(tt.refusal)
				h.reader.push(step{next: garbageAt(1)}, step{next: batchAt(t, 2, spansOf(t, 0), at(0), at(0))})

				got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

				if !errors.Is(err, tt.wantErr) || len(got) != 0 || h.feed.Stats().Stop != tt.wantStop {
					t.Errorf("Receive = %d deliveries, %v, stop %s; want none, %v and %s", len(got), err,
						h.feed.Stats().Stop, tt.wantErr, tt.wantStop)
				}
				if copies := h.letters.copies(); len(copies) != 0 {
					t.Errorf("DLQ holds %+v, want nothing", copies)
				}
			})
		})
	}
}

func TestAStopWhileTheDLQIsDownLeavesQuietly(t *testing.T) {
	tests := []struct {
		name     string
		stop     func(h *harness)
		wantStop StopReason
	}{
		{name: "Close", stop: func(h *harness) { h.feed.Close() }, wantStop: StopClosed},
		{name: "a lost lease", stop: func(h *harness) { h.lease.lose() }, wantStop: StopLeaseLost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				for range queuedSteps {
					h.letters.refuse(jetstream.ErrNoStreamResponse)
				}
				h.reader.push(step{next: garbageAt(1)})
				synctest.Wait()

				tt.stop(h)

				if err := h.feed.Wait(t.Context()); err != nil {
					t.Fatalf("Wait = %v, want the pump to leave the hold", err)
				}
				if stop := h.feed.Stats().Stop; stop != tt.wantStop {
					t.Errorf("stop %s, want %s and not a DLQ refusal", stop, tt.wantStop)
				}
			})
		})
	}
}

func TestASuspendedLeaseHoldsTheCopyWithoutAnAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.lease.suspended.Store(true)
		h.reader.push(step{next: garbageAt(1)}, step{next: batchAt(t, 2, spansOf(t, 0), at(0), at(0))})

		held := receiveData(t, h.feed)

		if tries := h.letters.tries(); len(held) != 0 || tries != 0 {
			t.Fatalf("delivered %v after %d DLQ attempts, want nothing tried", held, tries)
		}
		if got := h.feed.Stats().DeadLetterLastError; got != stallLeaseInvalid {
			t.Errorf("stall class %q, want %q", got, stallLeaseInvalid)
		}
		h.lease.suspended.Store(false)
		time.Sleep(2 * deadLetterRetryGap)
		if got := receiveData(t, h.feed); len(got) != 1 {
			t.Errorf("delivered %v once the lease is valid again, want 2", got)
		}
	})
}

func TestTheFirstCorruptionWaitsForTheSeedWithoutDroppingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.letters.failLast(context.DeadlineExceeded)
		h.reader.push(step{next: garbageAt(1)}, step{next: batchAt(t, 2, spansOf(t, 0), at(0), at(0))})

		held := receiveData(t, h.feed)

		if len(held) != 0 || h.feed.Stats().DeadLetterStalled.IsZero() {
			t.Fatalf("delivered %v, stall %v; want the record held", held, h.feed.Stats().DeadLetterStalled)
		}
		time.Sleep(2 * deadLetterRetryGap)
		if got := receiveData(t, h.feed); len(got) != 1 || len(h.letters.copies()) != 1 {
			t.Errorf("delivered %v with %d copies, want 2 after the copy of 1", got, len(h.letters.copies()))
		}
	})
}

func TestACorruptRecordGivesItsCreditBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		good := batchAt(t, 1, spansOf(t, 0), at(0), at(0))
		weight := weightOf(t, good)
		options := testOptions()
		options.MaxFootprint, options.MaxBytes = weight, 2*weight
		h := open(t, &Start{}, options, answering())
		h.reader.push(step{next: good}, step{next: corruptBodyAt(t, 2)},
			step{next: batchAt(t, 3, spansOf(t, 0), at(0), at(0))})

		got, err := receiveNow(t, h.feed)

		if err != nil || len(got) != 2 || got[1].Position().Sequence != 3 {
			t.Fatalf("Receive = %v, %v; want 1 held and 3 within the credit the corrupt record gave back",
				summarize(got), err)
		}
	})
}

func TestADeadLetterCarriesWhatTheOperatorNeeds(t *testing.T) {
	tests := []struct {
		name          string
		size          int
		wantBody      int
		wantTruncated string
	}{
		{name: "a small record", size: 7, wantBody: 7},
		{name: "a record over the limit", size: record.MaxRecordBytes + 10, wantBody: record.MaxRecordBytes,
			wantTruncated: strconv.Itoa(record.MaxRecordBytes + 10)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			letter := &deadLetter{partition: 3, sequence: 42, created: streamCreated(), reason: "crc",
				data: make([]byte, tt.size), window: window{low: 7, high: 42}}

			msg := letter.message("faultline.dlq.p3")

			want := map[string]string{headerPartition: "3", headerSequence: "42", headerReason: "crc",
				headerCreated: strconv.FormatInt(streamCreated().UnixNano(), decimal), headerWindow: "7,42",
				headerTruncated: tt.wantTruncated}
			got := map[string]string{}
			for name := range want {
				got[name] = msg.Header.Get(name)
			}
			if diff := cmp.Diff(want, got); diff != "" || len(msg.Data) != tt.wantBody {
				t.Errorf("%d body bytes, headers mismatch (-want +got):\n%s", len(msg.Data), diff)
			}
			wantID := "dlq-p3-" + strconv.FormatInt(streamCreated().UnixNano(), decimal) + "-42"
			if id := letter.msgID(); id != wantID {
				t.Errorf("Msg-ID %q, want %q", id, wantID)
			}
		})
	}
}

func TestTheLatestCopyTellsTheWindowBack(t *testing.T) {
	tests := []struct {
		name    string
		created string
		window  string
		want    lastLetter
		wantErr error
	}{
		{name: "a copy this feed wrote", created: "1000", window: "7,42",
			want: lastLetter{created: time.Unix(0, 1000), window: window{low: 7, high: 42}}},
		{name: "no window", created: "1000", window: "", wantErr: errMalformedLetter},
		{name: "a word for a position", created: "1000", window: "7,x", wantErr: errMalformedLetter},
		{name: "no stream identity", created: "", window: "7,42", wantErr: errMalformedLetter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := nats.Header{}
			header.Set(headerCreated, tt.created)
			header.Set(headerWindow, tt.window)

			got, err := parseLetter(header)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("parseLetter = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(lastLetter{}, window{})); diff != "" {
				t.Errorf("letter mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestAnEmptiedDLQForgetsTheWindow: an operator who empties the DLQ resets what the seed finds, so two more
// corruptions are copied before the third stops the feed — the runbook's price of emptying it.
func TestAnEmptiedDLQForgetsTheWindow(t *testing.T) {
	letters := newFakeLetters()
	synctest.Test(t, func(t *testing.T) {
		readTwoCorruptions(t, letters)
		letters.purge()
		h := reopen(t, &Start{After: processing.Position{Sequence: 5}}, testOptions(), answering(), letters)
		h.reader.push(step{next: garbageAt(7)}, step{next: garbageAt(9)}, step{next: garbageAt(11)})

		_, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

		if !errors.Is(err, ErrSystemicCorruption) {
			t.Fatalf("Receive = %v, want the stop at 11", err)
		}
		want := []storedLetter{{Sequence: 7, Reason: "length", High: 7, Bytes: 7},
			{Sequence: 9, Reason: "length", Low: 7, High: 9, Bytes: 7}}
		if diff := cmp.Diff(want, letters.copies()); diff != "" {
			t.Errorf("DLQ after it was emptied mismatch (-want +got):\n%s", diff)
		}
	})
}

const (
	offsetKind  = 3
	offsetFlags = 4
	futureKind  = 9
	futureFlags = 1
)

func TestUnknownVersionHaltsTheFeedWithoutSkipping(t *testing.T) {
	tests := []struct {
		name   string
		offset int
		value  byte
	}{
		{name: "a later format version", offset: offsetVersion, value: futureVersion},
		{name: "a record kind this processor does not know", offset: offsetKind, value: futureKind},
		{name: "header flags this processor does not know", offset: offsetFlags, value: futureFlags},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openPlain(t)
				next := batchAt(t, 1, spansOf(t, 0), at(0), at(0))
				next.data[tt.offset] = tt.value
				reseal(next.data)
				h.reader.push(step{next: next}, step{next: batchAt(t, 2, spansOf(t, 0), at(0), at(0))})

				got, err := h.feed.Receive(t.Context(), receiveLimit, longWait)

				if !errors.Is(err, ErrUnknownRecordVersion) || len(got) != 0 || len(h.letters.copies()) != 0 {
					t.Errorf("Receive = %d deliveries, %v with %d copies; want a stop, nothing skipped or copied",
						len(got), err, len(h.letters.copies()))
				}
			})
		})
	}
}

func TestADeadLetteredLastRecordLeavesNothingHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openPlain(t)
		h.reader.push(step{next: garbageAt(1)})

		receiveData(t, h.feed)

		checkAccounted(t, new(h.feed.Stats()))
	})
}
