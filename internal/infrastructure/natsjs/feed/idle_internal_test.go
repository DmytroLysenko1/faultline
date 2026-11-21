package feed

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	checkpointed  = 10
	firstPulseAt  = 45 * time.Second
	secondPulseAt = 90 * time.Second
	quietFor      = 100 * time.Second
)

func openIdle(t *testing.T) *harness {
	t.Helper()
	start := &Start{After: processing.Position{Sequence: checkpointed}, ReceivedAt: at(0)}
	return open(t, start, idleOptions(), answering())
}

func checkPulses(t *testing.T, h *harness, want []writtenPulse) {
	t.Helper()
	if diff := cmp.Diff(want, h.journal.pulses()); diff != "" {
		t.Errorf("heartbeats in the stream mismatch (-want +got):\n%s", diff)
	}
}

func TestAnIdlePartitionGetsHeartbeatsFromItsProcessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := open(t, &Start{}, idleOptions(), answering())
		h.reader.push(step{next: h.journal.ingest(t, spansOf(t, 0), at(0))})
		receiveData(t, h.feed)

		time.Sleep(quietFor)
		got, err := receiveNow(t, h.feed)

		checkPulses(t, h, []writtenPulse{
			{After: 1, At: 2, WrittenAt: at(firstPulseAt)},
			{After: 2, At: 3, WrittenAt: at(secondPulseAt)},
		})
		want := []received{
			{Sequence: 2, Heartbeat: true, ReceivedAt: at(firstPulseAt)},
			{Sequence: 3, Heartbeat: true, ReceivedAt: at(secondPulseAt)},
		}
		if diff := cmp.Diff(want, summarize(got)); err != nil || diff != "" {
			t.Errorf("Receive = %v; heartbeats handed out mismatch (-want +got):\n%s", err, diff)
		}
		if diff := cmp.Diff(pulseCounts{Written: 2}, pulseCountsOf(new(h.feed.Stats()))); diff != "" {
			t.Errorf("heartbeat counts mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestARestoredIdlePartitionGetsAHeartbeatRightAfterItsCheckpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openIdle(t)

		time.Sleep(firstPulseAt + time.Second)

		checkPulses(t, h, []writtenPulse{{After: checkpointed, At: checkpointed + 1, WrittenAt: at(firstPulseAt)}})
	})
}

func TestALastCorruptRecordIsTheBaseOfTheNextHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openIdle(t)
		h.reader.push(step{next: h.journal.corrupt()})

		time.Sleep(firstPulseAt + time.Second)

		checkPulses(t, h, []writtenPulse{{After: checkpointed + 1, At: checkpointed + 2, WrittenAt: at(firstPulseAt)}})
		if copies := h.letters.copies(); len(copies) != 1 {
			t.Errorf("DLQ holds %+v, want the corrupt record", copies)
		}
	})
}

func quiet(t *testing.T, h *harness) {
	t.Helper()
	time.Sleep(quietFor)
	checkPulses(t, h, nil)
}

func TestAHeartbeatIsWrittenOnlyAfterTheFirstDataRecord(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, h *harness)
	}{
		{name: "nothing read", prepare: func(*testing.T, *harness) {}},
		{name: "only a former holder's heartbeat read", prepare: func(t *testing.T, h *harness) {
			h.reader.push(step{next: h.journal.foreign(t, at(0), 0)})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := open(t, &Start{}, idleOptions(), answering())
				tt.prepare(t, h)

				quiet(t, h)
			})
		})
	}
}

func TestAStalledFeedWritesNoHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := batchAt(t, checkpointed+1, spansOf(t, 0), at(0), at(0))
		options := idleOptions()
		options.MaxFootprint, options.MaxBytes = weightOf(t, first), weightOf(t, first)
		h := open(t, &Start{After: processing.Position{Sequence: checkpointed}, ReceivedAt: at(0)}, options,
			answering())
		h.reader.push(step{next: first}, step{next: batchAt(t, checkpointed+2, spansOf(t, 0), at(0), at(0))})
		if _, err := receiveNow(t, h.feed); err != nil {
			t.Fatalf("Receive: %v", err)
		}

		quiet(t, h)
	})
}

func TestOnlyTheLeaseHolderReadsWritesHeartbeatsAndPurges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openIdle(t)
		h.lease.suspended.Store(true)
		h.reader.push(step{next: h.journal.corrupt()})

		quiet(t, h)
		err := h.feed.Commit(t.Context(), processing.Position{Sequence: checkpointed})

		if tries, purges := h.letters.tries(), h.stream.purged(); !errors.Is(err, ErrLeaseSuspended) || tries != 0 ||
			len(purges) != 0 {
			t.Errorf("Commit = %v after %d DLQ attempts and purges %v; want ErrLeaseSuspended and nothing written",
				err, tries, purges)
		}
	})
}

func openRead(t *testing.T) *harness {
	t.Helper()
	h := open(t, &Start{}, idleOptions(), answering())
	h.reader.push(step{next: h.journal.ingest(t, spansOf(t, 0), at(0))})
	receiveData(t, h.feed)
	return h
}

func TestALaggingFeedWritesNoHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openRead(t)
		lagAt := 40 * time.Second
		time.AfterFunc(lagAt, func() {
			h.reader.push(step{next: h.journal.ingestBehind(t, spansOf(t, 0), at(lagAt), 1)})
		})

		time.Sleep(lagAt + nextWait/2)
		checkPulses(t, h, nil)
		time.Sleep(nextWait)

		caughtUp := lagAt + nextWait
		checkPulses(t, h, []writtenPulse{{After: 2, At: 3, WrittenAt: at(caughtUp)}})
	})
}

func TestAValidLeaseAgainResumesTheHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openIdle(t)
		h.lease.suspended.Store(true)
		time.Sleep(firstPulseAt + time.Second)

		h.lease.suspended.Store(false)
		time.Sleep(nextWait)

		resumed := firstPulseAt + nextWait
		checkPulses(t, h, []writtenPulse{{After: checkpointed, At: checkpointed + 1, WrittenAt: at(resumed)}})
	})
}

func TestAFailedHeartbeatIsCountedAndTriedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openIdle(t)
		h.journal.fail(context.DeadlineExceeded)

		time.Sleep(firstPulseAt + nextWait + time.Second)

		retried := firstPulseAt + nextWait
		checkPulses(t, h, []writtenPulse{{After: checkpointed, At: checkpointed + 1, WrittenAt: at(retried)}})
		if diff := cmp.Diff(pulseCounts{Written: 1, Failures: 1}, pulseCountsOf(new(h.feed.Stats()))); diff != "" {
			t.Errorf("heartbeat counts mismatch, the feed still running (-want +got):\n%s", diff)
		}
	})
}

func TestAHeartbeatNeverLandsBeforeAnUnreadRecord(t *testing.T) {
	tests := []struct {
		name          string
		hidden        bool
		cuts          int
		want          []writtenPulse
		wantConflicts uint64
		wantUnread    uint64
		wantUnforced  uint64
	}{
		{
			name: "records read between three conflicts force one heartbeat",
			cuts: starvedConflicts,
			want: []writtenPulse{{After: 3, At: 5, WrittenAt: at(firstPulseAt), Unconditional: true},
				{After: 5, At: 6, WrittenAt: at(secondPulseAt)}},
			wantConflicts: starvedConflicts,
			wantUnforced:  1,
		},
		{
			name:          "conflicts with nothing read never force one",
			hidden:        true,
			cuts:          queuedSteps,
			wantConflicts: 1,
			wantUnread:    3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := openRead(t)
				h.journal.interfere(t, tt.cuts, tt.hidden)

				time.Sleep(quietFor)

				checkPulses(t, h, tt.want)
				stats := h.feed.Stats()
				got := []uint64{stats.HeartbeatConflicts, stats.HeartbeatConflictsUnread, stats.HeartbeatsUnconditional}
				if diff := cmp.Diff([]uint64{tt.wantConflicts, tt.wantUnread, tt.wantUnforced}, got); diff != "" {
					t.Errorf("conflicts, unread, unconditional mismatch (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestStarvationCountsOnlyConflictsInARow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := openRead(t)
		cuts := 0
		h.journal.lock.Lock()
		h.journal.cut = 2 * (starvedConflicts - 1)
		h.journal.cutIn = func(sequence uint64, writtenAt time.Time) *message {
			cuts++
			if cuts == starvedConflicts-1 {
				return heartbeatAt(t, sequence, writtenAt, writtenAt)
			}
			return batchAt(t, sequence, spansOf(t, 0), writtenAt, writtenAt)
		}
		h.journal.lock.Unlock()

		time.Sleep(quietFor)

		want := pulseCounts{Written: 1, Conflicts: 2 * (starvedConflicts - 1)}
		if diff := cmp.Diff(want, pulseCountsOf(new(h.feed.Stats()))); diff != "" {
			t.Errorf("heartbeat counts mismatch, none forced (-want +got):\n%s", diff)
		}
		checkPulses(t, h, []writtenPulse{{After: 5, At: 6, WrittenAt: at(secondPulseAt)}})
	})
}

const (
	leaderBehind = time.Minute
	longQuiet    = 200 * time.Second
)

func TestALaggingLeaderGivesOneHeartbeatPerIdlePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := open(t, &Start{}, idleOptions(), answering())
		h.journal.lock.Lock()
		h.journal.leaderLag = leaderBehind
		h.journal.lock.Unlock()
		h.reader.push(step{next: batchAt(t, h.journal.ingest(t, spansOf(t, 0), at(0)).sequence, spansOf(t, 0), at(0),
			at(-leaderBehind))})

		time.Sleep(longQuiet)

		landings := h.journal.landings()
		if len(landings) == 0 || len(landings) > int(longQuiet/testIdleAfter) {
			t.Fatalf("%d heartbeats in %s, want at least one and at most one per %s", len(landings), longQuiet,
				testIdleAfter)
		}
		for index := 1; index < len(landings); index++ {
			if gap := landings[index].Sub(landings[index-1]); gap < testIdleAfter {
				t.Errorf("heartbeats %d and %d %s apart, want at least %s", index-1, index, gap, testIdleAfter)
			}
		}
	})
}

type pulseCounts struct {
	Written       uint64
	Conflicts     uint64
	Unread        uint64
	Failures      uint64
	Unconditional uint64
	Stop          StopReason
}

func pulseCountsOf(stats *Stats) pulseCounts {
	return pulseCounts{
		Written:       stats.HeartbeatsWritten,
		Conflicts:     stats.HeartbeatConflicts,
		Unread:        stats.HeartbeatConflictsUnread,
		Failures:      stats.HeartbeatFailures,
		Unconditional: stats.HeartbeatsUnconditional,
		Stop:          stats.Stop,
	}
}
