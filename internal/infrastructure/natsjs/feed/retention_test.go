package feed_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
)

const (
	quickIdle      = time.Second
	heartbeatWait  = 45 * time.Second
	writtenRecords = 10
	bigRecords     = 40
	bigRecordBytes = 7 << 20
	recordsPerCut  = (256 << 20) / bigRecordBytes
	smallStream    = 16 << 20
	fillerBytes    = 1 << 20
)

func at(sequence uint64) processing.Position {
	return processing.Position{Sequence: sequence}
}

// heartbeatAfter waits for the feed to hand out a heartbeat and returns its position.
func heartbeatAfter(t *testing.T, opened *feed.Feed) uint64 {
	t.Helper()
	deadline := time.Now().Add(heartbeatWait)
	for time.Now().Before(deadline) {
		deliveries, err := opened.Receive(t.Context(), receiveLimit, receiveWait)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		for _, delivery := range deliveries {
			delivery.Release()
			if delivery.Batch().Heartbeat {
				return delivery.Position().Sequence
			}
		}
	}
	t.Fatalf("no heartbeat within %s", heartbeatWait)
	return 0
}

func quickOptions(l *live) *feed.Options {
	options := optionsOf(l.topology)
	options.IdleAfter = quickIdle
	return options
}

// readAndCommit reads the two records written and purges them, as a processor does after a checkpoint.
func readAndCommit(t *testing.T, l *live) {
	t.Helper()
	l.publishMany(t, 2, 0)
	opened := l.open(t, feed.Start{}, quickOptions(l))
	receive(t, opened, 2)
	if err := opened.Commit(t.Context(), at(2)); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	shut(t, opened)
}

func restoredAtTwo(t *testing.T, l *live, conn *natsjs.Conn) *feed.Feed {
	t.Helper()
	start := feed.Start{After: at(2), Created: l.info(t).Created, ReceivedAt: time.Now()}
	opened, err := feed.Open(t.Context(), conn, newHeldLease(), start, quickOptions(l))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { shut(t, opened) })
	return opened
}

func TestAnIdlePartitionGetsHeartbeatsFromItsProcessorOnNATS(t *testing.T) {
	tests := []struct {
		name     string
		nodes    int
		replicas int
		prepare  func(t *testing.T, l *live) *feed.Feed
		wantAt   uint64
	}{
		{name: "a new stream, R1", nodes: singleNode, replicas: oneReplica, wantAt: 3,
			prepare: func(t *testing.T, l *live) *feed.Feed {
				l.publishMany(t, 2, 0)
				opened := l.open(t, feed.Start{}, quickOptions(l))
				receive(t, opened, 2)
				return opened
			}},
		{name: "a restore at the last record of a purged stream, R1", nodes: singleNode, replicas: oneReplica,
			wantAt: 3, prepare: func(t *testing.T, l *live) *feed.Feed {
				readAndCommit(t, l)
				return restoredAtTwo(t, l, l.conn)
			}},
		{name: "a restore at the last record of a purged stream, R3", nodes: threeNodes, replicas: threeReplicas,
			wantAt: 3, prepare: func(t *testing.T, l *live) *feed.Feed {
				readAndCommit(t, l)
				return restoredAtTwo(t, l, l.conn)
			}},
		{name: "a purged stream after a server restart, R1", nodes: singleNode, replicas: oneReplica, wantAt: 3,
			prepare: func(t *testing.T, l *live) *feed.Feed {
				readAndCommit(t, l)
				l.cluster.Stop(0)
				l.cluster.Restart(0)
				return restoredAtTwo(t, l, l.redial(t))
			}},
		{name: "a corrupt last record, R1", nodes: singleNode, replicas: oneReplica, wantAt: 3,
			prepare: func(t *testing.T, l *live) *feed.Feed {
				l.publish(t, spansOf(t, 0))
				l.publishRaw(t, []byte(garbage))
				opened := l.open(t, feed.Start{}, quickOptions(l))
				receive(t, opened, 1)
				return opened
			}},
		{name: "a first start on a truncated stream, R1", nodes: singleNode, replicas: oneReplica, wantAt: 4,
			prepare: func(t *testing.T, l *live) *feed.Feed {
				l.publishMany(t, 3, 0)
				l.purgeBelow(t, 3)
				opened := l.open(t, feed.Start{}, quickOptions(l))
				receive(t, opened, 1)
				return opened
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := startLive(t, tt.nodes, tt.replicas)
			opened := tt.prepare(t, l)

			got := heartbeatAfter(t, opened)

			stats := opened.Stats()
			if got != tt.wantAt || stats.HeartbeatConflicts != 0 || stats.HeartbeatsWritten == 0 {
				t.Errorf("heartbeat at %d with %d conflicts, %d written; want right after the last record at %d",
					got, stats.HeartbeatConflicts, stats.HeartbeatsWritten, tt.wantAt)
			}
		})
	}
}

func TestPurgeNeverPassesTheFirstUnfinishedPosition(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	l.publishMany(t, writtenRecords, 0)
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))
	receive(t, opened, writtenRecords)

	half := opened.Commit(t.Context(), at(5))
	afterHalf := l.info(t).State.FirstSeq
	all := opened.Commit(t.Context(), at(writtenRecords))
	afterAll := l.info(t).State
	past := opened.Commit(t.Context(), at(writtenRecords+1))

	if err := errors.Join(half, all); err != nil || afterHalf != 6 {
		t.Fatalf("Commit(5) = %v, first record %d; want 6", err, afterHalf)
	}
	if afterAll.Msgs != 0 || afterAll.FirstSeq != writtenRecords+1 {
		t.Errorf("after Commit(%d): %d records from %d, want none from %d", writtenRecords, afterAll.Msgs,
			afterAll.FirstSeq, writtenRecords+1)
	}
	if !errors.Is(past, feed.ErrPositionRange) || l.info(t).State.LastSeq != writtenRecords {
		t.Errorf("Commit past the end = %v, want ErrPositionRange and the stream as it was", past)
	}
}

func TestARecreatedStreamStopsTheFeedWithoutPurging(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	l.publishMany(t, writtenRecords, 0)
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))
	receive(t, opened, writtenRecords)
	l.recreate(t)
	l.publishMany(t, writtenRecords, 0)

	err := opened.Commit(t.Context(), at(5))
	_, stopped := receiveUntilStopped(t, opened)

	if !errors.Is(err, feed.ErrStreamIdentity) && !errors.Is(err, processing.ErrFeedClosed) {
		t.Errorf("Commit = %v, want ErrStreamIdentity", err)
	}
	if !errors.Is(stopped, feed.ErrStreamIdentity) {
		t.Errorf("Receive = %v, want the feed stopped by the stream's identity", stopped)
	}
	if state := l.info(t).State; state.FirstSeq != 1 || state.Msgs != writtenRecords {
		t.Errorf("new stream holds %d records from %d, want all %d kept", state.Msgs, state.FirstSeq, writtenRecords)
	}
}

// TestPurgeKeepsTheReplayTailBoundedByBytes: a large backlog goes in steps of about 256 MiB, so the records a
// restart replays shrink by a step every checkpoint interval, and two steps empty this stream.
func TestPurgeKeepsTheReplayTailBoundedByBytes(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	for range bigRecords {
		l.publishRaw(t, make([]byte, bigRecordBytes))
	}
	start := feed.Start{ReceivedAt: time.Now(), After: at(bigRecords), Created: l.info(t).Created}
	opened := l.open(t, start, optionsOf(l.topology))

	first := opened.Commit(t.Context(), at(bigRecords))
	afterFirst := l.info(t).State.FirstSeq
	second := opened.Commit(t.Context(), at(bigRecords))

	if err := errors.Join(first, second); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if afterFirst != recordsPerCut+1 || l.info(t).State.Msgs != 0 {
		t.Errorf("first record %d after one Commit, %d left after two; want %d and none", afterFirst,
			l.info(t).State.Msgs, recordsPerCut+1)
	}
}

// TestASlowProcessorTurnsIntoBackpressureNotLoss (FMEA 8): a full stream refuses new records instead of
// dropping old ones, and Commit is what makes room.
func TestASlowProcessorTurnsIntoBackpressureNotLoss(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	l.topology.MaxBytesPerPartition = smallStream
	l.recreate(t)
	written := 0
	for l.publishOnce(t, make([]byte, fillerBytes), rawID(written)) == nil {
		written++
	}
	start := feed.Start{ReceivedAt: time.Now(), After: at(uint64(written)), Created: l.info(t).Created}
	opened := l.open(t, start, optionsOf(l.topology))
	refused := l.publishOnce(t, make([]byte, fillerBytes), rawID(written))

	if err := opened.Commit(t.Context(), at(2)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if refused == nil {
		t.Error("a full stream took a new record, want a refusal")
	}
	if state := l.info(t).State; state.FirstSeq != 3 || state.LastSeq != uint64(written) {
		t.Errorf("stream holds %d..%d, want 3..%d: only what was committed is gone", state.FirstSeq,
			state.LastSeq, written)
	}
	if err := l.publishOnce(t, make([]byte, fillerBytes), rawID(written+1)); err != nil {
		t.Errorf("publish after Commit = %v, want room again", err)
	}
}

func rawID(index int) string {
	return "filler-" + strconv.Itoa(index)
}
