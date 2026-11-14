package feed_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/journaltest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
)

const (
	firstWritten   = 6
	truncatedBelow = 4
	pausedFor      = 40 * time.Second
	leaderRecords  = 10
	pulledRecords  = 40
	spansPerLarge  = 30_000
	largeFootprint = 8 << 20
	purgedBelow    = 36
	traceStride    = 1000
)

func TestTheNATSFeedKeepsTheFeedContract(t *testing.T) {
	journaltest.RunFeedContract(t, func(t *testing.T, credit int64) *journaltest.Journal {
		l := startLive(t, singleNode, oneReplica)
		options := optionsOf(l.topology)
		options.MaxBytes, options.MaxFootprint = credit, min(credit, maxFootprint)
		opened := l.open(t, feed.Start{}, options)
		return &journaltest.Journal{
			Feed:     opened,
			Append:   func(t *testing.T, spans []span.Span) { t.Helper(); l.publish(t, spans) },
			Close:    func(*testing.T) { opened.Close() },
			LimitErr: feed.ErrInvalidLimit,
		}
	})
}

func TestOpenRefusesOptionsItCannotKeep(t *testing.T) {
	valid := optionsOf(topologyOf(oneReplica))
	tests := []struct {
		name    string
		edit    func(options *feed.Options, start *feed.Start)
		noLease bool
	}{
		{name: "no lease", noLease: true},
		{name: "an invalid topology", edit: func(o *feed.Options, _ *feed.Start) { o.Topology.Replicas = 2 }},
		{name: "a partition outside it", edit: func(o *feed.Options, _ *feed.Start) { o.Partition = 1 }},
		{name: "no epoch", edit: func(o *feed.Options, _ *feed.Start) { o.Epoch = 0 }},
		{name: "a credit below the largest batch",
			edit: func(o *feed.Options, _ *feed.Start) { o.MaxBytes = maxFootprint - 1 }},
		{name: "no largest batch", edit: func(o *feed.Options, _ *feed.Start) { o.MaxFootprint = 0 }},
		{name: "a pull window below two records", edit: func(o *feed.Options, _ *feed.Start) { o.PullMaxBytes = 8 << 20 }},
		{name: "a start of another partition", edit: func(_ *feed.Options, s *feed.Start) { s.After.Partition = 1 }},
		{name: "no idle period", edit: func(o *feed.Options, _ *feed.Start) { o.IdleAfter = 0 }},
		{name: "a checkpoint without its ReceivedAt",
			edit: func(_ *feed.Options, s *feed.Start) { s.After.Sequence = 5 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, start := *valid, feed.Start{}
			if tt.edit != nil {
				tt.edit(&options, &start)
			}
			var lease interface {
				Valid() bool
				Lost() <-chan struct{}
			} = newHeldLease()
			if tt.noLease {
				lease = nil
			}

			opened, err := feed.Open(t.Context(), nil, lease, start, &options)

			if !errors.Is(err, feed.ErrInvalidOptions) || opened != nil {
				t.Errorf("Open = %v, %v; want ErrInvalidOptions and no feed", opened, err)
			}
		})
	}
	if _, err := feed.Open(t.Context(), nil, newHeldLease(), feed.Start{}, nil); !errors.Is(err, feed.ErrInvalidOptions) {
		t.Errorf("Open without options = %v, want ErrInvalidOptions", err)
	}
}

func TestOpenRefusesACheckpointTheStreamDoesNotMatch(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	l.publishMany(t, firstWritten, 0)
	l.purgeBelow(t, truncatedBelow)
	created := l.info(t).Created
	tests := []struct {
		name    string
		start   feed.Start
		missing bool
		wantErr error
	}{
		{name: "a position past the last record",
			start:   feed.Start{ReceivedAt: time.Now(), After: processing.Position{Sequence: firstWritten + 1}},
			wantErr: feed.ErrPositionRange},
		{name: "a position the stream already removed",
			start:   feed.Start{ReceivedAt: time.Now(), After: processing.Position{Sequence: truncatedBelow - 2}},
			wantErr: feed.ErrPositionRange},
		{name: "a stream created after the checkpoint", start: feed.Start{Created: created.Add(-time.Hour)},
			wantErr: feed.ErrStreamIdentity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opened, err := feed.Open(t.Context(), l.conn, newHeldLease(), tt.start, optionsOf(l.topology))

			if !errors.Is(err, tt.wantErr) || opened != nil {
				t.Errorf("Open = %v, %v; want %v", opened, err, tt.wantErr)
			}
			if consumers := l.info(t).State.Consumers; consumers != 0 {
				t.Errorf("%d consumers left on the stream after the refused Open, want none", consumers)
			}
		})
	}
}

func TestOpenAcceptsACheckpointAtEitherEdgeOfTheStream(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	l.publishMany(t, firstWritten, 0)
	l.purgeBelow(t, truncatedBelow)
	tests := []struct {
		name  string
		after uint64
		want  []uint64
	}{
		{name: "just before the first record left", after: truncatedBelow - 1,
			want: consecutive(truncatedBelow, firstWritten-truncatedBelow+1)},
		{name: "at the last record", after: firstWritten, want: []uint64{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := feed.Start{ReceivedAt: time.Now(), After: processing.Position{Sequence: tt.after}}
			opened := l.open(t, start, optionsOf(l.topology))

			got := receive(t, opened, len(tt.want))

			if diff := cmp.Diff(tt.want, sequencesOf(got)); diff != "" {
				t.Errorf("positions mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestOpenFailsWhenThePartitionHasNoStream(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	options := optionsOf(topologyOf(oneReplica))
	options.Topology.Partitions, options.Partition = 2, 1
	tests := []struct {
		name         string
		start        feed.Start
		wantIdentity bool
	}{
		{name: "a first start", start: feed.Start{After: processing.Position{Partition: 1}}},
		{name: "a checkpoint of a stream that is gone", wantIdentity: true,
			start: feed.Start{After: processing.Position{Partition: 1}, Created: l.info(t).Created}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opened, err := feed.Open(t.Context(), l.conn, newHeldLease(), tt.start, options)

			if err == nil || opened != nil || errors.Is(err, feed.ErrStreamIdentity) != tt.wantIdentity {
				t.Errorf("Open on a partition without a stream = %v, %v; identity %t", opened, err, tt.wantIdentity)
			}
		})
	}
}

func TestRecordsComeAtTheirStreamPositionsAsTheJournalWroteThem(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	written := l.publishMany(t, firstWritten, 0)
	opened := l.open(t, feed.Start{ReceivedAt: time.Now(), After: processing.Position{Sequence: 2}, Created: l.info(t).Created},
		optionsOf(l.topology))

	got := receive(t, opened, firstWritten-2)

	if diff := cmp.Diff(consecutive(3, firstWritten-2), sequencesOf(got)); diff != "" {
		t.Errorf("positions mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(written[2:], spansIn(got)); diff != "" {
		t.Errorf("spans mismatch (-want +got):\n%s", diff)
	}
	if opened.Created() != l.info(t).Created {
		t.Errorf("Created %v, want the stream's %v", opened.Created(), l.info(t).Created)
	}
}

func TestAFirstStartOnATruncatedStreamReadsFromItsFirstRecordWithoutAGap(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	l.publishMany(t, firstWritten, 0)
	l.purgeBelow(t, truncatedBelow)
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))

	got := receive(t, opened, firstWritten-truncatedBelow+1)

	if diff := cmp.Diff(consecutive(truncatedBelow, firstWritten-truncatedBelow+1), sequencesOf(got)); diff != "" {
		t.Errorf("positions mismatch (-want +got):\n%s", diff)
	}
	if gaps := opened.Stats().SequenceGapRecords; gaps != 0 {
		t.Errorf("a gap of %d records on a first start, want none", gaps)
	}
}

// TestAPurgeBeyondThePullWindowIsAGapNotALoss: records already pulled still come; the ones removed before
// the pull reach them are counted, so every position is either handed out or in the gap.
func TestAPurgeBeyondThePullWindowIsAGapNotALoss(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	large := spansOf(t, 0)
	for range spansPerLarge / spansPerRecord {
		large = append(large, large[:spansPerRecord]...)
	}
	for index := range pulledRecords {
		large[0] = spansOf(t, uint64(index+1)*traceStride)[0]
		l.publish(t, large)
	}
	options := optionsOf(l.topology)
	options.MaxFootprint = largeFootprint
	opened := l.open(t, feed.Start{}, options)
	first := receive(t, opened, 1)

	l.purgeBelow(t, purgedBelow)
	rest := receive(t, opened, 1)
	rest = append(rest, receiveAll(t, opened, rest[len(rest)-1].Position().Sequence)...)

	got := slices.Concat(first, rest)
	gaps := opened.Stats().SequenceGapRecords
	if uint64(len(got))+gaps != pulledRecords || got[len(got)-1].Position().Sequence != pulledRecords {
		t.Errorf("%d handed out + a gap of %d, last at %d; want every one of %d positions accounted for", len(got),
			gaps, got[len(got)-1].Position().Sequence, pulledRecords)
	}
	if gaps == 0 {
		t.Error("no gap after a purge beyond the pull window")
	}
}

func receiveAll(t *testing.T, opened *feed.Feed, after uint64) []processing.Delivery {
	t.Helper()
	var got []processing.Delivery
	for after < pulledRecords {
		next := receive(t, opened, 1)
		got = append(got, next...)
		after = next[len(next)-1].Position().Sequence
	}
	return got
}

func TestARecreatedStreamStopsTheFeedWithoutHandingOutItsRecords(t *testing.T) {
	tests := []struct {
		name        string
		newRecords  int
		wantDropped bool
	}{
		{name: "recreated empty: the silence is not a heartbeat"},
		{name: "recreated and written again", newRecords: firstWritten},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := startLive(t, singleNode, oneReplica)
			l.publishMany(t, firstWritten, 0)
			opened := l.open(t, feed.Start{}, optionsOf(l.topology))
			receive(t, opened, firstWritten)

			l.recreate(t)
			l.publishMany(t, tt.newRecords, traceStride)
			got, err := receiveUntilStopped(t, opened)

			if !stoppedBy(err, feed.ErrStreamIdentity) || len(got) != 0 {
				t.Fatalf("after the recreate: %d handed out, %v; want none and ErrStreamIdentity", len(got), err)
			}
			stats := opened.Stats()
			if stats.Heartbeats != 0 || stats.DeliveredBatches != firstWritten || stats.Stop != feed.StopIdentity {
				t.Errorf("heartbeats %d, delivered %d, stop %s; want 0, %d and identity", stats.Heartbeats,
					stats.DeliveredBatches, stats.Stop, firstWritten)
			}
		})
	}
}

// TestAPumpPausedLongerThanAPullRequestResumesWithoutAReset (E5): a delivery held for 40 s stops the pump on
// its credit, and the records behind it still come in order afterwards, from the same consumer.
func TestAPumpPausedLongerThanAPullRequestResumesWithoutAReset(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	written := l.publishMany(t, 3, 0)
	options := optionsOf(l.topology)
	options.MaxFootprint = weightOf(t, written[0])
	options.MaxBytes = options.MaxFootprint
	opened := l.open(t, feed.Start{}, options)
	held := receiveHolding(t, opened)

	if paused, err := opened.Receive(t.Context(), receiveLimit, pausedFor); err != nil || len(paused) != 0 {
		t.Fatalf("Receive while the credit is held = %d deliveries, %v; want none", len(paused), err)
	}
	held.Release()
	got := receive(t, opened, len(written)-1)

	if diff := cmp.Diff(consecutive(2, len(written)-1), sequencesOf(got)); diff != "" {
		t.Errorf("positions after the pause mismatch (-want +got):\n%s", diff)
	}
	if resets := opened.Stats().ConsumerResets; resets != 0 {
		t.Errorf("%d consumer resets across the pause, want none", resets)
	}
}

// receiveHolding takes the first delivery without releasing it; with a credit of one record it holds the
// pump.
func receiveHolding(t *testing.T, opened *feed.Feed) processing.Delivery {
	t.Helper()
	deliveries, err := opened.Receive(t.Context(), 1, receiveWait)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("first Receive = %d deliveries, %v; want 1", len(deliveries), err)
	}
	return deliveries[0]
}

// TestLosingTheConsumersNodeLosesAndRepeatsNothing (FMEA 9): the ordered consumer dies with its node, nats.go
// notices the missing heartbeats and recreates it; the feed checks the stream again and carries on from the
// next position.
func TestLosingTheConsumersNodeLosesAndRepeatsNothing(t *testing.T) {
	l := startLive(t, threeNodes, threeReplicas)
	l.publishMany(t, leaderRecords, 0)
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))
	before := receive(t, opened, leaderRecords)

	l.cluster.Stop(l.consumerNode(t))
	l.publishMany(t, leaderRecords, traceStride)
	after := receive(t, opened, leaderRecords)

	got := slices.Concat(before, after)
	if diff := cmp.Diff(consecutive(1, 2*leaderRecords), sequencesOf(got)); diff != "" {
		t.Errorf("positions across the leader change mismatch (-want +got):\n%s", diff)
	}
	if stats := opened.Stats(); stats.Stop != feed.StopNone || stats.SequenceGapRecords != 0 ||
		stats.ConsumerResets == 0 {
		t.Errorf("stop %s, gap %d, resets %d; want the consumer recreated and read on with no gap", stats.Stop,
			stats.SequenceGapRecords, stats.ConsumerResets)
	}
}
