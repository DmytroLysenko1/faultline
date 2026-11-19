package feed_test

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	garbage        = "garbage"
	oversizedExtra = 1000
	tinyMessage    = 64
	impostor       = "IMPOSTOR"
)

type letterShape struct {
	Subject   string
	Partition string
	Sequence  string
	Reason    string
	Window    string
	Created   string
	Truncated string
	Bytes     int
}

func shapesOf(letters []*jetstream.RawStreamMsg) []letterShape {
	shapes := make([]letterShape, 0, len(letters))
	for _, letter := range letters {
		shapes = append(shapes, letterShape{
			Subject: letter.Subject, Partition: letter.Header.Get("Dead-Letter-Partition"),
			Sequence: letter.Header.Get("Dead-Letter-Sequence"),
			Reason:   letter.Header.Get("Dead-Letter-Reason"), Window: letter.Header.Get("Dead-Letter-Window"),
			Created: letter.Header.Get("Dead-Letter-Stream-Created"), Truncated: letter.Header.Get("Dead-Letter-Truncated"),
			Bytes: len(letter.Data),
		})
	}
	return shapes
}

func TestACorruptRecordIsCopiedToTheDLQOnceAndSkipped(t *testing.T) {
	tests := []struct {
		name          string
		corrupt       []byte
		wantBytes     int
		wantTruncated string
	}{
		{name: "a small record", corrupt: []byte(garbage), wantBytes: len(garbage)},
		{name: "a record over the record limit", corrupt: make([]byte, record.MaxRecordBytes+oversizedExtra),
			wantBytes: record.MaxRecordBytes, wantTruncated: strconv.Itoa(record.MaxRecordBytes + oversizedExtra)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := startLive(t, singleNode, oneReplica)
			l.publish(t, spansOf(t, 0))
			l.publishRaw(t, tt.corrupt)
			l.publish(t, spansOf(t, 10))
			created := strconv.FormatInt(l.info(t).Created.UnixNano(), 10)
			first := l.open(t, feed.Start{}, optionsOf(l.topology))

			got := receive(t, first, 2)
			shut(t, first)
			again := l.open(t, feed.Start{}, optionsOf(l.topology))
			receive(t, again, 2)

			if diff := cmp.Diff([]uint64{1, 3}, sequencesOf(got)); diff != "" {
				t.Errorf("delivered positions mismatch (-want +got):\n%s", diff)
			}
			want := []letterShape{{Subject: "faultline.dlq.p0", Partition: "0", Sequence: "2", Reason: "length", Window: "0,2",
				Created: created, Truncated: tt.wantTruncated, Bytes: tt.wantBytes}}
			if diff := cmp.Diff(want, shapesOf(l.deadLetters(t))); diff != "" {
				t.Errorf("DLQ after a restart mismatch, one copy (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSystemicCorruptionHaltsTheFeedOnNATS(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	for range 3 {
		l.publishRaw(t, []byte(garbage))
	}
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))

	_, err := receiveUntilStopped(t, opened)

	if !errors.Is(err, feed.ErrSystemicCorruption) {
		t.Fatalf("Receive = %v, want ErrSystemicCorruption", err)
	}
	if letters := l.deadLetters(t); len(letters) != 2 {
		t.Errorf("DLQ holds %d copies, want the two before the stop", len(letters))
	}
}

func TestDLQOutageStallsVisiblyOnNATS(t *testing.T) {
	tests := []struct {
		name      string
		damage    func(t *testing.T, l *live)
		mend      func(t *testing.T, l *live)
		wantClass feed.DeadLetterClass
	}{
		{
			name: "a deleted DLQ, back with the p0 holder's next EnsureTopology",
			damage: func(t *testing.T, l *live) {
				if err := l.raw.DeleteStream(t.Context(), natsjs.DeadLetterStream); err != nil {
					t.Fatalf("delete the DLQ: %v", err)
				}
			},
			mend:      func(t *testing.T, l *live) { l.ensure(t) },
			wantClass: "no_responders",
		},
		{
			name: "a full DLQ, freed by its operator",
			damage: func(t *testing.T, l *live) {
				l.editDeadLetters(t, func(config *jetstream.StreamConfig) { config.MaxMsgs = 1 })
				l.publishDeadLetter(t)
			},
			mend:      func(t *testing.T, l *live) { l.editDeadLetters(t, func(c *jetstream.StreamConfig) { c.MaxMsgs = -1 }) },
			wantClass: "stream_full",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := startLive(t, singleNode, oneReplica)
			tt.damage(t, l)
			l.publishRaw(t, []byte(garbage))
			l.publish(t, spansOf(t, 0))
			opened := l.open(t, feed.Start{}, optionsOf(l.topology))

			settled(t, "a visible stall", func() bool { return opened.Stats().DeadLetterLastError == tt.wantClass })
			stalled := opened.Stats()
			tt.mend(t, l)
			got := receive(t, opened, 1)

			if stalled.DeadLetterStalled.IsZero() || stalled.Stop != feed.StopNone {
				t.Errorf("stall at %v, stop %s; want a stall and the feed running", stalled.DeadLetterStalled, stalled.Stop)
			}
			if diff := cmp.Diff([]uint64{2}, sequencesOf(got)); diff != "" {
				t.Errorf("delivered once the DLQ answers mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// publishDeadLetter fills the DLQ with a copy of another partition.
func (l *live) publishDeadLetter(t *testing.T) {
	t.Helper()
	if _, err := l.raw.Publish(t.Context(), l.topology.DeadLetterSubject(1), []byte(garbage)); err != nil {
		t.Fatalf("fill the DLQ: %v", err)
	}
}

func TestAPermanentDLQRefusalStopsTheFeedOnNATS(t *testing.T) {
	tests := []struct {
		name   string
		damage func(t *testing.T, l *live)
	}{
		{name: "a sealed DLQ", damage: func(t *testing.T, l *live) {
			l.editDeadLetters(t, func(config *jetstream.StreamConfig) { config.Sealed = true })
		}},
		{name: "a copy above the DLQ's message limit", damage: func(t *testing.T, l *live) {
			l.editDeadLetters(t, func(config *jetstream.StreamConfig) { config.MaxMsgSize = tinyMessage })
		}},
		{name: "another stream on the DLQ subjects", damage: func(t *testing.T, l *live) {
			if err := l.raw.DeleteStream(t.Context(), natsjs.DeadLetterStream); err != nil {
				t.Fatalf("delete the DLQ: %v", err)
			}
			if _, err := l.raw.CreateStream(t.Context(), jetstream.StreamConfig{Name: impostor,
				Subjects: []string{"faultline.dlq.>"}}); err != nil {
				t.Fatalf("create the impostor: %v", err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := startLive(t, singleNode, oneReplica)
			tt.damage(t, l)
			l.publishRaw(t, []byte(garbage))
			l.publish(t, spansOf(t, 0))
			opened := l.open(t, feed.Start{}, optionsOf(l.topology))

			got, err := receiveUntilStopped(t, opened)

			if !errors.Is(err, feed.ErrDeadLetterRejected) || len(got) != 0 {
				t.Errorf("Receive = %d deliveries, %v; want none and ErrDeadLetterRejected", len(got), err)
			}
		})
	}
}

func TestCloseEndsTheDeadLetterHoldOnNATS(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	if err := l.raw.DeleteStream(t.Context(), natsjs.DeadLetterStream); err != nil {
		t.Fatalf("delete the DLQ: %v", err)
	}
	l.publishRaw(t, []byte(garbage))
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))
	settled(t, "a stall", func() bool { return !opened.Stats().DeadLetterStalled.IsZero() })

	shut(t, opened)

	if stop := opened.Stats().Stop; stop != feed.StopClosed {
		t.Errorf("stop %s, want closed", stop)
	}
}

func TestClosedFeedConnectionStopsTheDeadLetterHold(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	if err := l.raw.DeleteStream(t.Context(), natsjs.DeadLetterStream); err != nil {
		t.Fatalf("delete the DLQ: %v", err)
	}
	l.publishRaw(t, []byte(garbage))
	l.publish(t, spansOf(t, 0))
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))
	settled(t, "a stall", func() bool { return !opened.Stats().DeadLetterStalled.IsZero() })

	l.conn.Feed.NATS.Close()
	got, err := receiveUntilStopped(t, opened)

	if !errors.Is(err, feed.ErrConsumerLost) || len(got) != 0 {
		t.Errorf("Receive = %d deliveries, %v; want ErrConsumerLost and nothing past the held record", len(got), err)
	}
}

// TestSystemicCorruptionHaltsInsteadOfDrainingOnNATS: the window survives a restart through the copy's
// Dead-Letter-Stream-Created, which must equal the stream's own Created as NATS reports it.
func TestSystemicCorruptionHaltsInsteadOfDrainingOnNATS(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	for index, corrupt := range []bool{false, true, false, true, false} {
		l.publishRecordOrGarbage(t, corrupt, uint64(index*spansPerRecord))
	}
	first := l.open(t, feed.Start{}, optionsOf(l.topology))
	receive(t, first, 3)
	shut(t, first)
	l.publishRaw(t, []byte(garbage))
	start := feed.Start{After: at(5), Created: l.info(t).Created, ReceivedAt: time.Now()}
	again := l.open(t, start, optionsOf(l.topology))

	_, err := receiveUntilStopped(t, again)

	if !errors.Is(err, feed.ErrSystemicCorruption) || len(l.deadLetters(t)) != 2 {
		t.Errorf("Receive = %v with %d copies; want the stop on the third corruption and two copies", err,
			len(l.deadLetters(t)))
	}
}

func (l *live) publishRecordOrGarbage(t *testing.T, corrupt bool, firstTrace uint64) {
	t.Helper()
	if corrupt {
		l.publishRaw(t, []byte(garbage))
		return
	}
	l.publish(t, spansOf(t, firstTrace))
}

const (
	versionOffset = 2
	crcOffset     = 24
)

func TestUnknownVersionHaltsTheFeedOnNATS(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	err := l.encoder.Encode(tenant, 0, spansOf(t, 0), time.Now(), 0, func(data []byte, _ string) error {
		newer := slices.Clone(data)
		newer[versionOffset] = record.Version + 1
		binary.LittleEndian.PutUint32(newer[crcOffset:], 0)
		binary.LittleEndian.PutUint32(newer[crcOffset:], crc32.Checksum(newer, crc32.MakeTable(crc32.Castagnoli)))
		l.publishRaw(t, newer)
		return nil
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	l.publish(t, spansOf(t, 10))
	opened := l.open(t, feed.Start{}, optionsOf(l.topology))

	got, stopped := receiveUntilStopped(t, opened)

	if !errors.Is(stopped, feed.ErrUnknownRecordVersion) || len(got) != 0 || len(l.deadLetters(t)) != 0 {
		t.Errorf("Receive = %d deliveries, %v; want a stop with nothing skipped or copied", len(got), stopped)
	}
}
