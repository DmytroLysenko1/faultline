package natsjstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	dedupStream   = "DEDUP"
	dedupSubject  = "dedup.chunk"
	dedupMsgID    = "nuid.0"
	dedupWindow   = 4 * time.Second
	dedupPoll     = 250 * time.Millisecond
	dedupPatience = 4 * dedupWindow
)

type dedupWorld struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	start  time.Time
}

func startDedup(t *testing.T) *dedupWorld {
	t.Helper()
	js := JetStream(t, Start(t, oneNode).URLs()[0])
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: dedupStream,
		Subjects: []string{dedupSubject}, Duplicates: dedupWindow, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return &dedupWorld{js: js, stream: stream, start: time.Now()}
}

func (d *dedupWorld) publish(t *testing.T) *jetstream.PubAck {
	t.Helper()
	ack, err := d.js.Publish(t.Context(), dedupSubject, []byte(body), jetstream.WithMsgID(dedupMsgID))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return ack
}

func (d *dedupWorld) held(t *testing.T) uint64 {
	t.Helper()
	info, err := d.stream.Info(t.Context())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	return info.State.Msgs
}

func TestAPurgeKeepsTheDedupWindowOfAMessageID(t *testing.T) {
	tests := []struct {
		name string
		args []jetstream.StreamPurgeOpt
	}{
		{name: "the whole stream purged"},
		{name: "the subject purged", args: []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject(dedupSubject)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := startDedup(t)
			d.publish(t)
			if err := d.stream.Purge(t.Context(), tt.args...); err != nil {
				t.Fatalf("purge: %v", err)
			}

			again := d.publish(t)

			if inWindow := time.Since(d.start) < dedupWindow; !inWindow || !again.Duplicate || d.held(t) != 0 {
				t.Fatalf("a repeat after the purge: duplicate %t, %d held, in the window %t; want suppressed",
					again.Duplicate, d.held(t), inWindow)
			}
			d.storedAfterTheWindow(t)
		})
	}
}

func (d *dedupWorld) storedAfterTheWindow(t *testing.T) {
	t.Helper()
	time.Sleep(time.Until(d.start.Add(dedupWindow)))
	deadline := time.Now().Add(dedupPatience)
	for d.publish(t).Duplicate {
		if time.Now().After(deadline) {
			t.Fatalf("the message id was still a duplicate %s after its window", dedupPatience)
		}
		time.Sleep(dedupPoll)
	}
	if got := d.held(t); got != 1 {
		t.Errorf("%d messages held after the window, want the repeat stored", got)
	}
}
