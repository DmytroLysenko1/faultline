package checkpoint

import (
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	unparsedPointer = `{"v":1,"epoch":`
	journalSubject  = "journal.p0"
	trimmedAt       = 91
)

func (w *world) trim(t *testing.T) {
	t.Helper()
	for range trimmedAt {
		if _, err := w.operator.Publish(t.Context(), journalSubject, []byte("r")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := w.journal(t).Purge(t.Context(), jetstream.WithPurgeSequence(trimmedAt)); err != nil {
		t.Fatalf("trim to %d: %v", trimmedAt, err)
	}
}

func (w *world) dropJournal(t *testing.T) {
	t.Helper()
	if err := w.operator.DeleteStream(t.Context(), w.options.Topology.Stream(w.options.Partition)); err != nil {
		t.Fatalf("delete journal: %v", err)
	}
}

func (w *world) rewriteMeta(t *testing.T, name string, change func(info *jetstream.ObjectInfo)) {
	t.Helper()
	subject := w.metaSubject(name)
	last, err := w.objectStream(t).GetLastMsgForSubject(t.Context(), subject)
	if err != nil {
		t.Fatalf("meta of %s: %v", name, err)
	}
	var info jetstream.ObjectInfo
	if err := json.Unmarshal(last.Data, &info); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	change(&info)
	w.publishMeta(t, subject, &info)
}

func (w *world) publishMeta(t *testing.T, subject string, info *jetstream.ObjectInfo) {
	t.Helper()
	body, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	w.publishRaw(t, subject, body)
}

func (w *world) publishRaw(t *testing.T, subject string, body []byte) {
	t.Helper()
	msg := nats.NewMsg(subject)
	msg.Data = body
	msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	if _, err := w.operator.PublishMsg(t.Context(), msg); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}

func (w *world) objectStream(t *testing.T) jetstream.Stream {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), objStreamPrefix+w.options.objectBucket())
	if err != nil {
		t.Fatalf("object stream: %v", err)
	}
	return stream
}

func (w *world) setRawPointer(t *testing.T, value string) {
	t.Helper()
	if _, err := w.kv.Put(t.Context(), w.options.pointerKey(), []byte(value)); err != nil {
		t.Fatalf("put raw pointer: %v", err)
	}
}

func (w *world) rawPointer(t *testing.T) ([]byte, bool) {
	t.Helper()
	entry, err := w.kv.Get(t.Context(), w.options.pointerKey())
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("get raw pointer: %v", err)
	}
	return entry.Value(), true
}

func (w *world) count(match func(f *frame) bool) *atomic.Int32 {
	var seen atomic.Int32
	w.proxy.intercept(func(f *frame) verdict {
		if match(f) {
			seen.Add(1)
		}
		return forward
	})
	return &seen
}

func (w *world) dropAll(match func(f *frame) bool) {
	w.proxy.intercept(func(f *frame) verdict {
		if match(f) {
			return drop
		}
		return forward
	})
}

func isPointerRead(f *frame) bool {
	return strings.HasPrefix(f.subject, "$JS.API.STREAM.MSG.GET."+kvStreamPrefix+PointerBucket)
}

func isRequest(f *frame) bool {
	return strings.HasPrefix(f.subject, "$JS.API.")
}

func (w *world) isJournalInfo(f *frame) bool {
	return f.subject == "$JS.API.STREAM.INFO."+w.options.Topology.Stream(w.options.Partition)
}

func (w *world) isBucketInfo(f *frame) bool {
	return f.subject == "$JS.API.STREAM.INFO."+objStreamPrefix+w.options.objectBucket()
}
