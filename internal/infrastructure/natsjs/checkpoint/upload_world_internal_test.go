package checkpoint

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	framed "github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	uploadChunk    = 512
	settleWithin   = 20 * time.Second
	settlePoll     = 10 * time.Millisecond
	stopWithin     = 6 * time.Second
	firstPosition  = 10
	secondPosition = 20
	thirdPosition  = 30
	chunkPrefix    = objSubjectPrefix + "faultline_checkpoints_p0" + chunkSubjectPart
	metaPrefix     = objSubjectPrefix + "faultline_checkpoints_p0" + metaSubjectPart
	pointerSubject = kvSubjectPrefix + PointerBucket + ".p0"
	bucketInfo     = "$JS.API.STREAM.INFO." + objStreamPrefix + "faultline_checkpoints_p0"
)

// fakeRetention is the feed as the store sees it: it remembers every position committed and can refuse.
type fakeRetention struct {
	created time.Time
	mu      sync.Mutex
	commits []uint64
	refuse  int
}

func (r *fakeRetention) Created() time.Time {
	return r.created
}

func (r *fakeRetention) Commit(_ context.Context, through processing.Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refuse > 0 {
		r.refuse--
		return errInjected
	}
	r.commits = append(r.commits, through.Sequence)
	return nil
}

func (r *fakeRetention) committed() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.commits...)
}

// started stakes an empty partition and starts the store with chunks small enough for a real image to span six.
func (w *world) started(t *testing.T) (*Sink, *fakeRetention) {
	t.Helper()
	if _, _, err := w.store.Latest(t.Context(), testPartition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v", err)
	}
	if _, err := w.store.StakeEmpty(t.Context()); err != nil {
		t.Fatalf("StakeEmpty: %v", err)
	}
	w.store.chunkBytes = uploadChunk
	retention := &fakeRetention{created: w.streamCreated(t)}
	sink, err := w.store.Start(t.Context(), retention)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { w.stop(t) })
	return sink, retention
}

func (w *world) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), stopWithin)
	defer cancel()
	w.store.Close(ctx)
	if err := w.store.Wait(ctx); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

func checkpointImage(t *testing.T) []byte {
	t.Helper()
	limits := topologytest.Limits()
	builder, err := topology.NewBuilder(&limits, testStart)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	var image bytes.Buffer
	if err := builder.WriteCheckpoint(&image); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	return image.Bytes()
}

func spoolAt(position uint64) *processing.CheckpointMeta {
	return &processing.CheckpointMeta{
		Position:   processing.Position{Partition: testPartition, Sequence: position},
		ReceivedAt: testStart,
	}
}

// spool hands the store one checkpoint through the processing port, as the Pipeline does.
func spoolCheckpoint(t *testing.T, sink *Sink, position uint64, image []byte) error {
	t.Helper()
	writer, err := sink.Begin(t.Context(), spoolAt(position))
	if err != nil {
		return err
	}
	if _, err := writer.Write(image); err != nil {
		t.Fatalf("write spool: %v", err)
	}
	return writer.Close()
}

// settled waits until the store has ended as many cycles as the counts in stats say, or fails the test.
func (w *world) settled(t *testing.T, done func(stats *Stats) bool) Stats {
	t.Helper()
	deadline := time.Now().Add(settleWithin)
	for time.Now().Before(deadline) {
		stats := w.store.Stats()
		if done(&stats) {
			return stats
		}
		time.Sleep(settlePoll)
	}
	stats := w.store.Stats()
	t.Fatalf("the upload did not settle within %s: %+v", settleWithin, stats)
	return stats
}

func committedThrough(position uint64) func(stats *Stats) bool {
	return func(stats *Stats) bool {
		return stats.CommittedThrough == position
	}
}

func skippedFor(reason string) func(stats *Stats) bool {
	return func(stats *Stats) bool {
		return stats.Skipped[reason] > 0
	}
}

func failedFor(reason string) func(stats *Stats) bool {
	return func(stats *Stats) bool {
		return stats.Failed[reason] > 0
	}
}

func (w *world) ended(t *testing.T) error {
	t.Helper()
	select {
	case <-w.store.Done():
		return w.store.Err()
	case <-time.After(settleWithin):
		t.Fatalf("the store did not end within %s", settleWithin)
		return nil
	}
}

// ownObjects are the NUIDs whose chunks the bucket still holds.
func (w *world) ownObjects(t *testing.T) []string {
	t.Helper()
	var held []string
	for _, subject := range w.subjects(t) {
		if nuid, chunk := strings.CutPrefix(subject, chunkPrefix); chunk {
			held = append(held, nuid)
		}
	}
	return held
}

// ownMetas are the names of this epoch's objects whose meta the bucket still holds.
func (w *world) ownMetas(t *testing.T) []string {
	t.Helper()
	var held []string
	for _, subject := range w.subjects(t) {
		encoded, meta := strings.CutPrefix(subject, metaPrefix)
		name, err := base64.URLEncoding.DecodeString(encoded)
		if !meta || err != nil {
			continue
		}
		if epoch, _, named := parseName(string(name)); named && epoch == testEpoch {
			held = append(held, string(name))
		}
	}
	return held
}

func isChunk(f *natsjstest.Frame) bool {
	return (f.Op == "PUB" || f.Op == "HPUB") && strings.HasPrefix(f.Subject, chunkPrefix)
}

func isMeta(f *natsjstest.Frame) bool {
	return (f.Op == "PUB" || f.Op == "HPUB") && strings.HasPrefix(f.Subject, metaPrefix)
}

func isChunkCount(f *natsjstest.Frame) bool {
	return f.Subject == bucketInfo && bytes.Contains(f.Payload, []byte(`"subjects_filter":"`+chunkPrefix))
}

// assertInvariants checks the bucket against the pointer and the commits (I-a, I-b, I-c): every commit is at most
// the current position, the current object reads back whole, and the bucket holds no object of this epoch but the
// current, the previous and the ones a terminal branch kept on purpose.
func (w *world) assertInvariants(t *testing.T, retention *fakeRetention, kept int) {
	t.Helper()
	value, _ := w.pointer(t)
	if value.Current == nil {
		if len(retention.committed()) > 0 || len(w.ownObjects(t)) > kept || len(w.ownMetas(t)) > kept {
			t.Errorf("no current, commits %v, objects %v, metas %v", retention.committed(), w.ownObjects(t),
				w.ownMetas(t))
		}
		return
	}
	for _, through := range retention.committed() {
		if through > value.Current.Position {
			t.Errorf("commit through %d is past the current %d", through, value.Current.Position)
		}
	}
	w.assertReadsBack(t, value.Current)
	held := 1 + kept
	if value.Previous != nil {
		held++
	}
	if objects := w.ownObjects(t); len(objects) > held {
		t.Errorf("the bucket holds %d objects %v, want at most %d", len(objects), objects, held)
	}
	if metas := w.ownMetas(t); len(metas) > held {
		t.Errorf("the bucket holds %d metas of this epoch %v, want at most %d", len(metas), metas, held)
	}
}

func (w *world) assertReadsBack(t *testing.T, current *objectRef) {
	t.Helper()
	info, err := w.objects.GetInfo(t.Context(), current.Name)
	if err != nil {
		t.Fatalf("info of the current %s: %v", current.Name, err)
	}
	reader := w.store.readerWith(t.Context(), info, missingIsCorrupt)
	if err := errors.Join(framed.Walk(reader, topology.CheckpointSections()...), reader.Close()); err != nil {
		t.Errorf("the current %s does not read back: %v", current.Name, err)
	}
}

func (w *world) objectInfo(t *testing.T, name string) *jetstream.ObjectInfo {
	t.Helper()
	info, err := w.objects.GetInfo(t.Context(), name)
	if err != nil {
		t.Fatalf("info of %s: %v", name, err)
	}
	return info
}
