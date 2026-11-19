package checkpoint

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	testPartition  = 0
	testEpoch      = 7
	olderEpoch     = testEpoch - 2
	testLeaseTTL   = 25 * time.Second
	testUploadRate = 64 << 20
	testWorst      = 8 << 20
	testMaxBytes   = 40 << 20
	testChunk      = 4 << 10
	testPartitions = 2
	testReplicas   = 1
	testStreamMax  = 1 << 30
	testDupWindow  = 5 * time.Minute
	testVersion    = 1
	worldTimeout   = 20 * time.Second
	imageBytes     = 3*testChunk + 17
	restoredAt     = 42
)

var (
	testStart   = time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	errInjected = errors.New("injected failure")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type storedObject struct {
	epoch          uint64
	position       uint64
	headerEpoch    uint64
	headerPosition uint64
}

func headersOf(object *storedObject) nats.Header {
	headers := nats.Header{}
	values := map[string]uint64{
		headerPosition:          cmp.Or(object.headerPosition, object.position),
		headerReceivedAt:        uint64(testStart.UnixNano()),
		headerStreamCreated:     uint64(testStart.Add(-time.Hour).UnixNano()),
		headerEpoch:             cmp.Or(object.headerEpoch, object.epoch),
		headerCheckpointVersion: 1,
		headerRecordVersion:     1,
		headerLimitsHash:        11,
		headerRulesHash:         12,
	}
	for name, value := range values {
		headers.Set(name, strconv.FormatUint(value, decimal))
	}
	return headers
}

func testOptions() *Options {
	return &Options{
		Topology: natsjs.Topology{
			Partitions:           testPartitions,
			Replicas:             testReplicas,
			MaxBytesPerPartition: testStreamMax,
			DuplicateWindow:      testDupWindow,
			Compression:          natsjs.CompressionNone,
			ConfigVersion:        testVersion,
		},
		Partition:  testPartition,
		Epoch:      testEpoch,
		LeaseTTL:   testLeaseTTL,
		UploadRate: testUploadRate,
		WorstImage: testWorst,
		MaxBytes:   testMaxBytes,
	}
}

func imageOf(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 31)
	}
	return data
}

type fakeLease struct {
	mu    sync.Mutex
	valid bool
}

func (l *fakeLease) Valid() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.valid
}

func (l *fakeLease) set(valid bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.valid = valid
}

type validTimes struct {
	mu   sync.Mutex
	left int
}

func (v *validTimes) Valid() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.left--
	return v.left >= 0
}

// world is a real JetStream: the store talks to it through the proxy, the operator writes to it directly.
type world struct {
	proxy    *natsProxy
	operator jetstream.JetStream
	kv       jetstream.KeyValue
	objects  jetstream.ObjectStore
	store    *Store
	lease    *fakeLease
	options  *Options
}

func startWorld(t *testing.T) *world {
	t.Helper()
	cluster := natsjstest.Start(t, 1)
	proxy := startProxy(t, strings.TrimPrefix(cluster.URLs()[0], "nats://"))
	options := testOptions()
	operator := connect(t, cluster.URLs()[0])
	createJournal(t, operator, options)
	ctx, cancel := context.WithTimeout(t.Context(), worldTimeout)
	defer cancel()
	proxied := connect(t, proxy.url())
	bound, err := openBuckets(ctx, proxied, proxied, options)
	if err != nil {
		t.Fatalf("open buckets: %v", err)
	}
	w := &world{proxy: proxy, operator: operator, options: options, lease: &fakeLease{valid: true}}
	w.kv, err = operator.KeyValue(ctx, PointerBucket)
	if err != nil {
		t.Fatalf("operator kv: %v", err)
	}
	w.objects, err = operator.ObjectStore(ctx, options.objectBucket())
	if err != nil {
		t.Fatalf("operator objects: %v", err)
	}
	w.store = newStore(options, w.lease, &bound, time.Now)
	return w
}

func connect(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

func createJournal(t *testing.T, js jetstream.JetStream, options *Options) {
	t.Helper()
	name := options.Topology.Stream(options.Partition)
	config := jetstream.StreamConfig{Name: name, Subjects: []string{journalSubject}}
	if _, err := js.CreateStream(t.Context(), config); err != nil {
		t.Fatalf("journal stream: %v", err)
	}
}

func (w *world) streamCreated(t *testing.T) time.Time {
	t.Helper()
	stream, err := w.operator.Stream(t.Context(), w.options.Topology.Stream(w.options.Partition))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	return stream.CachedInfo().Created
}

func (w *world) putObject(t *testing.T, epoch, position uint64, data []byte) *objectRef {
	t.Helper()
	return w.putShaped(t, &storedObject{epoch: epoch, position: position}, data)
}

func (w *world) putShaped(t *testing.T, object *storedObject, data []byte) *objectRef {
	t.Helper()
	name := strconv.FormatUint(object.epoch, decimal) + nameSeparator + strconv.FormatUint(object.position, decimal)
	info, err := w.objects.Put(t.Context(), jetstream.ObjectMeta{Name: name, Headers: headersOf(object),
		Opts: &jetstream.ObjectMetaOptions{ChunkSize: testChunk}}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
	return &objectRef{Name: info.Name, NUID: info.NUID, Digest: info.Digest, Size: info.Size, Position: object.position}
}

func (w *world) setPointer(t *testing.T, value *pointerValue) {
	t.Helper()
	if err := w.writePointer(context.WithoutCancel(t.Context()), value); err != nil {
		t.Fatalf("put pointer: %v", err)
	}
}

// putPointer is setPointer for proxy callbacks, which must not stop the proxy's goroutine.
func (w *world) putPointer(t *testing.T, value *pointerValue) {
	t.Helper()
	if err := w.writePointer(context.WithoutCancel(t.Context()), value); err != nil {
		t.Errorf("put pointer: %v", err)
	}
}

func (w *world) writePointer(ctx context.Context, value *pointerValue) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.kv.Put(ctx, w.options.pointerKey(), data)
	return err
}

func (w *world) pointer(t *testing.T) (*pointerValue, uint64) {
	t.Helper()
	entry, err := w.kv.Get(t.Context(), w.options.pointerKey())
	if err != nil {
		t.Fatalf("get pointer: %v", err)
	}
	var value pointerValue
	if err := json.Unmarshal(entry.Value(), &value); err != nil {
		t.Fatalf("unmarshal pointer: %v", err)
	}
	return &value, entry.Revision()
}

func (w *world) restorable(t *testing.T) (*objectRef, processing.CheckpointMeta) {
	t.Helper()
	ref := w.putObject(t, olderEpoch, restoredAt, imageOf(imageBytes))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch,
		StreamCreated: w.streamCreated(t).Format(createdLayout), Current: ref})
	meta, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return ref, meta
}

func isPointerWrite(f *frame) bool {
	return (f.op == "PUB" || f.op == "HPUB") && f.subject == kvSubjectPrefix+PointerBucket+"."+testOptions().pointerKey()
}

func isInfoLookup(f *frame) bool {
	return strings.HasPrefix(f.subject, "$JS.API.STREAM.MSG.GET."+objStreamPrefix) &&
		bytes.Contains(f.payload, []byte(`"last_by_subj":"`+objSubjectPrefix))
}

func isChunkRead(f *frame) bool {
	return strings.HasPrefix(f.subject, "$JS.API.STREAM.MSG.GET."+objStreamPrefix) &&
		bytes.Contains(f.payload, []byte(`"next_by_subj"`))
}
