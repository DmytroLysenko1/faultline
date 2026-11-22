package checkpoint

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const natsResetPendingAcks = "github.com/nats-io/nats.go/jetstream.(*jetStream).resetPendingAcksOnReconnect"

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
	testSpool      = "spool"
)

var (
	testStart   = time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	errInjected = errors.New("injected failure")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction(natsResetPendingAcks))
}

type storedObject struct {
	partition      uint64
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
		headerPartition:         object.partition,
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
		SpoolDir:   testSpool,
	}
}

func idleStats() Stats {
	return Stats{
		Epoch:          testEpoch,
		BucketMaxBytes: testMaxBytes,
		Skipped: map[string]uint64{"busy": 0, "lease_suspended": 0, "bucket_full": 0, "retention_suspended": 0,
			"stopped": 0, "not_started": 0, "final_skipped": 0},
		Failed: map[string]uint64{"upload": 0, "chunk_count": 0, "verify": 0, "cas": 0, "position_regressed": 0,
			"name_collision": 0, "commit": 0, "bucket_read": 0, "bucket_unresolved": 0},
		UploadFailed: map[string]uint64{"other": 0, "bucket_full": 0, "sealed": 0, "too_large": 0, "timeout": 0,
			"foreign_stream": 0, "account_full": 0, "insufficient_resources": 0},
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

type world struct {
	requested atomic.Int64
	proxy     *natsjstest.Proxy
	operator  jetstream.JetStream
	kv        jetstream.KeyValue
	objects   jetstream.ObjectStore
	store     *Store
	lease     *fakeLease
	options   *Options
}

func startWorld(t *testing.T) *world {
	t.Helper()
	return startWorldOn(t, testReplicas)
}

func startWorldOn(t *testing.T, nodes int) *world {
	t.Helper()
	options := testOptions()
	options.Topology.Replicas = nodes
	return startWorldWith(t, options, dedupWindow)
}

func startWorldWith(t *testing.T, options *Options, window time.Duration) *world {
	t.Helper()
	if err := options.validate(window); err != nil {
		t.Fatalf("options for a %s window: %v", window, err)
	}
	cluster := natsjstest.Start(t, options.Topology.Replicas)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	options.SpoolDir = t.TempDir()
	operator := connect(t, cluster.URLs()[0])
	createJournal(t, operator, options)
	ctx, cancel := context.WithTimeout(t.Context(), worldTimeout)
	defer cancel()
	proxied := connect(t, proxy.URL())
	bound, err := openBuckets(ctx, proxied, proxied, options, window)
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
	natsjstest.Eventually(t, "journal stream "+name, func(ctx context.Context) error {
		_, err := js.CreateStream(ctx, config)
		return err
	})
	natsjstest.AwaitStreamLeader(t, js, name)
}

func (w *world) streamCreated(t *testing.T) time.Time {
	t.Helper()
	return streamCreatedAt(t, w.operator, w.options.Topology.Stream(w.options.Partition))
}

func streamCreatedAt(t *testing.T, js jetstream.JetStream, name string) time.Time {
	t.Helper()
	var created time.Time
	natsjstest.Eventually(t, "the info of stream "+name, func(ctx context.Context) error {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			return err
		}
		created = stream.CachedInfo().Created
		return nil
	})
	return created
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

func isPointerWrite(f *natsjstest.Frame) bool {
	return (f.Op == "PUB" || f.Op == "HPUB") && f.Subject == kvSubjectPrefix+PointerBucket+"."+testOptions().pointerKey()
}

func isInfoLookup(f *natsjstest.Frame) bool {
	return strings.HasPrefix(f.Subject, "$JS.API.STREAM.MSG.GET."+objStreamPrefix) &&
		bytes.Contains(f.Payload, []byte(`"last_by_subj":"`+objSubjectPrefix))
}

func isChunkRead(f *natsjstest.Frame) bool {
	return strings.HasPrefix(f.Subject, "$JS.API.STREAM.MSG.GET."+objStreamPrefix) &&
		bytes.Contains(f.Payload, []byte(`"next_by_subj"`))
}

func latestErr(_ processing.CheckpointMeta, reader io.ReadCloser, err error) error {
	if reader == nil {
		return err
	}
	return errors.Join(err, reader.Close())
}

func closeOnCleanup(t *testing.T, reader io.ReadCloser) {
	t.Helper()
	if reader == nil {
		return
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close the latest reader: %v", err)
		}
	})
}
