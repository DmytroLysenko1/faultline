package checkpoint_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	connectTimeout = 2 * time.Second
	drainTimeout   = time.Second
	closeTimeout   = 5 * time.Second
	singleNode     = 1
	threeNodes     = 3
	cliChunk       = 128 << 10
	bigImage       = 6 << 20
	smallImage     = 300 << 10
	worstImage     = 8 << 20
	bucketBytes    = 40 << 20
	grownBucket    = 48 << 20
	uploadRate     = 64 << 20
	leaseTTL       = 25 * time.Second
	ownEpoch       = 7
	position       = 42
	stepdownWait   = 5 * time.Second
	bindWithin     = 20 * time.Second
	bindAttempt    = 2 * time.Second
)

type live struct {
	cluster *natsjstest.Cluster
	conn    *natsjs.Conn
	raw     *nats.Conn
	js      jetstream.JetStream
	options *checkpoint.Options
}

func startLive(t *testing.T, nodes int) *live {
	t.Helper()
	cluster := natsjstest.Start(t, nodes)
	conn, err := natsjs.Dial(t.Context(), &natsjs.ConnOptions{URLs: cluster.URLs(), Name: t.Name(),
		ConnectTimeout: connectTimeout, DrainTimeout: drainTimeout})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeTimeout)
		defer cancel()
		if err := conn.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	raw, err := nats.Connect(strings.Join(cluster.URLs(), ","))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(raw.Close)
	js, err := jetstream.New(raw)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	options := liveOptions(nodes)
	options.SpoolDir = t.TempDir()
	return &live{cluster: cluster, conn: conn, raw: raw, js: js, options: options}
}

func liveOptions(replicas int) *checkpoint.Options {
	return &checkpoint.Options{
		Topology: natsjs.Topology{
			Partitions:           1,
			Replicas:             replicas,
			MaxBytesPerPartition: 1 << 30,
			DuplicateWindow:      5 * time.Minute,
			Compression:          natsjs.CompressionNone,
			ConfigVersion:        1,
		},
		Epoch:      ownEpoch,
		LeaseTTL:   leaseTTL,
		UploadRate: uploadRate,
		WorstImage: worstImage,
		MaxBytes:   bucketBytes,
	}
}

func (l *live) open(t *testing.T) *checkpoint.Store {
	t.Helper()
	store, err := checkpoint.Open(t.Context(), l.conn, natsjstest.Held{}, l.options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store
}

func (l *live) ensureJournal(t *testing.T) {
	t.Helper()
	if err := l.conn.EnsureTopology(t.Context(), l.options.Topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
}

func (l *live) created(t *testing.T) time.Time {
	t.Helper()
	stream, err := l.js.Stream(t.Context(), l.options.Topology.Stream(0))
	if err != nil {
		t.Fatalf("journal stream: %v", err)
	}
	return stream.CachedInfo().Created
}

func (l *live) putWithCLI(t *testing.T, name string, image []byte, meta *processing.CheckpointMeta) *jetstream.ObjectInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), bindWithin)
	defer cancel()
	store, err := l.js.ObjectStore(ctx, "faultline_checkpoints_p0")
	if err != nil {
		t.Fatalf("object store: %v", err)
	}
	info, err := store.Put(ctx, jetstream.ObjectMeta{Name: name, Headers: metaHeaders(meta),
		Opts: &jetstream.ObjectMetaOptions{ChunkSize: cliChunk}}, bytes.NewReader(image))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return info
}

func metaHeaders(meta *processing.CheckpointMeta) nats.Header {
	headers := nats.Header{}
	for name, value := range map[string]uint64{
		"Checkpoint-Position":       meta.Position.Sequence,
		"Checkpoint-Received-At":    uint64(meta.ReceivedAt.UnixNano()),
		"Checkpoint-Stream-Created": uint64(meta.StreamCreated.UnixNano()),
		"Checkpoint-Epoch":          meta.Epoch,
		"Checkpoint-Format-Version": uint64(meta.CheckpointVersion),
		"Checkpoint-Record-Version": uint64(meta.RecordVersion),
		"Checkpoint-Limits-Hash":    meta.LimitsHash,
		"Checkpoint-Rules-Hash":     uint64(meta.RulesHash),
		"Checkpoint-Partition":      uint64(meta.Position.Partition),
	} {
		headers.Set(name, strconv.FormatUint(value, 10))
	}
	return headers
}

type runbookObject struct {
	Name     string `json:"name"`
	NUID     string `json:"nuid"`
	Digest   string `json:"digest"`
	Size     uint64 `json:"size"`
	Position uint64 `json:"position"`
}

type runbookPointer struct {
	V             int            `json:"v"`
	Epoch         uint64         `json:"epoch"`
	StreamCreated string         `json:"stream_created"`
	Current       *runbookObject `json:"current"`
	Previous      *runbookObject `json:"previous"`
}

func (l *live) raisePointer(t *testing.T, info *jetstream.ObjectInfo, created time.Time) {
	t.Helper()
	kv := l.pointerBucket(t)
	value, err := json.Marshal(runbookPointer{V: 1, Epoch: 1, StreamCreated: created.UTC().Format(time.RFC3339Nano),
		Current: &runbookObject{Name: info.Name, NUID: info.NUID, Digest: info.Digest, Size: info.Size, Position: position}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := kv.Create(t.Context(), "p0", value); err != nil {
		t.Fatalf("kv create: %v", err)
	}
}

func (l *live) pointerBucket(t *testing.T) jetstream.KeyValue {
	t.Helper()
	deadline := time.Now().Add(bindWithin)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), bindAttempt)
		kv, err := l.js.KeyValue(ctx, checkpoint.PointerBucket)
		cancel()
		if err == nil {
			return kv
		}
		if time.Now().After(deadline) {
			t.Fatalf("pointer bucket after %s: %v", bindWithin, err)
		}
	}
}

func checkpointMeta(created time.Time) processing.CheckpointMeta {
	return processing.CheckpointMeta{
		Position:          processing.Position{Sequence: position},
		ReceivedAt:        time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC),
		StreamCreated:     created,
		Epoch:             1,
		CheckpointVersion: 1,
		RecordVersion:     1,
		LimitsHash:        11,
		RulesHash:         12,
	}
}

func image(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/251)
	}
	return data
}

func TestOpenCreatesBucketsOnlyTheLeaderAnswersFor(t *testing.T) {
	l := startLive(t, singleNode)

	l.open(t)

	for _, name := range []string{"KV_faultline_checkpoint_pointers", "OBJ_faultline_checkpoints_p0"} {
		stream, err := l.js.Stream(t.Context(), name)
		if err != nil {
			t.Fatalf("stream %s: %v", name, err)
		}
		config := stream.CachedInfo().Config
		got := []any{config.AllowDirect, config.Storage, config.Duplicates, config.Replicas, config.Discard}
		want := []any{false, jetstream.FileStorage, 2 * time.Minute, singleNode, jetstream.DiscardNew}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%s config (-want +got):\n%s", name, diff)
		}
	}
}

func TestTwoProcessorsOpeningAtOnceBothGetTheBuckets(t *testing.T) {
	l := startLive(t, singleNode)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := checkpoint.Open(t.Context(), l.conn, natsjstest.Held{}, l.options)
			errs <- err
		}()
	}

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Open: %v", err)
		}
	}
}

func TestABucketThatServesDirectReadsIsRefused(t *testing.T) {
	l := startLive(t, singleNode)
	if _, err := l.js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "faultline_checkpoints_p0",
		MaxBytes: bucketBytes, Storage: jetstream.FileStorage}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	_, err := checkpoint.Open(t.Context(), l.conn, natsjstest.Held{}, l.options)

	if !errors.Is(err, checkpoint.ErrBucketDrift) {
		t.Fatalf("Open: err = %v, want %v", err, checkpoint.ErrBucketDrift)
	}
}

func TestTheBucketOnlyGrows(t *testing.T) {
	tests := []struct {
		name  string
		sizes []int64
		want  int64
	}{
		{name: "a larger bucket grows it", sizes: []int64{bucketBytes, grownBucket}, want: grownBucket},
		{name: "a smaller bucket keeps it", sizes: []int64{grownBucket, bucketBytes}, want: grownBucket},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := startLive(t, singleNode)
			for _, size := range tt.sizes {
				l.options.MaxBytes = size
				l.open(t)
			}

			stream, err := l.js.Stream(t.Context(), "OBJ_faultline_checkpoints_p0")
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			if got := stream.CachedInfo().Config.MaxBytes; got != tt.want {
				t.Fatalf("bucket MaxBytes = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestStakingOnTheRealBucketPurgesAStaleObjectOfAnOlderEpoch(t *testing.T) {
	l := startLive(t, singleNode)
	l.ensureJournal(t)
	store := l.open(t)
	created := l.created(t)
	meta := checkpointMeta(created)
	l.raisePointer(t, l.putWithCLI(t, "1-42", image(smallImage), &meta), created)
	stale := checkpointMeta(created)
	l.putWithCLI(t, "1-30", image(smallImage), &stale)
	restored, reader, err := store.Latest(t.Context(), 0)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := store.Stake(t.Context(), &restored); err != nil {
		t.Fatalf("Stake: %v", err)
	}

	objects, err := l.js.ObjectStore(t.Context(), "faultline_checkpoints_p0")
	if err != nil {
		t.Fatalf("object store: %v", err)
	}
	if _, err := objects.GetInfo(t.Context(), "1-30"); !errors.Is(err, jetstream.ErrObjectNotFound) {
		t.Errorf("stale object 1-30: err = %v, want %v", err, jetstream.ErrObjectNotFound)
	}
	if _, err := objects.GetInfo(t.Context(), "1-42"); err != nil {
		t.Errorf("current object 1-42: %v", err)
	}
	if got := store.Stats(); got.SweptObjects != 1 {
		t.Errorf("swept %d objects, want 1", got.SweptObjects)
	}
}

func TestACheckpointRaisedByTheRunbookIsRestoredAndStaked(t *testing.T) {
	l := startLive(t, singleNode)
	l.ensureJournal(t)
	store := l.open(t)
	created := l.created(t)
	want := image(smallImage)
	meta := checkpointMeta(created)
	l.raisePointer(t, l.putWithCLI(t, "1-42", want, &meta), created)

	gotMeta, reader, err := store.Latest(t.Context(), 0)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	got := new(bytes.Buffer)
	_, readErr := got.ReadFrom(reader)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		t.Fatalf("read: %v", err)
	}
	staked, err := store.Stake(t.Context(), &gotMeta)

	if err != nil || !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("Stake: %v, read %d bytes of %d", err, got.Len(), len(want))
	}
	if diff := cmp.Diff(processing.Staked{After: meta.Position, ReceivedAt: meta.ReceivedAt, Created: created},
		staked); diff != "" {
		t.Errorf("staked (-want +got):\n%s", diff)
	}
}

func TestAReadThroughALeaderChangeOfTheBucketStillRestores(t *testing.T) {
	l := startLive(t, threeNodes)
	l.ensureJournal(t)
	store := l.open(t)
	created := l.created(t)
	want := image(bigImage)
	meta := checkpointMeta(created)
	l.raisePointer(t, l.putWithCLI(t, "1-42", want, &meta), created)
	_, reader, err := store.Latest(t.Context(), 0)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()
	first := make([]byte, cliChunk)
	if _, err := reader.Read(first); err != nil {
		t.Fatalf("first read: %v", err)
	}

	if _, err := l.raw.Request("$JS.API.STREAM.LEADER.STEPDOWN.OBJ_faultline_checkpoints_p0", nil, stepdownWait); err != nil {
		t.Fatalf("stepdown: %v", err)
	}
	rest := new(bytes.Buffer)
	_, err = rest.ReadFrom(reader)

	got := slices.Concat(first, rest.Bytes())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read across the stepdown: err = %v, %d bytes of %d", err, len(first)+rest.Len(), len(want))
	}
}

func TestStakeEmptyAndDiscardWorkOnTheRealBucket(t *testing.T) {
	l := startLive(t, singleNode)
	l.ensureJournal(t)
	store := l.open(t)
	if _, _, err := store.Latest(t.Context(), 0); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest: %v, want %v", err, processing.ErrNoCheckpoint)
	}

	if _, err := store.StakeEmpty(t.Context()); err != nil {
		t.Fatalf("StakeEmpty: %v", err)
	}

	next := l.open(t)
	if _, _, err := next.Latest(t.Context(), 0); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest after StakeEmpty: %v, want %v", err, processing.ErrNoCheckpoint)
	}
}

func TestDiscardOnTheRealBucketEmptiesThePointerAndKeepsThePreviousObject(t *testing.T) {
	l := startLive(t, singleNode)
	l.ensureJournal(t)
	store := l.open(t)
	created := l.created(t)
	meta := checkpointMeta(created)
	info := l.putWithCLI(t, "1-42", image(smallImage), &meta)
	l.raisePointer(t, info, created)
	_, reader, err := store.Latest(t.Context(), 0)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err = store.Discard(t.Context(), processing.DiscardTarget{Partition: 0, Checkpoint: info.NUID})

	if err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if _, _, err := l.open(t).Latest(t.Context(), 0); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest after Discard: %v, want %v", err, processing.ErrNoCheckpoint)
	}
	objects, err := l.js.ObjectStore(t.Context(), "faultline_checkpoints_p0")
	if err != nil {
		t.Fatalf("object store: %v", err)
	}
	if _, err := objects.GetInfo(t.Context(), "1-42"); err != nil {
		t.Errorf("the discarded object is gone before the first commit: %v", err)
	}
}

func TestLatestOverADeletedObjectBucketIsLostState(t *testing.T) {
	l := startLive(t, singleNode)
	l.ensureJournal(t)
	store := l.open(t)
	created := l.created(t)
	meta := checkpointMeta(created)
	l.raisePointer(t, l.putWithCLI(t, "1-42", image(smallImage), &meta), created)
	if err := l.js.DeleteStream(t.Context(), "OBJ_faultline_checkpoints_p0"); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	_, _, err := store.Latest(t.Context(), 0)

	if !errors.Is(err, processing.ErrCheckpointUnusable) {
		t.Fatalf("Latest: err = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
}
