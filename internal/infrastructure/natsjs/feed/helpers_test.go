package feed_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	singleNode      = 1
	threeNodes      = 3
	oneReplica      = 1
	threeReplicas   = 3
	streamBytes     = 1 << 30
	duplicateWindow = 2 * time.Minute
	connectTimeout  = 2 * time.Second
	drainTimeout    = 500 * time.Millisecond
	closeTimeout    = 5 * time.Second
	// stopTimeout covers nats.go stopping a consumer reset: the attempt and the sleep in progress, 20 s.
	stopTimeout     = 30 * time.Second
	publishAttempt  = 2 * time.Second
	publishAttempts = 30
	receiveLimit    = 64
	receiveWait     = 10 * time.Second
	identityWait    = 45 * time.Second
	maxReceives     = 200
	feedMaxBytes    = 64 << 20
	maxFootprint    = 6 << 20
	pullMaxBytes    = 16 << 20
	testEpoch       = 3
	tenant          = "acme"
	spansPerRecord  = 2
	serviceName     = "checkout"
	nodeNamePrefix  = "n"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func topologyOf(replicas int) natsjs.Topology {
	return natsjs.Topology{
		Partitions:           1,
		Replicas:             replicas,
		MaxBytesPerPartition: streamBytes,
		DuplicateWindow:      duplicateWindow,
		Compression:          natsjs.CompressionNone,
		ConfigVersion:        1,
	}
}

func optionsOf(topology natsjs.Topology) *feed.Options {
	return &feed.Options{
		Topology:     topology,
		Epoch:        testEpoch,
		MaxBytes:     feedMaxBytes,
		MaxFootprint: maxFootprint,
		PullMaxBytes: pullMaxBytes,
	}
}

// heldLease is a partition lease that is never lost, for tests that are not about the lease.
type heldLease struct {
	lost chan struct{}
}

func newHeldLease() *heldLease {
	return &heldLease{lost: make(chan struct{})}
}

func (l *heldLease) Lost() <-chan struct{} {
	return l.lost
}

type live struct {
	cluster  *natsjstest.Cluster
	conn     *natsjs.Conn
	raw      jetstream.JetStream
	topology natsjs.Topology
	encoder  record.Encoder
}

// startLive is the processor's wiring on an in-process cluster with the partition's stream created; raw is
// a connection of the test's own for the writes and the stream surgery the feed must notice.
func startLive(t testing.TB, nodes, replicas int) *live {
	t.Helper()
	cluster := natsjstest.Start(t, nodes)
	conn, err := natsjs.Dial(t.Context(), &natsjs.ConnOptions{
		URLs: cluster.URLs(), Name: t.Name(), ConnectTimeout: connectTimeout, DrainTimeout: drainTimeout,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { closeConn(t, conn) })
	l := &live{cluster: cluster, conn: conn, raw: rawJetStream(t, cluster), topology: topologyOf(replicas)}
	l.ensure(t)
	return l
}

func closeConn(t testing.TB, conn *natsjs.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func rawJetStream(t testing.TB, cluster *natsjstest.Cluster) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(strings.Join(cluster.URLs(), ","))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

func (l *live) ensure(t testing.TB) {
	t.Helper()
	if err := l.conn.EnsureTopology(t.Context(), l.topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
}

func (l *live) stream() string {
	return l.topology.Stream(0)
}

func (l *live) info(t testing.TB) *jetstream.StreamInfo {
	t.Helper()
	stream, err := l.raw.Stream(t.Context(), l.stream())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	return stream.CachedInfo()
}

// recreate deletes the partition's stream and creates it anew, as an operator's mistake would.
func (l *live) recreate(t *testing.T) {
	t.Helper()
	if err := l.raw.DeleteStream(t.Context(), l.stream()); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}
	l.ensure(t)
}

func (l *live) purgeBelow(t *testing.T, sequence uint64) {
	t.Helper()
	stream, err := l.raw.Stream(t.Context(), l.stream())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if err := stream.Purge(t.Context(), jetstream.WithPurgeSequence(sequence)); err != nil {
		t.Fatalf("Purge below %d: %v", sequence, err)
	}
}

func spansOf(t testing.TB, firstTrace uint64) []span.Span {
	t.Helper()
	spans := make([]span.Span, 0, spansPerRecord)
	for offset := range uint64(spansPerRecord) {
		spans = append(spans, telemetrytest.NumberedSpan(t, kind.Client, firstTrace+offset, serviceName))
	}
	return spans
}

// publish writes one record of spans as the journal does.
func (l *live) publish(t testing.TB, spans []span.Span) {
	t.Helper()
	err := l.encoder.Encode(tenant, 0, spans, time.Now(), 0, func(data []byte, msgID string) error {
		return l.publishData(t, data, msgID)
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// publishData retries a write that found no leader yet, as after a leader stopped; each attempt is bounded.
func (l *live) publishData(t testing.TB, data []byte, msgID string) error {
	t.Helper()
	var failure error
	for range publishAttempts {
		if failure = l.publishOnce(t, data, msgID); failure == nil {
			return nil
		}
	}
	return fmt.Errorf("after %d attempts: %w", publishAttempts, failure)
}

func (l *live) publishOnce(t testing.TB, data []byte, msgID string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), publishAttempt)
	defer cancel()
	_, err := l.raw.Publish(ctx, l.topology.Subject(0), data, jetstream.WithMsgID(msgID))
	return err
}

// weightOf is the credit a record of spans takes: the weight its header carries.
func weightOf(t testing.TB, spans []span.Span) int64 {
	t.Helper()
	var weight int64
	err := new(record.Encoder).Encode(tenant, 0, spans, time.Now(), 0, func(data []byte, _ string) error {
		header, err := record.ReadHeader(data, 0, maxFootprint)
		weight = header.Weight
		return err
	})
	if err != nil {
		t.Fatalf("weigh: %v", err)
	}
	return weight
}

func (l *live) publishMany(t testing.TB, count int, firstTrace uint64) [][]span.Span {
	t.Helper()
	written := make([][]span.Span, 0, count)
	for index := range count {
		spans := spansOf(t, firstTrace+uint64(index*spansPerRecord))
		l.publish(t, spans)
		written = append(written, spans)
	}
	return written
}

// open opens the partition's feed and closes it on cleanup before the connection closes.
func (l *live) open(t testing.TB, start feed.Start, options *feed.Options) *feed.Feed {
	t.Helper()
	opened, err := feed.Open(t.Context(), l.conn, newHeldLease(), start, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { shut(t, opened) })
	return opened
}

func shut(t testing.TB, opened *feed.Feed) {
	t.Helper()
	opened.Close()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), stopTimeout)
	defer cancel()
	if err := opened.Wait(ctx); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

// receive takes count data deliveries, releasing each, or fails the test.
func receive(t *testing.T, opened *feed.Feed, count int) []processing.Delivery {
	t.Helper()
	var got []processing.Delivery
	if count == 0 {
		return got
	}
	for range maxReceives {
		deliveries, err := opened.Receive(t.Context(), receiveLimit, receiveWait)
		if err != nil {
			t.Fatalf("Receive after %d of %d: %v", len(got), count, err)
		}
		got = append(got, released(deliveries)...)
		if len(got) >= count {
			return got
		}
	}
	t.Fatalf("%d of %d deliveries after %d receives", len(got), count, maxReceives)
	return nil
}

// receiveUntilStopped hands out deliveries until the feed stops, and returns what came and why it stopped.
func receiveUntilStopped(t *testing.T, opened *feed.Feed) ([]processing.Delivery, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), identityWait)
	defer cancel()
	var got []processing.Delivery
	for {
		deliveries, err := opened.Receive(ctx, receiveLimit, receiveWait)
		if err != nil {
			return got, err
		}
		got = append(got, released(deliveries)...)
	}
}

func released(deliveries []processing.Delivery) []processing.Delivery {
	for _, delivery := range deliveries {
		delivery.Release()
	}
	return slices.DeleteFunc(deliveries, func(delivery processing.Delivery) bool {
		return delivery.Batch().Heartbeat
	})
}

func sequencesOf(deliveries []processing.Delivery) []uint64 {
	sequences := make([]uint64, 0, len(deliveries))
	for _, delivery := range deliveries {
		sequences = append(sequences, delivery.Position().Sequence)
	}
	return sequences
}

func spansIn(deliveries []processing.Delivery) [][]span.Span {
	spans := make([][]span.Span, 0, len(deliveries))
	for _, delivery := range deliveries {
		spans = append(spans, delivery.Batch().Spans)
	}
	return spans
}

func consecutive(first uint64, count int) []uint64 {
	sequences := make([]uint64, 0, count)
	for offset := range uint64(count) {
		sequences = append(sequences, first+offset)
	}
	return sequences
}

func stoppedBy(err, want error) bool {
	return errors.Is(err, want)
}

// consumerNode is the index of the node that holds the feed's consumer, the only one on the stream.
func (l *live) consumerNode(t *testing.T) int {
	t.Helper()
	stream, err := l.raw.Stream(t.Context(), l.stream())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	names := stream.ConsumerNames(t.Context())
	var host string
	for name := range names.Name() {
		consumer, err := stream.Consumer(t.Context(), name)
		if err != nil {
			t.Fatalf("Consumer %s: %v", name, err)
		}
		host = consumer.CachedInfo().Cluster.Leader
	}
	var node int
	if _, err := fmt.Sscanf(host, nodeNamePrefix+"%d", &node); err != nil {
		t.Fatalf("consumer node %q is not a test node: %v", host, err)
	}
	return node
}
