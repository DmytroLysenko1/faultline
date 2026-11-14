package feed

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	testTenant        = "acme"
	testEpoch         = 7
	testMaxBytes      = 64 << 20
	testMaxFootprint  = 6 << 20
	testPullMaxBytes  = 16 << 20
	testStreamBytes   = 1 << 30
	testWindow        = 2 * time.Minute
	firstConsumer     = "fl-p0-e7_1"
	secondConsumer    = "fl-p0-e7_2"
	queuedSteps       = 64
	spansPerRecord    = 2
	serviceName       = "checkout"
	lastStreamRecord  = 1000
	firstStreamRecord = 1
)

func streamCreated() time.Time {
	return telemetrytest.Epoch().Add(-time.Hour)
}

func recreated() time.Time {
	return telemetrytest.Epoch().Add(-time.Minute)
}

func testOptions() *Options {
	return &Options{
		Topology: natsjs.Topology{
			Partitions:           1,
			Replicas:             1,
			MaxBytesPerPartition: testStreamBytes,
			DuplicateWindow:      testWindow,
			ConfigVersion:        1,
		},
		Epoch:        testEpoch,
		MaxBytes:     testMaxBytes,
		MaxFootprint: testMaxFootprint,
		PullMaxBytes: testPullMaxBytes,
	}
}

// step is one return of the consumer's Next: a record, a timeout, an error, or a reset that holds Next
// until Stop, as nats.go does while it recreates the consumer. A rename happens inside Next, like a reset.
type step struct {
	rename string
	next   *message
	err    error
	hang   bool
}

type fakeReader struct {
	steps     chan step
	lock      sync.Mutex
	name      string
	stopped   chan struct{}
	stopOnce  sync.Once
	stopTakes time.Duration
	lingers   time.Duration
}

func newFakeReader() *fakeReader {
	return &fakeReader{
		steps:   make(chan step, queuedSteps),
		name:    firstConsumer,
		stopped: make(chan struct{}),
	}
}

func (r *fakeReader) Next(wait time.Duration) (message, error) {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case next := <-r.steps:
		return r.take(next)
	case <-timer.C:
		return message{}, nats.ErrTimeout
	case <-r.stopped:
		return message{}, jetstream.ErrMsgIteratorClosed
	}
}

func (r *fakeReader) take(next step) (message, error) {
	if next.rename != "" {
		r.lock.Lock()
		r.name = next.rename
		r.lock.Unlock()
	}
	switch {
	case next.hang:
		<-r.stopped
		time.Sleep(r.lingers)
		return message{}, jetstream.ErrMsgIteratorClosed
	case next.next != nil:
		return *next.next, nil
	case next.err != nil:
		return message{}, next.err
	}
	return message{}, nats.ErrTimeout
}

func (r *fakeReader) Consumer() string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.name
}

// Stop takes stopTakes, as nats.go's Stop waits for the reset attempt and sleep in progress; a Next held by
// a reset returns lingers after Stop.
func (r *fakeReader) Stop() {
	time.Sleep(r.stopTakes)
	r.stopOnce.Do(func() { close(r.stopped) })
}

func (r *fakeReader) push(steps ...step) {
	for _, next := range steps {
		r.steps <- next
	}
}

// answer is one reply of the stream's Info; takes delays it and an attempt deadline cuts it short.
type answer struct {
	found identity
	err   error
	takes time.Duration
}

type fakeStream struct {
	lock    sync.Mutex
	answers []answer
	calls   int
}

func answering(answers ...answer) *fakeStream {
	return &fakeStream{answers: answers}
}

func (s *fakeStream) Describe(ctx context.Context) (identity, error) {
	reply := s.next()
	if reply.takes > 0 {
		if err := pause(ctx, reply.takes); err != nil {
			return identity{}, err
		}
	}
	return reply.found, reply.err
}

func (s *fakeStream) next() answer {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.calls++
	if len(s.answers) == 0 {
		return answer{found: identity{Created: streamCreated()}}
	}
	reply := s.answers[0]
	if len(s.answers) > 1 {
		s.answers = s.answers[1:]
	}
	return reply
}

func (s *fakeStream) described() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.calls
}

// fakeLease is lost on lose, or from its lostAt-th look at Lost on, to land a loss between two checks.
type fakeLease struct {
	lost     chan struct{}
	loseOnce sync.Once
	lock     sync.Mutex
	looks    int
	lostAt   int
}

func newFakeLease() *fakeLease {
	return &fakeLease{lost: make(chan struct{})}
}

func (l *fakeLease) Lost() <-chan struct{} {
	l.lock.Lock()
	l.looks++
	due := l.lostAt > 0 && l.looks >= l.lostAt
	l.lock.Unlock()
	if due {
		l.lose()
	}
	return l.lost
}

func (l *fakeLease) lose() {
	l.loseOnce.Do(func() { close(l.lost) })
}

type harness struct {
	feed   *Feed
	reader *fakeReader
	stream *fakeStream
	lease  *fakeLease
}

func open(t *testing.T, start *Start, options *Options, stream *fakeStream) *harness {
	t.Helper()
	h := &harness{reader: newFakeReader(), stream: stream, lease: newFakeLease()}
	found := &source{
		reader:   h.reader,
		stream:   stream,
		identity: identity{Created: streamCreated(), First: firstStreamRecord, Last: lastStreamRecord},
	}
	h.feed = newFeed(t.Context(), h.lease, found, start, options)
	t.Cleanup(func() { h.shut(t) })
	return h
}

func openPlain(t *testing.T) *harness {
	t.Helper()
	return open(t, &Start{}, testOptions(), answering())
}

func (h *harness) shut(t *testing.T) {
	t.Helper()
	h.feed.Close()
	if err := h.feed.Wait(context.WithoutCancel(t.Context())); err != nil {
		t.Errorf("Wait: %v", err)
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

// batchAt is a record as the journal writes it, read at sequence with the leader's stamp.
func batchAt(t *testing.T, sequence uint64, spans []span.Span, writtenAt, stamped time.Time) *message {
	t.Helper()
	return batchOf(t, 0, sequence, spans, writtenAt, stamped)
}

func batchOf(t *testing.T, partition uint16, sequence uint64, spans []span.Span, writtenAt, stamped time.Time) *message {
	t.Helper()
	var data []byte
	err := new(record.Encoder).Encode(testTenant, partition, spans, writtenAt, 0, func(made []byte, _ string) error {
		data = slices.Clone(made)
		return nil
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return &message{data: data, sequence: sequence, stamped: stamped}
}

func heartbeatAt(t *testing.T, sequence uint64, writtenAt, stamped time.Time) *message {
	t.Helper()
	data, err := record.EncodeHeartbeat(testTenant, 0, writtenAt)
	if err != nil {
		t.Fatalf("EncodeHeartbeat: %v", err)
	}
	return &message{data: data, sequence: sequence, stamped: stamped}
}

func weightOf(t *testing.T, next *message) int64 {
	t.Helper()
	header, err := record.ReadHeader(next.data, 0, testMaxFootprint)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	return header.Weight
}

func at(offset time.Duration) time.Time {
	return telemetrytest.Epoch().Add(offset)
}

// received is what Receive handed out, by position, heartbeat and ReceivedAt.
type received struct {
	Sequence   uint64
	Heartbeat  bool
	ReceivedAt time.Time
	Spans      int
}

func summarize(deliveries []processing.Delivery) []received {
	got := make([]received, 0, len(deliveries))
	for _, delivery := range deliveries {
		got = append(got, received{
			Sequence:   delivery.Position().Sequence,
			Heartbeat:  delivery.Batch().Heartbeat,
			ReceivedAt: delivery.Batch().ReceivedAt,
			Spans:      len(delivery.Batch().Spans),
		})
	}
	return got
}
