package feed

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sync"
	"sync/atomic"
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
	"github.com/DmytroLysenko1/faultline/pkg/retry"
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
	testIdleAfter     = 35 * time.Second
	neverIdle         = 1000 * time.Hour
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
		IdleAfter:    neverIdle,
	}
}

func idleOptions() *Options {
	options := testOptions()
	options.IdleAfter = testIdleAfter
	return options
}

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

func (r *fakeReader) Stop() {
	time.Sleep(r.stopTakes)
	r.stopOnce.Do(func() { close(r.stopped) })
}

func (r *fakeReader) push(steps ...step) {
	for _, next := range steps {
		r.steps <- next
	}
}

type answer struct {
	found identity
	err   error
	takes time.Duration
}

type fakeStream struct {
	lock     sync.Mutex
	answers  []answer
	calls    int
	purges   []uint64
	purgeErr error
	onInfo   func()
	atOpen   identity
	live     *identity
}

func (s *fakeStream) shrink(below uint64) {
	if s.live == nil || below <= s.live.First {
		return
	}
	removed := min(below-s.live.First, s.live.Msgs)
	average := s.live.Bytes / max(s.live.Msgs, 1)
	s.live.First, s.live.Msgs, s.live.Bytes = below, s.live.Msgs-removed, s.live.Bytes-removed*average
}

func (s *fakeStream) current() (identity, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.live == nil {
		return identity{}, false
	}
	return *s.live, true
}

func (s *fakeStream) opened() identity {
	if s.atOpen.Created.IsZero() {
		return identity{Created: streamCreated(), First: firstStreamRecord, Last: lastStreamRecord}
	}
	return s.atOpen
}

func (s *fakeStream) Purge(_ context.Context, below uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.purgeErr != nil {
		return s.purgeErr
	}
	s.purges = append(s.purges, below)
	s.shrink(below)
	return nil
}

func (s *fakeStream) purged() []uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return slices.Clone(s.purges)
}

func answering(answers ...answer) *fakeStream {
	return &fakeStream{answers: answers}
}

func (s *fakeStream) Describe(ctx context.Context) (identity, error) {
	reply := s.next()
	if s.onInfo != nil {
		s.onInfo()
	}
	if live, found := s.current(); found {
		return live, nil
	}
	if reply.takes > 0 {
		if err := retry.Sleep(ctx, reply.takes); err != nil {
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

type fakeLease struct {
	lost      chan struct{}
	loseOnce  sync.Once
	lock      sync.Mutex
	looks     int
	lostAt    int
	suspended atomic.Bool
}

func (l *fakeLease) Valid() bool {
	select {
	case <-l.lost:
		return false
	default:
		return !l.suspended.Load()
	}
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
	feed    *Feed
	reader  *fakeReader
	stream  *fakeStream
	lease   *fakeLease
	letters *fakeLetters
	journal *fakeJournal
}

func open(t *testing.T, start *Start, options *Options, stream *fakeStream) *harness {
	t.Helper()
	return reopen(t, start, options, stream, newFakeLetters())
}

func reopen(t *testing.T, start *Start, options *Options, stream *fakeStream, letters *fakeLetters) *harness {
	t.Helper()
	h := &harness{reader: newFakeReader(), stream: stream, lease: newFakeLease(), letters: letters}
	h.journal = &fakeJournal{reader: h.reader, last: start.After.Sequence}
	found := &source{
		reader:   h.reader,
		stream:   stream,
		identity: stream.opened(),
		letters:  letters,
		pulses:   h.journal,
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

type fakeLetters struct {
	lock      sync.Mutex
	stored    []deadLetter
	seen      map[string]bool
	refusals  []error
	lastFails []error
	malformed bool
	attempts  int
}

func newFakeLetters() *fakeLetters {
	return &fakeLetters{seen: make(map[string]bool)}
}

func (l *fakeLetters) Publish(_ context.Context, letter *deadLetter) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.attempts++
	if len(l.refusals) > 0 {
		refusal := l.refusals[0]
		l.refusals = l.refusals[1:]
		return refusal
	}
	if l.seen[letter.msgID()] {
		return nil
	}
	l.seen[letter.msgID()] = true
	copied := *letter
	copied.data = slices.Clone(letter.data)
	l.stored = append(l.stored, copied)
	return nil
}

func (l *fakeLetters) Last(_ context.Context, partition uint16) (lastLetter, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if len(l.lastFails) > 0 {
		failure := l.lastFails[0]
		l.lastFails = l.lastFails[1:]
		return lastLetter{}, failure
	}
	if l.malformed {
		return lastLetter{}, errMalformedLetter
	}
	for _, letter := range slices.Backward(l.stored) {
		if letter.partition == partition {
			return lastLetter{created: letter.created, window: letter.window}, nil
		}
	}
	return lastLetter{}, errNoLetter
}

func (l *fakeLetters) refuse(refusals ...error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.refusals = append(l.refusals, refusals...)
}

func (l *fakeLetters) failLast(failures ...error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.lastFails = append(l.lastFails, failures...)
}

func (l *fakeLetters) expire(sequence uint64) {
	l.lock.Lock()
	defer l.lock.Unlock()
	for _, letter := range l.stored {
		if letter.sequence == sequence {
			delete(l.seen, letter.msgID())
		}
	}
}

func (l *fakeLetters) purge() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.stored = nil
}

func (l *fakeLetters) copies() []storedLetter {
	l.lock.Lock()
	defer l.lock.Unlock()
	got := make([]storedLetter, 0, len(l.stored))
	for _, letter := range l.stored {
		got = append(got, storedLetter{
			Sequence: letter.sequence,
			Reason:   string(letter.reason),
			Low:      letter.window.low,
			High:     letter.window.high,
			Bytes:    len(letter.data),
		})
	}
	return got
}

func (l *fakeLetters) tries() int {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.attempts
}

type storedLetter struct {
	Sequence uint64
	Reason   string
	Low      uint64
	High     uint64
	Bytes    int
}

type fakeJournal struct {
	lock      sync.Mutex
	reader    *fakeReader
	last      uint64
	failures  []error
	written   []writtenPulse
	cut       int
	hidden    bool
	cutIn     func(sequence uint64, writtenAt time.Time) *message
	leaderLag time.Duration
	landed    []time.Time
}

type writtenPulse struct {
	After         uint64
	At            uint64
	WrittenAt     time.Time
	Unconditional bool
}

func (j *fakeJournal) Pulse(_ context.Context, beat *pulse) error {
	return j.write(beat, false)
}

func (j *fakeJournal) PulseUnconditionally(_ context.Context, beat *pulse) error {
	return j.write(beat, true)
}

func (j *fakeJournal) write(beat *pulse, unconditional bool) error {
	j.lock.Lock()
	defer j.lock.Unlock()
	if len(j.failures) > 0 {
		failure := j.failures[0]
		j.failures = j.failures[1:]
		return failure
	}
	if !unconditional && j.cut > 0 {
		j.cut--
		j.writeCut(beat.writtenAt)
	}
	if !unconditional && beat.after != j.last {
		return errPulseConflict
	}
	j.last++
	j.written = append(j.written, writtenPulse{After: beat.after, At: j.last, WrittenAt: beat.writtenAt,
		Unconditional: unconditional})
	data, err := record.EncodeHeartbeat(record.HeartbeatTenant, 0, beat.writtenAt)
	if err != nil {
		return err
	}
	j.landed = append(j.landed, time.Now())
	j.reader.push(step{next: &message{data: data, sequence: j.last, stamped: beat.writtenAt.Add(-j.leaderLag)}})
	return nil
}

func (j *fakeJournal) landings() []time.Time {
	j.lock.Lock()
	defer j.lock.Unlock()
	return slices.Clone(j.landed)
}

func (j *fakeJournal) ingest(t *testing.T, spans []span.Span, writtenAt time.Time) *message {
	t.Helper()
	j.lock.Lock()
	defer j.lock.Unlock()
	j.last++
	return batchAt(t, j.last, spans, writtenAt, writtenAt)
}

func (j *fakeJournal) fail(failures ...error) {
	j.lock.Lock()
	defer j.lock.Unlock()
	j.failures = append(j.failures, failures...)
}

func (j *fakeJournal) pulses() []writtenPulse {
	j.lock.Lock()
	defer j.lock.Unlock()
	return slices.Clone(j.written)
}

const (
	offsetVersion = 2
	offsetCRC     = 24
	futureVersion = record.Version + 1
	codeSealed    = 10109
)

func unknownVersionAt(t *testing.T, sequence uint64) *message {
	t.Helper()
	next := batchAt(t, sequence, spansOf(t, 0), at(0), at(0))
	next.data[offsetVersion] = futureVersion
	reseal(next.data)
	return next
}

func reseal(data []byte) {
	binary.LittleEndian.PutUint32(data[offsetCRC:], 0)
	binary.LittleEndian.PutUint32(data[offsetCRC:], crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
}

func (j *fakeJournal) writeCut(writtenAt time.Time) {
	j.last++
	cut := j.cutIn(j.last, writtenAt)
	if !j.hidden {
		j.reader.push(step{next: cut})
	}
}

func (j *fakeJournal) foreign(t *testing.T, writtenAt time.Time, pending uint64) *message {
	t.Helper()
	j.lock.Lock()
	defer j.lock.Unlock()
	j.last++
	return &message{data: heartbeatAt(t, j.last, writtenAt, writtenAt).data, sequence: j.last, stamped: writtenAt,
		pending: pending}
}

func (j *fakeJournal) ingestBehind(t *testing.T, spans []span.Span, writtenAt time.Time, pending uint64) *message {
	t.Helper()
	next := j.ingest(t, spans, writtenAt)
	next.pending = pending
	return next
}

func (j *fakeJournal) corrupt() *message {
	j.lock.Lock()
	defer j.lock.Unlock()
	j.last++
	return garbageAt(j.last)
}

func (j *fakeJournal) interfere(t *testing.T, records int, hidden bool) {
	t.Helper()
	j.lock.Lock()
	defer j.lock.Unlock()
	j.cut, j.hidden = records, hidden
	j.cutIn = func(sequence uint64, writtenAt time.Time) *message {
		return batchAt(t, sequence, spansOf(t, 0), writtenAt, writtenAt)
	}
}
