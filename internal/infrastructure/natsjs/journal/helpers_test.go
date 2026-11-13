package journal_test

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	testTenant     = "tenant-a"
	publishTimeout = 2 * time.Second
	rulesVersion   = 7
	onePartition   = 1
	twoPartitions  = 2
	fourPartitions = 4
	maxFootprint   = int64(1) << 40
	oneReplica     = 1
	firstVersion   = 1
	dedupWindow    = 5 * time.Minute
	reserve        = 100 * time.Millisecond
	doomedAt       = 150 * time.Millisecond
)

var errFake = errors.New("fake publish failure")

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// wallClock reads time.Now, so inside a synctest bubble the journal sees the bubble's fake time.
type wallClock struct{}

func (wallClock) Now() time.Time {
	return time.Now()
}

func topologyOf(partitions, replicas int) natsjs.Topology {
	return natsjs.Topology{
		Partitions:           partitions,
		Replicas:             replicas,
		MaxBytesPerPartition: natsjs.MaxMessageBytes,
		DuplicateWindow:      dedupWindow,
		Compression:          natsjs.CompressionNone,
		ConfigVersion:        firstVersion,
	}
}

// frozenClock stamps records and latencies with a time long gone, never the wall clock.
type frozenClock struct{}

func (frozenClock) Now() time.Time {
	return time.Unix(0, 0)
}

func optionsOf(topology natsjs.Topology) *journal.Options {
	return &journal.Options{
		Tenant:         testTenant,
		Topology:       topology,
		RulesVersion:   rulesVersion,
		PublishTimeout: publishTimeout,
		Clock:          wallClock{},
	}
}

func newJournal(t *testing.T, publisher *fakePublisher, partitions int) *journal.Journal {
	t.Helper()
	journalOf, err := journal.New(publisher, optionsOf(topologyOf(partitions, oneReplica)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return journalOf
}

func traceOf(n uint64) ids.TraceID {
	var trace ids.TraceID
	binary.BigEndian.PutUint64(trace[:8], n*0x9e3779b97f4a7c15|1)
	binary.BigEndian.PutUint64(trace[8:], n)
	return trace
}

func spanOf(trace ids.TraceID, n uint64) span.Span {
	var id ids.SpanID
	binary.BigEndian.PutUint64(id[:], n)
	return span.Span{
		Service: resource.ServiceRef{Namespace: "shop", Name: "checkout"},
		TraceID: trace,
		SpanID:  id,
	}
}

// spanOn is the first span whose trace lands on partition of two.
func spanOn(partition uint16) span.Span {
	n := uint64(1)
	for record.PartitionOf(traceOf(n), twoPartitions) != partition {
		n++
	}
	return spanOf(traceOf(n), n)
}

// spansAcross makes count spans of distinct traces, spread over the partitions by their hash.
func spansAcross(count int, first uint64) []span.Span {
	return lo.Times(count, func(index int) span.Span {
		n := first + uint64(index)
		return spanOf(traceOf(n), n)
	})
}

func spansOfPartition(spans []span.Span, partition uint16, partitions int) []span.Span {
	return lo.Filter(spans, func(observed span.Span, _ int) bool {
		return record.PartitionOf(observed.TraceID, partitions) == partition
	})
}

// answer is what the fake stream of one partition replies after waiting for after; hang waits for
// the publish context instead, the way a lost quorum looks to the client; then runs just before the
// reply.
type answer struct {
	after     time.Duration
	hang      bool
	duplicate bool
	fails     bool
	reason    natsjs.Reason
	then      func()
	plain     bool
	meaning   error
}

type call struct {
	partition uint16
	msgID     string
	data      []byte
	budget    time.Duration
	bounded   bool
}

// fakePublisher answers per partition and keeps a copy of every record: the encoder reuses its
// buffer once the publish returns.
type fakePublisher struct {
	lock    sync.Mutex
	answers map[uint16]answer
	calls   []call
}

func acking() *fakePublisher {
	return answering(map[uint16]answer{})
}

func answering(answers map[uint16]answer) *fakePublisher {
	return &fakePublisher{answers: answers}
}

func (f *fakePublisher) Publish(ctx context.Context, partition uint16, data []byte, msgID string) (bool, error) {
	reply := f.note(ctx, partition, data, msgID)
	if err := wait(ctx, reply); err != nil {
		return false, err
	}
	if reply.then != nil {
		reply.then()
	}
	if reply.plain {
		return false, errors.Join(reply.meaning, errFake)
	}
	if reply.fails {
		return false, &natsjs.PublishError{Reason: reply.reason, Cause: errFake}
	}
	return reply.duplicate, nil
}

func (f *fakePublisher) note(ctx context.Context, partition uint16, data []byte, msgID string) answer {
	deadline, bounded := ctx.Deadline()
	f.lock.Lock()
	defer f.lock.Unlock()
	f.calls = append(f.calls, call{
		partition: partition,
		msgID:     msgID,
		data:      slices.Clone(data),
		budget:    time.Until(deadline),
		bounded:   bounded,
	})
	return f.answers[partition]
}

func wait(ctx context.Context, reply answer) error {
	if !reply.hang && reply.after == 0 {
		return nil
	}
	var elapsed <-chan time.Time
	if !reply.hang {
		timer := time.NewTimer(reply.after)
		defer timer.Stop()
		elapsed = timer.C
	}
	select {
	case <-elapsed:
		return nil
	case <-ctx.Done():
		return interrupted(ctx.Err())
	}
}

// interrupted is how natsjs classifies a publish whose context ended.
func interrupted(err error) error {
	if errors.Is(err, context.Canceled) {
		return &natsjs.PublishError{Reason: natsjs.ReasonCanceled, Cause: err}
	}
	return &natsjs.PublishError{Reason: natsjs.ReasonTimeout, Cause: err}
}

func (f *fakePublisher) published() []call {
	f.lock.Lock()
	defer f.lock.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakePublisher) partitions() []uint16 {
	return slices.Sorted(slices.Values(lo.Map(f.published(), func(made call, _ int) uint16 { return made.partition })))
}

func decode(t *testing.T, made call) (record.Header, []span.Span) {
	t.Helper()
	header, err := record.ReadHeader(made.data, made.partition, maxFootprint)
	if err != nil {
		t.Fatalf("partition %d: ReadHeader: %v", made.partition, err)
	}
	spans, err := record.Decode(made.data, header)
	if err != nil {
		t.Fatalf("partition %d: Decode: %v", made.partition, err)
	}
	return header, spans
}

func appendWithin(t *testing.T, journalOf *journal.Journal, within time.Duration, spans []span.Span) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), within)
	defer cancel()
	return journalOf.Append(ctx, spans)
}
