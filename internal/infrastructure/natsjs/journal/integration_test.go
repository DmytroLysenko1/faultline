package journal_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	threeNodes           = 3
	threeReplicas        = 3
	streamBytes          = 64 << 20
	connectTimeout       = 2 * time.Second
	drainTimeout         = 500 * time.Millisecond
	closeTimeout         = 5 * time.Second
	fillRecordBytes      = 1 << 20
	maxFillRecords       = 64
	answerWithin         = publishTimeout + time.Second
	storeFailed          = jetstream.ErrorCode(10077)
	maximumBytesExceeded = "maximum bytes exceeded"
)

func liveTopology(partitions int) natsjs.Topology {
	topology := topologyOf(partitions, threeReplicas)
	topology.MaxBytesPerPartition = streamBytes
	return topology
}

type liveJournal struct {
	cluster  *natsjstest.Cluster
	conn     *natsjs.Conn
	topology natsjs.Topology
	journal  *journal.Journal
}

// startJournal is the ingester's wiring on an in-process R3 cluster: Dial, EnsureTopology, the
// partition publisher, the journal — and on cleanup the journal closes before its connection.
func startJournal(tb testing.TB, topology natsjs.Topology) *liveJournal {
	tb.Helper()
	cluster := natsjstest.Start(tb, threeNodes)
	conn, err := natsjs.Dial(tb.Context(), &natsjs.ConnOptions{
		URLs: cluster.URLs(), Name: "faultline-journal-test", ConnectTimeout: connectTimeout, DrainTimeout: drainTimeout,
	})
	if err != nil {
		tb.Fatalf("Dial: %v", err)
	}
	live := &liveJournal{cluster: cluster, conn: conn, topology: topology}
	tb.Cleanup(func() { live.close(tb) })
	if err := conn.EnsureTopology(tb.Context(), topology, natsjstest.Held{}); err != nil {
		tb.Fatalf("EnsureTopology: %v", err)
	}
	publisher, err := conn.Publisher(topology)
	if err != nil {
		tb.Fatalf("Publisher: %v", err)
	}
	live.journal, err = journal.New(publisher, optionsOf(topology))
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	return live
}

func (l *liveJournal) close(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(tb.Context()), closeTimeout)
	defer cancel()
	if l.journal != nil {
		if err := l.journal.Close(ctx); err != nil {
			tb.Errorf("journal Close: %v", err)
		}
	}
	if err := l.conn.Close(ctx); err != nil {
		tb.Errorf("conn Close: %v", err)
	}
}

func (l *liveJournal) append(t *testing.T, spans []span.Span) error {
	t.Helper()
	return appendWithin(t, l.journal, callerBudget, spans)
}

func (l *liveJournal) rawJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(l.cluster.URLs()[0])
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

func (l *liveJournal) stored(t *testing.T, partition uint16) uint64 {
	t.Helper()
	stream, err := l.rawJetStream(t).Stream(t.Context(), l.topology.Stream(partition))
	if err != nil {
		t.Fatalf("stream %d: %v", partition, err)
	}
	return stream.CachedInfo().State.Msgs
}

// TestARequestIsStoredOncePerPartitionAndItsRetryIsADuplicate: the whole path on three replicas —
// OK only after every PubAck, and a client retry adds nothing.
func TestARequestIsStoredOncePerPartitionAndItsRetryIsADuplicate(t *testing.T) {
	live := startJournal(t, liveTopology(fourPartitions))
	spans := spansAcross(requestSpans, 1)

	first := live.append(t, spans)
	again := live.append(t, spans)

	if err := errors.Join(first, again); err != nil {
		t.Fatalf("Append: %v", err)
	}
	for partition := range uint16(fourPartitions) {
		if got := live.stored(t, partition); got != 1 {
			t.Errorf("partition %d holds %d records, want 1", partition, got)
		}
	}
	if diff := cmp.Diff(outcomes{Published: fourPartitions, Duplicates: fourPartitions},
		outcomesOf(live.journal)); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

// TestAFullPartitionFailsTheRequestAndTheRetryDoesNotDuplicateTheRest (FMEA 12): one partition is
// full, the other stores its record; the request is overloaded, and the client's retry of the whole
// request stores nothing twice.
func TestAFullPartitionFailsTheRequestAndTheRetryDoesNotDuplicateTheRest(t *testing.T) {
	live := startJournal(t, liveTopology(twoPartitions))
	live.fill(t, 1)
	spans := []span.Span{spanOn(0), spanOn(1)}

	first := live.append(t, spans)
	retry := live.append(t, spans)

	for _, err := range []error{first, retry} {
		if !errors.Is(err, ingest.ErrOverloaded) {
			t.Errorf("Append = %v, want ErrOverloaded", err)
		}
	}
	if got := live.stored(t, 0); got != 1 {
		t.Errorf("the healthy partition holds %d records, want 1", got)
	}
	if diff := cmp.Diff(outcomes{Published: 1, Duplicates: 1, Full: 2}, outcomesOf(live.journal)); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

// outcomes are the counters a live test can predict; latencies depend on the machine.
type outcomes struct {
	Published  uint64
	Duplicates uint64
	Full       uint64
}

func outcomesOf(journalOf *journal.Journal) outcomes {
	stats := journalOf.Stats()
	return outcomes{Published: stats.Published, Duplicates: stats.Duplicates, Full: stats.Failed[natsjs.ReasonStreamFull]}
}

// fill shrinks one partition's stream to a single message's worth and fills it with foreign bytes,
// halving them on every refusal, so not even a byte is left for a record.
func (l *liveJournal) fill(t *testing.T, partition uint16) {
	t.Helper()
	js := l.rawJetStream(t)
	stream, err := js.Stream(t.Context(), l.topology.Stream(partition))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	config := stream.CachedInfo().Config
	config.MaxBytes = natsjs.MaxMessageBytes
	if _, err := js.UpdateStream(t.Context(), config); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	size := fillRecordBytes
	for index := 0; size > 0; index++ {
		if index > maxFillRecords {
			t.Fatalf("%d records still fit into %d bytes", index, natsjs.MaxMessageBytes)
		}
		_, err := js.Publish(t.Context(), l.topology.Subject(partition), make([]byte, size),
			jetstream.WithMsgID(fmt.Sprintf("fill-%d", index)))
		if err != nil && !isFull(err) {
			t.Fatalf("fill: %v", err)
		}
		if err != nil {
			size /= 2
		}
	}
}

// TestTwoOfThreeReplicasDownIsUnavailableNotOK (FR-ING-003): without a quorum the request is
// retryable, answered within the publish budget, and never acknowledged.
func TestTwoOfThreeReplicasDownIsUnavailableNotOK(t *testing.T) {
	live := startJournal(t, liveTopology(twoPartitions))
	connected := live.conn.Publish.NATS.ConnectedServerName()
	for node, running := range live.cluster.Servers {
		if running.Name() != connected {
			live.cluster.Stop(node)
		}
	}
	started := time.Now()

	err := live.append(t, spansAcross(requestSpans, 1))

	if !errors.Is(err, ingest.ErrUnavailable) {
		t.Errorf("Append = %v, want ErrUnavailable", err)
	}
	if took := time.Since(started); took > answerWithin {
		t.Errorf("answered after %s, budget %s", took, answerWithin)
	}
	if live.journal.Stats().Published != 0 {
		t.Error("a record was acknowledged without a quorum")
	}
}

func isFull(err error) bool {
	apiErr, isAPI := errors.AsType[*jetstream.APIError](err)
	return isAPI && apiErr.ErrorCode == storeFailed && apiErr.Description == maximumBytesExceeded
}
