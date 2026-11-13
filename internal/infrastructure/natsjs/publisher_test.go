package natsjs_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var (
	notAnAck   = []byte("not json")
	foreignAck = []byte(`{"stream":"SOMEONE_ELSE","seq":1}`)
)

const (
	fillRecordBytes = 1 << 20
	maxFillRecords  = 64
	answerWithin    = publishBudget + time.Second
)

func msgID(index int) string {
	return fmt.Sprintf("%032d", index)
}

func TestARepeatedRecordIsAcknowledgedAsADuplicate(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(twoPartitions, threeReplicas)
	publisher := publisherOf(t, ensured(t, cluster, topology), topology)
	data := make([]byte, smallRecordBytes)

	first, firstErr := publishOnce(t, publisher, secondPartition, data, msgID(1))
	again, againErr := publishOnce(t, publisher, secondPartition, data, msgID(1))

	if firstErr != nil || againErr != nil {
		t.Fatalf("publish = %v, %v", firstErr, againErr)
	}
	if first || !again {
		t.Errorf("duplicate = %t then %t, want false then true", first, again)
	}
	assertStored(t, cluster, topology.Stream(secondPartition), 1)
}

func assertStored(t *testing.T, cluster *natsjstest.Cluster, stream string, want uint64) {
	t.Helper()
	info, err := rawJetStream(t, cluster).Stream(t.Context(), stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	if got := info.CachedInfo().State.Msgs; got != want {
		t.Errorf("stream %s holds %d records, want %d", stream, got, want)
	}
}

// TestTheLargestRecordFitsWithItsHeaders: a record of exactly the guard plus the Msg-Id and
// Expect-Stream headers must be stored, or a valid request would be retried until it is dropped.
func TestTheLargestRecordFitsWithItsHeaders(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(onePartition, threeReplicas)
	publisher := publisherOf(t, ensured(t, cluster, topology), topology)

	_, err := publishOnce(t, publisher, firstPartition, make([]byte, record.MaxRecordBytes), msgID(1))

	if err != nil {
		t.Fatalf("a record of %d bytes: %v", record.MaxRecordBytes, err)
	}
	assertStored(t, cluster, topology.Stream(firstPartition), 1)
}

func TestAPartitionOutsideTheTopologyIsNeverPublished(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(onePartition, oneReplica)
	publisher := publisherOf(t, ensured(t, cluster, topology), topology)

	_, err := publishOnce(t, publisher, secondPartition, make([]byte, smallRecordBytes), msgID(1))

	if !errors.Is(err, ingest.ErrUnavailable) || !errors.Is(err, natsjs.ErrUnknownPartition) {
		t.Errorf("err = %v, want ErrUnavailable and ErrUnknownPartition", err)
	}
}

func TestPublisherRefusesAnInvalidTopology(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)

	_, err := dial(t, cluster).Publisher(natsjs.Topology{})

	if !errors.Is(err, natsjs.ErrInvalidTopology) {
		t.Errorf("err = %v, want ErrInvalidTopology", err)
	}
}

type liveAnswer struct {
	name       string
	args       func(t *testing.T, cluster *natsjstest.Cluster, conn *natsjs.Conn, topology natsjs.Topology) error
	wantErr    error
	wantReason natsjs.Reason
}

// TestEveryNATSErrorMapsToOneIngestSentinel: the answers come from a live server, not from literals,
// so a server that changes a code or a description fails here instead of turning into "unexpected".
func TestEveryNATSErrorMapsToOneIngestSentinel(t *testing.T) {
	tests := []liveAnswer{
		{name: "a full stream", args: fillTheStream, wantErr: ingest.ErrOverloaded,
			wantReason: natsjs.ReasonStreamFull},
		{name: "a subject another stream owns", args: subjectOfAnotherStream, wantErr: ingest.ErrUnavailable,
			wantReason: natsjs.ReasonStreamMismatch},
		{name: "a sealed stream", args: sealTheStream, wantErr: ingest.ErrUnavailable,
			wantReason: natsjs.ReasonSealed},
		{name: "a closed connection", args: publishAfterClose, wantErr: ingest.ErrUnavailable,
			wantReason: natsjs.ReasonClosed},
		{name: "a record over max_payload", args: publishOverThePayload, wantErr: ingest.ErrBatchTooLarge,
			wantReason: natsjs.ReasonTooLarge},
		{name: "an expired deadline", args: publishWithAnExpiredDeadline, wantErr: context.DeadlineExceeded,
			wantReason: natsjs.ReasonTimeout},
		{name: "an answer that is no PubAck", args: answeredBySomeoneElse(notAnAck), wantErr: ingest.ErrUnavailable,
			wantReason: natsjs.ReasonUnexpected},
		{name: "a PubAck from another stream", args: answeredBySomeoneElse(foreignAck), wantErr: ingest.ErrUnavailable,
			wantReason: natsjs.ReasonStreamMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNode)
			topology := topologyOf(onePartition, oneReplica)
			conn := dial(t, cluster)

			err := tt.args(t, cluster, conn, topology)

			assertPublishError(t, err, tt.wantErr, tt.wantReason)
		})
	}
}

func assertPublishError(t *testing.T, err, wantErr error, wantReason natsjs.Reason) {
	t.Helper()
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	failure, ok := errors.AsType[*natsjs.PublishError](err)
	if !ok {
		t.Fatalf("err = %v, want a *PublishError", err)
	}
	if failure.Reason != wantReason {
		t.Errorf("reason = %s, want %s (%v)", failure.Reason, wantReason, failure.Cause)
	}
	if !strings.Contains(err.Error(), wantReason.String()) {
		t.Errorf("message %q does not name the reason %s", err.Error(), wantReason)
	}
}

func fillTheStream(t *testing.T, _ *natsjstest.Cluster, conn *natsjs.Conn, topology natsjs.Topology) error {
	t.Helper()
	topology.MaxBytesPerPartition = natsjs.MaxMessageBytes
	if err := conn.EnsureTopology(t.Context(), topology); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	publisher := publisherOf(t, conn, topology)
	for index := range maxFillRecords {
		if _, err := publishOnce(t, publisher, firstPartition, make([]byte, fillRecordBytes), msgID(index)); err != nil {
			return err
		}
	}
	t.Fatalf("%d records of %d bytes fit into %d bytes", maxFillRecords, fillRecordBytes, natsjs.MaxMessageBytes)
	return nil
}

func subjectOfAnotherStream(t *testing.T, cluster *natsjstest.Cluster, conn *natsjs.Conn,
	topology natsjs.Topology,
) error {
	t.Helper()
	if _, err := rawJetStream(t, cluster).CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "SOMEONE_ELSE", Subjects: []string{topology.Subject(firstPartition)},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := publishOnce(t, publisherOf(t, conn, topology), firstPartition, make([]byte, smallRecordBytes), msgID(1))
	return err
}

func sealTheStream(t *testing.T, cluster *natsjstest.Cluster, conn *natsjs.Conn, topology natsjs.Topology) error {
	t.Helper()
	if err := conn.EnsureTopology(t.Context(), topology); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	js := rawJetStream(t, cluster)
	stream, err := js.Stream(t.Context(), topology.Stream(firstPartition))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	sealed := stream.CachedInfo().Config
	sealed.Sealed = true
	if _, err := js.UpdateStream(t.Context(), sealed); err != nil {
		t.Fatalf("seal: %v", err)
	}
	_, err = publishOnce(t, publisherOf(t, conn, topology), firstPartition, make([]byte, smallRecordBytes), msgID(1))
	return err
}

func publishAfterClose(t *testing.T, cluster *natsjstest.Cluster, _ *natsjs.Conn, topology natsjs.Topology) error {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), optionsFor(cluster.URLs()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	publisher := publisherOf(t, conn, topology)
	closeConn(t, conn)
	_, err = publishOnce(t, publisher, firstPartition, make([]byte, smallRecordBytes), msgID(1))
	return err
}

func publishOverThePayload(t *testing.T, _ *natsjstest.Cluster, conn *natsjs.Conn, topology natsjs.Topology) error {
	t.Helper()
	data := make([]byte, natsjstest.MaxPayload+1)
	_, err := publishOnce(t, publisherOf(t, conn, topology), firstPartition, data, msgID(1))
	return err
}

func publishWithAnExpiredDeadline(t *testing.T, _ *natsjstest.Cluster, conn *natsjs.Conn,
	topology natsjs.Topology,
) error {
	t.Helper()
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := publisherOf(t, conn, topology).Publish(ctx, firstPartition, make([]byte, smallRecordBytes), msgID(1))
	return err
}

// answeredBySomeoneElse puts a plain subscriber where the stream should be: whatever it answers is not
// a PubAck of the partition's stream and must never become OK.
func answeredBySomeoneElse(reply []byte) func(*testing.T, *natsjstest.Cluster, *natsjs.Conn, natsjs.Topology) error {
	return func(t *testing.T, cluster *natsjstest.Cluster, conn *natsjs.Conn, topology natsjs.Topology) error {
		t.Helper()
		nc := rawConn(t, cluster)
		if _, err := nc.Subscribe(topology.Subject(firstPartition), func(request *nats.Msg) {
			if err := request.Respond(reply); err != nil {
				t.Errorf("respond: %v", err)
			}
		}); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		_, err := publishOnce(t, publisherOf(t, conn, topology), firstPartition, make([]byte, smallRecordBytes), msgID(1))
		return err
	}
}

// TestTwoOfThreeReplicasDownGivesRetryableErrorNotOK (FR-ING-003): with the quorum gone a publish is
// unavailable within the budget — no responders after the hidden retries, or the deadline when the
// survivor still believes it leads — and never acknowledged.
func TestTwoOfThreeReplicasDownGivesRetryableErrorNotOK(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(onePartition, threeReplicas)
	conn := ensured(t, cluster, topology)
	publisher := publisherOf(t, conn, topology)
	for _, node := range otherNodes(survivingNode(t, cluster, conn.Publish)) {
		cluster.Stop(node)
	}
	started := time.Now()

	_, err := publishOnce(t, publisher, firstPartition, make([]byte, smallRecordBytes), msgID(1))

	if !errors.Is(err, ingest.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	failure, _ := errors.AsType[*natsjs.PublishError](err)
	if reason := failure.Reason; reason != natsjs.ReasonNoResponders && reason != natsjs.ReasonTimeout {
		t.Errorf("reason = %s (%v), want no responders or timeout", reason, failure.Cause)
	}
	if took := time.Since(started); took > answerWithin {
		t.Errorf("answered after %s, budget %s", took, answerWithin)
	}
}

// survivingNode keeps the node the link uses, so the client is not also reconnecting.
func survivingNode(t *testing.T, cluster *natsjstest.Cluster, link *natsjs.Link) int {
	t.Helper()
	connected := link.NATS.ConnectedServerName()
	for node, running := range cluster.Servers {
		if running.Name() == connected {
			return node
		}
	}
	t.Fatalf("the publish connection is on %q, no such node", connected)
	return 0
}
