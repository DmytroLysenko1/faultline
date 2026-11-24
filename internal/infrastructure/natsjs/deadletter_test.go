package natsjs_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	deadLetterAge     = 7 * 24 * time.Hour
	deadLetterBytes   = 1 << 30
	deadLetterFloor   = 5 * time.Minute
	shortDedup        = 30 * time.Second
	longDedup         = 10 * time.Minute
	deadLetterSubject = "dlq.>"
)

func deadLetterShape(replicas int, duplicates time.Duration) streamShape {
	return streamShape{
		Subjects: []string{deadLetterSubject}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew,
		Storage: jetstream.FileStorage, Replicas: replicas, MaxAge: deadLetterAge,
		MaxMsgSize: record.MaxRecordBytes + natsjs.HeaderBudget, MaxBytes: deadLetterBytes, Duplicates: duplicates,
		Compression: jetstream.NoCompression, Version: "1",
	}
}

func TestEnsureTopologyCreatesTheDeadLetterQueue(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)

	ensured(t, cluster, topologyOf(twoPartitions, threeReplicas))

	js := rawJetStream(t, cluster)
	if diff := cmp.Diff(deadLetterShape(threeReplicas, deadLetterFloor), shapeOf(t, js, natsjs.DeadLetterStream)); diff != "" {
		t.Errorf("dead letter stream (-want +got):\n%s", diff)
	}
	stream, err := js.Stream(t.Context(), natsjs.DeadLetterStream)
	if err != nil || stream.CachedInfo().Config.AllowDirect {
		t.Errorf("Stream = %v; want direct reads off", err)
	}
}

func TestDeadLetterDedupWindowHasAFloor(t *testing.T) {
	tests := []struct {
		name   string
		window time.Duration
		want   time.Duration
	}{
		{name: "a journal window below the floor", window: shortDedup, want: deadLetterFloor},
		{name: "a journal window above it", window: longDedup, want: longDedup},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNode)
			topology := topologyOf(onePartition, oneReplica)
			topology.DuplicateWindow = tt.window

			ensured(t, cluster, topology)

			if got := shapeOf(t, rawJetStream(t, cluster), natsjs.DeadLetterStream).Duplicates; got != tt.want {
				t.Errorf("dead letter duplicate window %s, want %s", got, tt.want)
			}
		})
	}
}

func TestAHandShortenedDeadLetterDedupWindowIsPutBack(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(onePartition, oneReplica)
	conn := ensured(t, cluster, topology)
	js := rawJetStream(t, cluster)
	stream, err := js.Stream(t.Context(), natsjs.DeadLetterStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	shortened := stream.CachedInfo().Config
	shortened.Duplicates = shortDedup
	if _, err := js.UpdateStream(t.Context(), shortened); err != nil {
		t.Fatalf("hand edit: %v", err)
	}

	if err := conn.EnsureTopology(t.Context(), topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	if got := shapeOf(t, js, natsjs.DeadLetterStream).Duplicates; got != deadLetterFloor {
		t.Errorf("dead letter duplicate window %s after EnsureTopology, want %s back", got, deadLetterFloor)
	}
}

func TestDeadLetterTopologyFailureKeepsPartitions(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(twoPartitions, oneReplica)
	js := rawJetStream(t, cluster)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: natsjs.DeadLetterStream, Subjects: []string{deadLetterSubject}, Discard: jetstream.DiscardOld,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	err := dial(t, cluster).EnsureTopology(t.Context(), topology, natsjstest.Held{})

	if !errors.Is(err, natsjs.ErrDeadLetterTopology) || !errors.Is(err, natsjs.ErrStreamDrift) {
		t.Fatalf("EnsureTopology = %v, want ErrDeadLetterTopology for the drift", err)
	}
	for _, partition := range []uint16{firstPartition, secondPartition} {
		if got := shapeOf(t, js, topology.Stream(partition)); got.MaxBytes != testMaxBytes {
			t.Errorf("stream of partition %d = %+v, want it ensured", partition, got)
		}
	}
	if got := shapeOf(t, js, natsjs.DeadLetterStream).Discard; got != jetstream.DiscardOld {
		t.Errorf("dead letter discard %v: a drifted stream was rewritten", got)
	}
}
