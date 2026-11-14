package natsjs_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	racingProcessors = 4
	grownMaxBytes    = 2 * testMaxBytes
	electionWithin   = 30 * time.Second
)

type streamShape struct {
	Subjects    []string
	Retention   jetstream.RetentionPolicy
	Discard     jetstream.DiscardPolicy
	Storage     jetstream.StorageType
	Replicas    int
	MaxAge      time.Duration
	MaxMsgSize  int32
	MaxBytes    int64
	Duplicates  time.Duration
	Compression jetstream.StoreCompression
	Version     string
	Partitions  string
}

// shapeOf reads a stream the way an operator would; right after creation an R3 stream may still be
// electing its leader, so the read is retried until it answers.
func shapeOf(t *testing.T, js jetstream.JetStream, name string) streamShape {
	t.Helper()
	var stream jetstream.Stream
	eventually(t, "stream "+name+" answers", func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		found, err := js.Stream(ctx, name)
		stream = found
		return err == nil
	})
	config := stream.CachedInfo().Config
	return streamShape{
		Subjects: config.Subjects, Retention: config.Retention, Discard: config.Discard, Storage: config.Storage,
		Replicas: config.Replicas, MaxAge: config.MaxAge, MaxMsgSize: config.MaxMsgSize, MaxBytes: config.MaxBytes,
		Duplicates: config.Duplicates, Compression: config.Compression,
		Version: config.Metadata["faultline.config_version"], Partitions: config.Metadata["faultline.partitions"],
	}
}

// TestEnsureTopologyCreatesAStreamPerPartitionThatKeepsEveryRecord: limits retention, new records
// refused when full, no age limit — nothing acknowledged is dropped by the server on its own (I15).
func TestEnsureTopologyCreatesAStreamPerPartitionThatKeepsEveryRecord(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(twoPartitions, threeReplicas)

	ensured(t, cluster, topology)

	js := rawJetStream(t, cluster)
	for _, partition := range []uint16{firstPartition, secondPartition} {
		want := streamShape{
			Subjects: []string{topology.Subject(partition)}, Retention: jetstream.LimitsPolicy,
			Discard: jetstream.DiscardNew, Storage: jetstream.FileStorage, Replicas: threeReplicas,
			MaxMsgSize: natsjs.MaxMessageBytes, MaxBytes: testMaxBytes, Duplicates: testWindow,
			Compression: jetstream.NoCompression, Version: "1", Partitions: "2",
		}
		if diff := cmp.Diff(want, shapeOf(t, js, topology.Stream(partition))); diff != "" {
			t.Errorf("stream of partition %d (-want +got):\n%s", partition, diff)
		}
	}
}

func TestTwoProcessorsEnsuringTheSameTopologyAgree(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(twoPartitions, threeReplicas)
	conns := make([]*natsjs.Conn, racingProcessors)
	for index := range conns {
		conns[index] = dial(t, cluster)
	}
	errs := make([]error, racingProcessors)
	var processors sync.WaitGroup

	for index, conn := range conns {
		processors.Go(func() { errs[index] = conn.EnsureTopology(t.Context(), topology, natsjstest.Held{}) })
	}
	processors.Wait()

	for index, err := range errs {
		if err != nil {
			t.Errorf("processor %d: %v", index, err)
		}
	}
	if _, err := conns[0].VerifyTopology(t.Context(), topology); err != nil {
		t.Errorf("after the race: %v", err)
	}
}

// TestAnOlderReleaseCannotRewriteANewerTopology: during a rollout the old processor must stop, not
// put its smaller MaxBytes back over the new one.
func TestAnOlderReleaseCannotRewriteANewerTopology(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	newer := topologyOf(onePartition, oneReplica)
	newer.ConfigVersion, newer.MaxBytesPerPartition = secondVersion, grownMaxBytes
	ensured(t, cluster, newer)
	older := topologyOf(onePartition, oneReplica)

	err := dial(t, cluster).EnsureTopology(t.Context(), older, natsjstest.Held{})

	assertDrift(t, err, "faultline.config_version")
	if got := shapeOf(t, rawJetStream(t, cluster), older.Stream(firstPartition)).MaxBytes; got != grownMaxBytes {
		t.Errorf("max bytes = %d, want the newer %d kept", got, grownMaxBytes)
	}
}

func TestANewerReleaseUpdatesTheMutableFields(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	ensured(t, cluster, topologyOf(onePartition, oneReplica))
	newer := topologyOf(onePartition, oneReplica)
	newer.ConfigVersion, newer.MaxBytesPerPartition = secondVersion, grownMaxBytes
	newer.Compression = natsjs.CompressionS2

	err := dial(t, cluster).EnsureTopology(t.Context(), newer, natsjstest.Held{})

	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	got := shapeOf(t, rawJetStream(t, cluster), newer.Stream(firstPartition))
	if got.MaxBytes != grownMaxBytes || got.Compression != jetstream.S2Compression || got.Version != "2" {
		t.Errorf("stream = %+v, want the newer max bytes, compression and version", got)
	}
}

func TestAChangedImmutableFieldIsDriftNotAnUpdate(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(onePartition, oneReplica)
	js := rawJetStream(t, cluster)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: topology.Stream(firstPartition), Subjects: []string{topology.Subject(firstPartition)},
		Discard: jetstream.DiscardOld, MaxBytes: testMaxBytes,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	topology.ConfigVersion = secondVersion

	err := dial(t, cluster).EnsureTopology(t.Context(), topology, natsjstest.Held{})

	assertDrift(t, err, "discard")
	if got := shapeOf(t, js, topology.Stream(firstPartition)).Discard; got != jetstream.DiscardOld {
		t.Errorf("discard = %v: an immutable field was rewritten", got)
	}
}

func TestVerifyTopologyAcceptsTheStreamsTheProcessorEnsured(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(twoPartitions, threeReplicas)
	ensured(t, cluster, topology)

	verified, err := dial(t, cluster).VerifyTopology(t.Context(), topology)

	if err != nil || len(verified.Notes) > 0 {
		t.Errorf("VerifyTopology = %+v, %v; want no drift at all", verified, err)
	}
}

// TestVerifyTopologyIgnoresWhatTheServerAddsAfterARestart: the server writes _nats.* metadata and
// its defaults into every stream; comparing whole configs would stop every ingester on every start.
func TestVerifyTopologyIgnoresWhatTheServerAddsAfterARestart(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(onePartition, oneReplica)
	ensured(t, cluster, topology)
	cluster.Stop(0)
	cluster.Restart(0)
	cluster.AwaitLeader()

	verified, err := dial(t, cluster).VerifyTopology(t.Context(), topology)

	if err != nil || len(verified.Notes) > 0 {
		t.Errorf("VerifyTopology after a restart = %+v, %v; want no drift at all", verified, err)
	}
}

// TestVerifyTopologyStopsOnlyWhatTheIngesterDependsOn: a different partition count would split
// traces, a shorter dedup window would turn retries into duplicates, a missing stream loses records —
// those stop the start. A grown MaxBytes, another compression or a newer version are the processor's
// business: reported, never in the way of a rollout or a scale-up.
func TestVerifyTopologyStopsOnlyWhatTheIngesterDependsOn(t *testing.T) {
	tests := []struct {
		name         string
		args         func(*natsjs.Topology)
		wantBlocking string
		wantNote     string
	}{
		{name: "partition count", args: func(t *natsjs.Topology) { t.Partitions = onePartition },
			wantBlocking: "faultline.partitions"},
		{name: "a missing stream", args: func(t *natsjs.Topology) { t.Partitions = twoPartitions + 1 },
			wantBlocking: "stream"},
		{name: "a dedup window shorter than the ingester's", args: func(t *natsjs.Topology) {
			t.DuplicateWindow = 2 * testWindow
		}, wantBlocking: "duplicate_window"},
		{name: "a longer dedup window", args: func(t *natsjs.Topology) { t.DuplicateWindow = testWindow / 2 },
			wantNote: "duplicate_window"},
		{name: "max bytes", args: func(t *natsjs.Topology) { t.MaxBytesPerPartition = grownMaxBytes },
			wantNote: "max_bytes"},
		{name: "compression", args: func(t *natsjs.Topology) { t.Compression = natsjs.CompressionS2 },
			wantNote: "compression"},
		{name: "config version", args: func(t *natsjs.Topology) { t.ConfigVersion = secondVersion },
			wantNote: "faultline.config_version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNode)
			ensured(t, cluster, topologyOf(twoPartitions, oneReplica))
			ingester := topologyOf(twoPartitions, oneReplica)
			tt.args(&ingester)

			verified, err := dial(t, cluster).VerifyTopology(t.Context(), ingester)

			assertVerification(t, verified, err, tt.wantBlocking, tt.wantNote)
		})
	}
}

// TestAShortDedupWindowSaysWhatNATSHas: the ingester's start fails with a diff an operator can act
// on, both sides filled in.
func TestAShortDedupWindowSaysWhatNATSHas(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	ensured(t, cluster, topologyOf(onePartition, oneReplica))
	ingester := topologyOf(onePartition, oneReplica)
	ingester.DuplicateWindow = 2 * testWindow

	verified, _ := dial(t, cluster).VerifyTopology(t.Context(), ingester)

	want := []natsjs.Drift{{
		Stream: ingester.Stream(firstPartition),
		Field:  "duplicate_window",
		Want:   (2 * testWindow).String(),
		Got:    testWindow.String(),
	}}
	if diff := cmp.Diff(want, verified.Blocking); diff != "" {
		t.Errorf("blocking drift (-want +got):\n%s", diff)
	}
}

func assertVerification(t *testing.T, verified natsjs.Verification, err error, blocking, note string) {
	t.Helper()
	if blocking != "" {
		assertDrift(t, err, blocking)
		return
	}
	if err != nil {
		t.Fatalf("VerifyTopology = %v, want only notes", err)
	}
	if !namesField(verified.Notes, note) {
		t.Errorf("notes %+v do not name %s", verified.Notes, note)
	}
}

func namesField(drifts []natsjs.Drift, name string) bool {
	for _, drift := range drifts {
		if drift.Field == name {
			return true
		}
	}
	return false
}

func assertDrift(t *testing.T, err error, field string) {
	t.Helper()
	if !errors.Is(err, natsjs.ErrStreamDrift) {
		t.Fatalf("err = %v, want ErrStreamDrift", err)
	}
	drift, ok := errors.AsType[*natsjs.DriftError](err)
	if !ok {
		t.Fatalf("err = %v, want a *DriftError listing the fields", err)
	}
	if !namesField(drift.Drifts, field) {
		t.Errorf("drift %+v does not name %s", drift.Drifts, field)
	}
}

// TestEnsureTopologyWaitsForTheClusterToElectALeader: a processor that starts while the cluster has no
// metadata leader keeps trying, and creates the streams once the nodes are back.
func TestEnsureTopologyWaitsForTheClusterToElectALeader(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	topology := topologyOf(onePartition, threeReplicas)
	conn := dial(t, cluster)
	survivor := survivingNode(t, cluster, conn.Control)
	stopped := otherNodes(survivor)
	for _, node := range stopped {
		cluster.Stop(node)
	}
	ensuredErr := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), electionWithin)
	defer cancel()

	go func() { ensuredErr <- conn.EnsureTopology(ctx, topology, natsjstest.Held{}) }()
	for _, node := range stopped {
		cluster.Restart(node)
	}

	if err := <-ensuredErr; err != nil {
		t.Fatalf("EnsureTopology across the election: %v", err)
	}
	if _, err := conn.VerifyTopology(t.Context(), topology); err != nil {
		t.Errorf("VerifyTopology: %v", err)
	}
}

func TestEnsureTopologyGivesUpWhenItsContextEnds(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	conn := dial(t, cluster)
	for _, node := range otherNodes(survivingNode(t, cluster, conn.Control)) {
		cluster.Stop(node)
	}
	ctx, cancel := context.WithTimeout(t.Context(), publishBudget)
	defer cancel()

	err := conn.EnsureTopology(ctx, topologyOf(onePartition, threeReplicas), natsjstest.Held{})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the deadline", err)
	}
}

func otherNodes(survivor int) []int {
	var others []int
	for node := range threeNodes {
		if node != survivor {
			others = append(others, node)
		}
	}
	return others
}

func TestADriftNamesEveryDifferingField(t *testing.T) {
	err := &natsjs.DriftError{Drifts: []natsjs.Drift{
		{Stream: "S", Field: "max_bytes", Want: "1", Got: "2"},
		{Stream: "S", Field: "replicas", Want: "3", Got: "1"},
	}}

	got := err.Error()

	for _, part := range []string{"S max_bytes: want 1, nats has 2", "S replicas: want 3, nats has 1"} {
		if !strings.Contains(got, part) {
			t.Errorf("%q does not contain %q", got, part)
		}
	}
}

// TestEnsureTopologyRidesOutAReconnect: the node the processor dialled goes away; with no reconnect
// buffer every call fails fast while the client moves to another node, and EnsureTopology waits
// for it instead of failing the start.
func TestEnsureTopologyRidesOutAReconnect(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes)
	conn := dialURLs(t, cluster.URLs()[:1])
	cluster.Stop(0)
	ctx, cancel := context.WithTimeout(t.Context(), eventuallyWithin)
	defer cancel()

	err := conn.EnsureTopology(ctx, topologyOf(onePartition, oneReplica), natsjstest.Held{})

	if err != nil {
		t.Errorf("EnsureTopology across a reconnect: %v", err)
	}
}

// TestADriftOnOneStreamLeavesEveryStreamAsItWas: an operator changed one partition's stream by hand;
// the new release must stop without first moving the other partitions to its version, or the
// rollback would find them "newer" and refuse to start too.
func TestADriftOnOneStreamLeavesEveryStreamAsItWas(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	current := topologyOf(twoPartitions, oneReplica)
	ensured(t, cluster, current)
	js := rawJetStream(t, cluster)
	stream, err := js.Stream(t.Context(), current.Stream(secondPartition))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	changed := stream.CachedInfo().Config
	changed.MaxAge = time.Hour
	if _, err := js.UpdateStream(t.Context(), changed); err != nil {
		t.Fatalf("hand edit: %v", err)
	}
	next := current
	next.ConfigVersion, next.MaxBytesPerPartition = secondVersion, grownMaxBytes

	err = dial(t, cluster).EnsureTopology(t.Context(), next, natsjstest.Held{})

	assertDrift(t, err, "max_age")
	first := shapeOf(t, js, current.Stream(firstPartition))
	if first.Version != "1" || first.MaxBytes != testMaxBytes {
		t.Errorf("partition 0 = version %s, max bytes %d: updated before the drift on partition 1 was found",
			first.Version, first.MaxBytes)
	}
}

// TestAWrongTopologyCreatesNothing: a partition count set too high by mistake must not leave the
// extra streams behind, each reserving its MaxBytes on the server, before the drift stops the start.
func TestAWrongTopologyCreatesNothing(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	ensured(t, cluster, topologyOf(onePartition, oneReplica))
	wrong := topologyOf(twoPartitions, oneReplica)

	err := dial(t, cluster).EnsureTopology(t.Context(), wrong, natsjstest.Held{})

	assertDrift(t, err, "faultline.partitions")
	_, err = rawJetStream(t, cluster).Stream(t.Context(), wrong.Stream(secondPartition))
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("stream of partition 1 = %v: created before the drift was found", err)
	}
}

type lostLease struct{}

func (lostLease) Valid() bool {
	return false
}

// TestEnsureTopologyWritesNothingWithoutTheLease: a stream configuration has no conditional update, so a
// processor that no longer holds the topology lease must not create or update a stream — another
// holder may be writing a different version.
func TestEnsureTopologyWritesNothingWithoutTheLease(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(onePartition, oneReplica)
	js := rawJetStream(t, cluster)
	conn := dial(t, cluster)

	missing := conn.EnsureTopology(t.Context(), topology, lostLease{})
	_, lookup := js.Stream(t.Context(), topology.Stream(firstPartition))
	ensured(t, cluster, topology)
	next := topology
	next.ConfigVersion = secondVersion
	stale := conn.EnsureTopology(t.Context(), next, lostLease{})
	none := conn.EnsureTopology(t.Context(), next, nil)

	for name, err := range map[string]error{"create": missing, "update": stale, "no lease": none} {
		if !errors.Is(err, natsjs.ErrNotHolder) {
			t.Errorf("%s = %v, want ErrNotHolder", name, err)
		}
	}
	if !errors.Is(lookup, jetstream.ErrStreamNotFound) {
		t.Errorf("stream after the refused create: %v, want not found", lookup)
	}
	if got := shapeOf(t, js, topology.Stream(firstPartition)).Version; got != "1" {
		t.Errorf("config version %s after the refused update, want 1", got)
	}
}

// expiringLease is held for its first checks only, like a lease that is lost while EnsureTopology works.
type expiringLease struct {
	left int
}

func (l *expiringLease) Valid() bool {
	l.left--
	return l.left >= 0
}

// TestEnsureTopologyChecksTheLeaseBeforeEveryWrite: a lease lost after the first stream was written stops
// the second write — one check before the whole run would let a slow EnsureTopology write without it.
func TestEnsureTopologyChecksTheLeaseBeforeEveryWrite(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	topology := topologyOf(twoPartitions, oneReplica)
	js := rawJetStream(t, cluster)

	err := dial(t, cluster).EnsureTopology(t.Context(), topology, &expiringLease{left: 1})

	_, first := js.Stream(t.Context(), topology.Stream(firstPartition))
	_, second := js.Stream(t.Context(), topology.Stream(firstPartition+1))
	if !errors.Is(err, natsjs.ErrNotHolder) || first != nil || !errors.Is(second, jetstream.ErrStreamNotFound) {
		t.Errorf("EnsureTopology = %v, first stream %v, second stream %v; want ErrNotHolder after the first",
			err, first, second)
	}
}
