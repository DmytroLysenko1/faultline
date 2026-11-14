package lease_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	fastTTL        = 6 * time.Second
	fastLost       = time.Second
	fastStep       = 200 * time.Millisecond
	fastElection   = 400 * time.Millisecond
	connectTimeout = 2 * time.Second
	drainTimeout   = 500 * time.Millisecond
	closeTimeout   = 5 * time.Second
	expirySlack    = 500 * time.Millisecond
	heldWindow     = 2 * time.Second
	afterTheTTL    = fastTTL + time.Second
	shortBucket    = 3 * time.Second
	longBucket     = 9 * time.Second
	singleNode     = 1
	threeNodes     = 3
	threeReplicas  = 3
	onePartition   = 1
	oneReplica     = 1
	testMaxBytes   = 1 << 30
	testWindow     = 5 * time.Minute
	firstVersion   = 1
	keyHistory     = 1
	first          = "processor-a"
	second         = "processor-b"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func fastOptions(instance string, replicas int) *lease.Options {
	return lease.WithElectionBudget(&lease.Options{
		Topology: natsjs.Topology{
			Partitions:           onePartition,
			Replicas:             replicas,
			MaxBytesPerPartition: testMaxBytes,
			DuplicateWindow:      testWindow,
			Compression:          natsjs.CompressionNone,
			ConfigVersion:        firstVersion,
		},
		Instance:    instance,
		TTL:         fastTTL,
		RenewEvery:  fastStep,
		RetryEvery:  fastStep,
		AttemptTime: fastStep,
		LostAfter:   fastLost,
		Logger:      slog.New(slog.DiscardHandler),
	}, fastElection)
}

func dial(t *testing.T, cluster *natsjstest.Cluster) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), &natsjs.ConnOptions{
		URLs:           cluster.URLs(),
		Name:           t.Name(),
		ConnectTimeout: connectTimeout,
		DrainTimeout:   drainTimeout,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { closeConn(t, conn) })
	return conn
}

func closeConn(t *testing.T, conn *natsjs.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func rawJetStream(t *testing.T, cluster *natsjstest.Cluster) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(cluster.URLs()[0])
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

func acquire(t *testing.T, conn *natsjs.Conn, instance string) *lease.Lease {
	t.Helper()
	return acquireReplicated(t, conn, instance, oneReplica)
}

func acquireReplicated(t *testing.T, conn *natsjs.Conn, instance string, replicas int) *lease.Lease {
	t.Helper()
	held, err := lease.Acquire(t.Context(), conn, fastOptions(instance, replicas))
	if err != nil {
		t.Fatalf("Acquire %s: %v", instance, err)
	}
	return held
}

type acquired struct {
	lease *lease.Lease
	err   error
}

func acquiringReplicated(t *testing.T, conn *natsjs.Conn, instance string, replicas int) <-chan acquired {
	t.Helper()
	done := make(chan acquired, 1)
	go func() {
		held, err := lease.Acquire(t.Context(), conn, fastOptions(instance, replicas))
		done <- acquired{lease: held, err: err}
	}()
	return done
}

func renewing(t *testing.T, held *lease.Lease) (stop func() error, returned <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- held.Run(ctx) }()
	t.Cleanup(cancel)
	return func() error {
		cancel()
		return <-done
	}, done
}

func holderOf(t *testing.T, js jetstream.JetStream) string {
	t.Helper()
	kv, err := js.KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}
	found, err := kv.Get(t.Context(), "processor.p0")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return ""
	}
	if err != nil {
		t.Fatalf("get the lease key: %v", err)
	}
	instance, _, _ := strings.Cut(string(found.Value()), "/")
	return instance
}

func TestASecondProcessorGetsThePartitionOnlyAfterTheFirstReleasesIt(t *testing.T) {
	for _, nodes := range []int{singleNode, threeNodes} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			cluster := natsjstest.Start(t, nodes)
			held := acquireReplicated(t, dial(t, cluster), first, nodes)
			stop, _ := renewing(t, held)
			waiting := acquiringReplicated(t, dial(t, cluster), second, nodes)

			select {
			case got := <-waiting:
				t.Fatalf("second processor acquired a held partition: %v", got.err)
			case <-time.After(heldWindow):
			}
			if err := stop(); err != nil {
				t.Fatalf("release: %v", err)
			}
			got := <-waiting

			if got.err != nil {
				t.Fatalf("Acquire after the release: %v", got.err)
			}
			if got.lease.Epoch <= held.Epoch || held.Valid() {
				t.Errorf("epochs %d then %d, first still valid %t; want a higher epoch and the first invalid",
					held.Epoch, got.lease.Epoch, held.Valid())
			}
			if holder := holderOf(t, rawJetStream(t, cluster)); holder != second {
				t.Errorf("key held by %q, want %q", holder, second)
			}
		})
	}
}

func TestAKilledHolderIsTakenOverOnceItsKeyExpiresAndItsLateReleaseLeavesTheNewKey(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	killed := acquire(t, dial(t, cluster), first)
	killedAt := time.Now()

	taken := acquire(t, dial(t, cluster), second)
	waited := time.Since(killedAt)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	lateRelease := killed.Run(ctx)

	if waited < fastTTL-expirySlack || taken.Epoch <= killed.Epoch {
		t.Errorf("taken over after %s with epoch %d over %d, want after the TTL %s with a higher epoch", waited,
			taken.Epoch, killed.Epoch, fastTTL)
	}
	if lateRelease != nil || killed.Stats().ReleaseFailures != 1 {
		t.Errorf("late release = %v with %d release failures, want nil and the refused delete counted",
			lateRelease, killed.Stats().ReleaseFailures)
	}
	if holder := holderOf(t, rawJetStream(t, cluster)); holder != second {
		t.Errorf("key held by %q after the late release, want %q", holder, second)
	}
}

// TestARenewedKeyExpiresTheTTLAfterItsLastRenewal: renewals keep the key past the TTL of its creation;
// once they stop reaching NATS the key expires a TTL after the last one, the old holder's lease is
// lost, and another processor takes the partition.
func TestARenewedKeyExpiresTheTTLAfterItsLastRenewal(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	cut := dial(t, cluster)
	held := acquire(t, cut, first)
	_, returned := renewing(t, held)
	time.Sleep(afterTheTTL)
	stillHeld := holderOf(t, rawJetStream(t, cluster))

	cutAt := time.Now()
	cut.Control.NATS.Close()
	taken := acquire(t, dial(t, cluster), second)
	waited := time.Since(cutAt)

	if stillHeld != first {
		t.Errorf("after the TTL the key is held by %q, want the renewing %q", stillHeld, first)
	}
	if waited < fastTTL-fastStep-expirySlack || waited > fastTTL+expirySlack || taken.Epoch <= held.Epoch {
		t.Errorf("taken over %s after renewals stopped with epoch %d over %d, want about the TTL %s", waited,
			taken.Epoch, held.Epoch, fastTTL)
	}
	if err := <-returned; !errors.Is(err, lease.ErrLeaseLost) {
		t.Errorf("Run of the cut holder = %v, want ErrLeaseLost", err)
	}
}

func TestADeletedBucketLosesTheLeaseAtTheTTL(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	held := acquire(t, dial(t, cluster), first)
	_, returned := renewing(t, held)
	deletedAt := time.Now()

	if err := rawJetStream(t, cluster).DeleteKeyValue(t.Context(), lease.Bucket); err != nil {
		t.Fatalf("delete the lease bucket: %v", err)
	}
	err := <-returned

	lostAfter := time.Since(deletedAt)
	if !errors.Is(err, lease.ErrLeaseLost) || lostAfter > fastTTL+expirySlack || lostAfter < fastTTL-fastStep-expirySlack {
		t.Errorf("Run = %v after %s, want ErrLeaseLost within the TTL %s", err, lostAfter, fastTTL)
	}
	if held.Valid() {
		t.Error("a lost lease is still valid")
	}
}

type bucketShape struct {
	Storage jetstream.StorageType
	MaxAge  time.Duration
	History int64
}

func TestTheLeaseBucketIsAFileBucketWhoseMaxAgeOnlyGrows(t *testing.T) {
	tests := []struct {
		Name     string
		Nodes    int
		Replicas int
		Args     *jetstream.KeyValueConfig
		Want     bucketShape
		WantErr  error
	}{
		{Name: "created when missing", Nodes: singleNode, Replicas: oneReplica,
			Want: bucketShape{Storage: jetstream.FileStorage, MaxAge: fastTTL,
				History: keyHistory}},
		{Name: "a shorter max age grows to the TTL", Nodes: singleNode, Replicas: oneReplica,
			Args: &jetstream.KeyValueConfig{Bucket: lease.Bucket,
				TTL: shortBucket, History: keyHistory, Storage: jetstream.FileStorage},
			Want: bucketShape{Storage: jetstream.FileStorage, MaxAge: fastTTL, History: keyHistory}},
		{Name: "a longer max age stays", Nodes: singleNode, Replicas: oneReplica,
			Args: &jetstream.KeyValueConfig{Bucket: lease.Bucket, TTL: longBucket,
				History: keyHistory, Storage: jetstream.FileStorage},
			Want: bucketShape{Storage: jetstream.FileStorage, MaxAge: longBucket, History: keyHistory}},
		{Name: "a memory bucket is refused", Nodes: singleNode, Replicas: oneReplica,
			Args: &jetstream.KeyValueConfig{Bucket: lease.Bucket, TTL: fastTTL,
				History: keyHistory, Storage: jetstream.MemoryStorage},
			Want:    bucketShape{Storage: jetstream.MemoryStorage, MaxAge: fastTTL, History: keyHistory},
			WantErr: lease.ErrBucketDrift},
		{Name: "a bucket with other replicas is refused", Nodes: threeNodes, Replicas: threeReplicas,
			Args: &jetstream.KeyValueConfig{Bucket: lease.Bucket, TTL: fastTTL, History: keyHistory,
				Storage: jetstream.FileStorage, Replicas: oneReplica},
			Want:    bucketShape{Storage: jetstream.FileStorage, MaxAge: fastTTL, History: keyHistory},
			WantErr: lease.ErrBucketDrift},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			cluster := natsjstest.Start(t, tt.Nodes)
			js := rawJetStream(t, cluster)
			if tt.Args != nil {
				if _, err := js.CreateKeyValue(t.Context(), *tt.Args); err != nil {
					t.Fatalf("prepare the bucket: %v", err)
				}
			}

			_, err := lease.Acquire(t.Context(), dial(t, cluster), fastOptions(first, tt.Replicas))

			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("Acquire = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, shapeOf(t, js)); diff != "" {
				t.Errorf("bucket (-want +got):\n%s", diff)
			}
		})
	}
}

func shapeOf(t *testing.T, js jetstream.JetStream) bucketShape {
	t.Helper()
	kv, err := js.KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}
	status, err := kv.Status(t.Context())
	if err != nil {
		t.Fatalf("bucket status: %v", err)
	}
	return bucketShape{Storage: status.Config().Storage, MaxAge: status.TTL(), History: status.History()}
}

func TestAcquireRefusesInvalidOptionsBeforeTouchingNATS(t *testing.T) {
	options := fastOptions("", oneReplica)

	_, err := lease.Acquire(t.Context(), nil, options)

	if !errors.Is(err, lease.ErrInvalidOptions) {
		t.Errorf("Acquire = %v, want ErrInvalidOptions", err)
	}
}
