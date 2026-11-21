package lease_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	feedMaxBytes      = 64 << 20
	feedMaxFootprint  = 6 << 20
	feedPullBytes     = 16 << 20
	feedIdleAfter     = 35 * time.Second
	receiveLimit      = 10
	receiveWait       = time.Second
	validPoll         = 10 * time.Millisecond
	firstPartitionKey = "processor.p0"
	intruderToken     = "intruder/1"
	firstPosition     = 1
)

func TestAFeedWhoseLeaseKeyIsTakenSuspendsCommitsThenStopsWithTheLeaseLost(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	conn := dial(t, cluster)
	options := fastOptions(first, oneReplica)
	if err := conn.EnsureTopology(t.Context(), options.Topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	held := acquire(t, conn, first)
	ran := runUntilTheTestEnds(t, held)
	opened := openFeed(t, conn, held)

	takeTheKey(t, cluster)
	waitUntilInvalid(t, held)

	err := opened.Commit(t.Context(), processing.Position{Sequence: firstPosition})
	if !errors.Is(err, feed.ErrLeaseSuspended) {
		t.Errorf("Commit with the lease invalid = %v, want ErrLeaseSuspended", err)
	}
	if err := receiveUntilStopped(t, opened); !errors.Is(err, processing.ErrLeaseLost) {
		t.Errorf("Receive = %v, want ErrLeaseLost", err)
	}
	if got := opened.Stats().Stop; got != feed.StopLeaseLost {
		t.Errorf("Stats().Stop = %v, want %v", got, feed.StopLeaseLost)
	}
	select {
	case err := <-ran:
		if !errors.Is(err, processing.ErrLeaseLost) {
			t.Errorf("Run = %v, want ErrLeaseLost", err)
		}
	case <-time.After(afterTheTTL):
		t.Errorf("Run has not returned %s after the feed stopped with the lease lost", afterTheTTL)
	}
}

func runUntilTheTestEnds(t *testing.T, held *lease.Lease) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	ran := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() { ran <- held.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return ran
}

func openFeed(t *testing.T, conn *natsjs.Conn, held *lease.Lease) *feed.Feed {
	t.Helper()
	opened, err := feed.Open(t.Context(), conn, held, feed.Start{}, &feed.Options{
		Topology:     fastOptions(first, oneReplica).Topology,
		Epoch:        held.Epoch,
		MaxBytes:     feedMaxBytes,
		MaxFootprint: feedMaxFootprint,
		PullMaxBytes: feedPullBytes,
		IdleAfter:    feedIdleAfter,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		opened.Close()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeTimeout)
		defer cancel()
		if err := opened.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	return opened
}

func takeTheKey(t *testing.T, cluster *natsjstest.Cluster) {
	t.Helper()
	kv, err := rawJetStream(t, cluster).KeyValue(t.Context(), lease.Bucket)
	if err != nil {
		t.Fatalf("lease bucket: %v", err)
	}
	if _, err := kv.Put(t.Context(), firstPartitionKey, []byte(intruderToken)); err != nil {
		t.Fatalf("take the key: %v", err)
	}
}

func waitUntilInvalid(t *testing.T, held *lease.Lease) {
	t.Helper()
	deadline := time.Now().Add(fastTTL)
	for held.Valid() {
		if time.Now().After(deadline) {
			t.Fatalf("the lease is still valid %s after its key was taken", fastTTL)
		}
		time.Sleep(validPoll)
	}
}

func receiveUntilStopped(t *testing.T, opened *feed.Feed) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), afterTheTTL)
	defer cancel()
	for {
		if _, err := opened.Receive(ctx, receiveLimit, receiveWait); err != nil {
			return err
		}
	}
}
