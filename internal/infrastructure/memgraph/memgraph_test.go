package memgraph_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
)

const (
	publishedRevisions = 1_000
	readerCount        = 4
)

func snapshotAt(revision uint64) *topology.Snapshot {
	return &topology.Snapshot{
		Revision: revision,
		Nodes: []graph.Node{
			{
				ID:              "svc:shop/cart",
				UnansweredCalls: revision,
			},
		},
	}
}

func newTestStore(t *testing.T, initial *topology.Snapshot) *memgraph.Store {
	t.Helper()
	store, err := memgraph.New(initial)
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	return store
}

func TestNewRejectsNilSnapshot(t *testing.T) {
	_, err := memgraph.New(nil)
	if !errors.Is(err, memgraph.ErrNilSnapshot) {
		t.Fatalf("err = %v, want %v", err, memgraph.ErrNilSnapshot)
	}
}

func TestLatestReturnsInitialThenPublishedSnapshot(t *testing.T) {
	initial := snapshotAt(0)
	store := newTestStore(t, initial)
	got, err := store.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got != initial {
		t.Fatalf("Latest returned %p, want the initial snapshot %p", got, initial)
	}
	published := snapshotAt(1)
	if err := store.Publish(t.Context(), published); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got, err = store.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got != published {
		t.Errorf("Latest returned %p, want the published snapshot %p", got, published)
	}
}

func TestPublishRejectsNilAndKeepsCurrent(t *testing.T) {
	initial := snapshotAt(0)
	store := newTestStore(t, initial)
	if err := store.Publish(t.Context(), nil); !errors.Is(err, memgraph.ErrNilSnapshot) {
		t.Fatalf("err = %v, want %v", err, memgraph.ErrNilSnapshot)
	}
	if store.Current.Load() != initial {
		t.Errorf("a rejected publish replaced the current snapshot")
	}
}

func TestCanceledContext(t *testing.T) {
	initial := snapshotAt(0)
	store := newTestStore(t, initial)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.Publish(ctx, snapshotAt(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish err = %v, want %v", err, context.Canceled)
	}
	if store.Current.Load() != initial {
		t.Errorf("a canceled publish replaced the current snapshot")
	}
	if _, err := store.Latest(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Latest err = %v, want %v", err, context.Canceled)
	}
}

func TestZeroStoreReportsNoSnapshot(t *testing.T) {
	var store memgraph.Store
	if _, err := store.Latest(t.Context()); !errors.Is(err, memgraph.ErrNoSnapshot) {
		t.Errorf("err = %v, want %v", err, memgraph.ErrNoSnapshot)
	}
}

func TestReadersSeeWholeSnapshotsWhilePublishing(t *testing.T) {
	store := newTestStore(t, snapshotAt(0))
	failures := make(chan string, readerCount)
	stopReaders := startReaders(t, store, failures)
	for revision := range uint64(publishedRevisions) {
		if err := store.Publish(t.Context(), snapshotAt(revision+1)); err != nil {
			stopReaders()
			t.Fatalf("Publish: %v", err)
		}
	}
	stopReaders()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func startReaders(t *testing.T, store *memgraph.Store, failures chan<- string) func() {
	done := make(chan struct{})
	var readers, started sync.WaitGroup
	started.Add(readerCount)
	for range readerCount {
		readers.Go(func() {
			readUntilDone(t, store, done, failures, sync.OnceFunc(started.Done))
		})
	}
	started.Wait()
	return sync.OnceFunc(func() {
		close(done)
		readers.Wait()
	})
}

func readUntilDone(t *testing.T, store *memgraph.Store, done <-chan struct{}, failures chan<- string, ready func()) {
	defer ready()
	var seen uint64
	for {
		select {
		case <-done:
			return
		default:
		}
		latest, err := store.Latest(t.Context())
		if err != nil || latest.Revision < seen || latest.Nodes[0].UnansweredCalls != latest.Revision {
			failures <- "reader saw an error, a revision going back or a snapshot mixed from two revisions"
			return
		}
		seen = latest.Revision
		ready()
	}
}
