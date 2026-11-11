package pipeline_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
)

const (
	atomicityIterations = 200
	callsPerBatch       = 7
	minBatchesPerRound  = 3
	batchesSpread       = 3
	readerCount         = 4
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type progress struct {
	seenRevision atomic.Uint64
	advanced     chan struct{}
	failed       chan struct{}
	failOnce     sync.Once
}

func newProgress() *progress {
	return &progress{
		advanced: make(chan struct{}, 1),
		failed:   make(chan struct{}),
	}
}

func (p *progress) saw(revision uint64) {
	for {
		seen := p.seenRevision.Load()
		if revision <= seen {
			return
		}
		if p.seenRevision.CompareAndSwap(seen, revision) {
			break
		}
	}
	select {
	case p.advanced <- struct{}{}:
	default:
	}
}

func (p *progress) fail() {
	p.failOnce.Do(func() {
		close(p.failed)
	})
}

func (p *progress) waitFor(ctx context.Context, revision uint64) error {
	for p.seenRevision.Load() < revision {
		select {
		case <-p.advanced:
		case <-p.failed:
			return processing.ErrFeedClosed
		case <-ctx.Done():
			return fmt.Errorf("wait for readers: %w", ctx.Err())
		}
	}
	return nil
}

type observedFeed struct {
	iterations [][]processing.Delivery
	handedOut  uint64
	readers    *progress
	clock      *processingtest.SteppingClock
}

func (f *observedFeed) Receive(ctx context.Context, _ int, _ time.Duration) ([]processing.Delivery, error) {
	if err := f.readers.waitFor(ctx, f.handedOut); err != nil {
		return nil, err
	}
	if len(f.iterations) == 0 {
		return nil, processing.ErrFeedClosed
	}
	next := f.iterations[0]
	f.iterations = f.iterations[1:]
	f.handedOut++
	f.clock.Advance()
	return next, nil
}

type atomicityScenario struct {
	feed       *observedFeed
	totals     map[uint64]bool
	finalCalls uint64
}

func newAtomicityScenario(t *testing.T, readers *progress) atomicityScenario {
	t.Helper()
	scenario := atomicityScenario{
		feed: &observedFeed{
			readers: readers,
			clock: &processingtest.SteppingClock{
				At:   telemetrytest.Epoch(),
				Step: processingtest.Cadence().FirePeriod,
			},
		},
		totals: map[uint64]bool{
			0: true,
		},
	}
	var trace, batches uint64
	for round := range atomicityIterations {
		var iteration []processing.Delivery
		for range minBatchesPerRound + round%batchesSpread {
			end := telemetrytest.Epoch().Add(time.Duration(round) * processingtest.Cadence().FirePeriod)
			iteration = append(iteration, processingtest.NewDelivery(processingtest.TimedPairs(t, trace, callsPerBatch, end)))
			trace += callsPerBatch
			batches++
		}
		scenario.feed.iterations = append(scenario.feed.iterations, iteration)
		scenario.totals[batches*callsPerBatch] = true
	}
	scenario.finalCalls = batches * callsPerBatch
	return scenario
}

type snapshotChecker struct {
	totals        map[uint64]bool
	callsRevision map[uint64]string
	lastRevision  uint64
}

// contentOf is everything a revision stands for: graph counts and the live and final summaries.
// The watermark and the stats are diagnostics and are left out (I2).
func contentOf(snapshot *topology.Snapshot) string {
	var content strings.Builder
	for i := range snapshot.Nodes {
		node := &snapshot.Nodes[i]
		fmt.Fprintf(&content, "%s %d %+v %+v;", node.ID, node.Served, node.Live, node.Final)
	}
	for i := range snapshot.Edges {
		edge := &snapshot.Edges[i]
		fmt.Fprintf(&content, "%s→%s %d %+v %+v;", edge.Source, edge.Target, edge.Calls, edge.Live, edge.Final)
	}
	return content.String()
}

func summariesHoldTheirRules(snapshot *topology.Snapshot) bool {
	summaries := make([]red.WindowStats, 0, 2*(len(snapshot.Nodes)+len(snapshot.Edges)))
	for i := range snapshot.Nodes {
		summaries = append(summaries, snapshot.Nodes[i].Live, snapshot.Nodes[i].Final)
	}
	for i := range snapshot.Edges {
		summaries = append(summaries, snapshot.Edges[i].Live, snapshot.Edges[i].Final)
	}
	for i := range summaries {
		stats := &summaries[i]
		if stats.Failed > stats.Requests || stats.P50 > stats.P95 || stats.P95 > stats.P99 {
			return false
		}
	}
	return true
}

func (c *snapshotChecker) check(t *testing.T, snapshot *topology.Snapshot) bool {
	t.Helper()
	calls := processingtest.TotalCalls(snapshot)
	if !c.totals[calls] {
		t.Errorf("revision %d shows %d calls, not the total of any whole iteration", snapshot.Revision, calls)
		return false
	}
	content := contentOf(snapshot)
	if seen, found := c.callsRevision[snapshot.Revision]; found && seen != content {
		t.Errorf("revision %d showed other graph or RED data before:\n%s\nnow:\n%s", snapshot.Revision, seen, content)
		return false
	}
	if !summariesHoldTheirRules(snapshot) {
		t.Errorf("revision %d has a summary that breaks its rules:\n%s", snapshot.Revision, content)
		return false
	}
	if snapshot.Revision < c.lastRevision {
		t.Errorf("revision went back from %d to %d", c.lastRevision, snapshot.Revision)
		return false
	}
	if paired := snapshot.PairingStats.Matched - snapshot.GraphStats.GraphOverflow; paired != calls {
		t.Errorf("revision %d: %d calls but matched − overflow = %d", snapshot.Revision, calls, paired)
		return false
	}
	c.callsRevision[snapshot.Revision] = content
	c.lastRevision = snapshot.Revision
	return true
}

func read(t *testing.T, store *memgraph.Store, scenario atomicityScenario, readers *progress, done <-chan struct{}) {
	t.Helper()
	checker := &snapshotChecker{
		totals:        scenario.totals,
		callsRevision: map[uint64]string{},
	}
	for {
		select {
		case <-done:
			return
		default:
		}
		snapshot, err := store.Latest(t.Context())
		if err != nil {
			t.Errorf("Latest: %v", err)
			readers.fail()
			return
		}
		if !checker.check(t, snapshot) {
			readers.fail()
			return
		}
		readers.saw(snapshot.Revision)
	}
}

func TestSnapshotAtomicity(t *testing.T) {
	readers := newProgress()
	scenario := newAtomicityScenario(t, readers)
	builder := processingtest.NewBuilder(t)
	store, err := memgraph.New(builder.Snapshot())
	if err != nil {
		t.Fatalf("memgraph.New: %v", err)
	}
	done := make(chan struct{})
	var reading sync.WaitGroup
	for range readerCount {
		reading.Go(func() {
			read(t, store, scenario, readers, done)
		})
	}
	runErr := processingtest.NewPipeline(t, scenario.feed, builder, store, scenario.feed.clock).Run(t.Context())
	close(done)
	reading.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if seen := readers.seenRevision.Load(); seen < atomicityIterations-1 {
		t.Errorf("readers saw revisions up to %d, want every one of %d iterations", seen, atomicityIterations)
	}
	final, err := store.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if calls := processingtest.TotalCalls(final); calls != scenario.finalCalls {
		t.Errorf("final snapshot has %d calls, want %d", calls, scenario.finalCalls)
	}
	if len(final.Edges) == 0 || final.Edges[0].Final.Requests == 0 || final.Edges[0].Live.Requests == 0 {
		t.Errorf("readers checked no RED: the final snapshot shows no live and final summary: %+v", final.Edges)
	}
}
