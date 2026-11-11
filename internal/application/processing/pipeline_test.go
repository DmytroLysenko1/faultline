package processing_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	firePeriod     = time.Second
	roomyHeld      = 1 << 30
	shortWait      = 300 * time.Millisecond
	restOfPeriod   = firePeriod - shortWait
	longPublish    = 3 * firePeriod / 2
	heartbeatAhead = 10 * time.Minute
)

var (
	errFeedBroken    = errors.New("feed broken")
	errPublishBroken = errors.New("publish broken")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

// answer is what one Receive returns after waiting `waited` on the fake clock.
type answer struct {
	deliveries []*processingtest.Delivery
	waited     time.Duration
	err        error
}

type scriptedFeed struct {
	clock   *fakeClock
	answers []answer
	calls   []receiveCall
}

type receiveCall struct {
	Limit   int
	MaxWait time.Duration
}

func (f *scriptedFeed) Receive(_ context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error) {
	f.calls = append(f.calls, receiveCall{Limit: limit, MaxWait: maxWait})
	if len(f.answers) == 0 {
		return nil, processing.ErrFeedClosed
	}
	next := f.answers[0]
	f.answers = f.answers[1:]
	f.clock.now = f.clock.now.Add(next.waited)
	return asDeliveries(next.deliveries), next.err
}

type blockingFeed struct {
	entered chan struct{}
}

func (f *blockingFeed) Receive(ctx context.Context, _ int, _ time.Duration) ([]processing.Delivery, error) {
	close(f.entered)
	<-ctx.Done()
	return nil, fmt.Errorf("receive: %w", ctx.Err())
}

func asDeliveries(fakes []*processingtest.Delivery) []processing.Delivery {
	if fakes == nil {
		return nil
	}
	deliveries := make([]processing.Delivery, 0, len(fakes))
	for _, fake := range fakes {
		deliveries = append(deliveries, fake)
	}
	return deliveries
}

type published struct {
	Calls         uint64
	Released      int
	Revision      uint64
	Observed      uint64
	EarlyPanes    uint64
	FinalRequests uint64
}

type recordingStore struct {
	watched      []*processingtest.Delivery
	fail         error
	failFrom     int
	cancel       context.CancelFunc
	published    []published
	afterPublish func(snapshot *topology.Snapshot)
	clock        *fakeClock
	takes        time.Duration
}

func (s *recordingStore) Publish(ctx context.Context, snapshot *topology.Snapshot) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.fail != nil && len(s.published) >= s.failFrom {
		return s.fail
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	s.published = append(s.published, published{
		Calls:         processingtest.TotalCalls(snapshot),
		Released:      releasedCount(s.watched),
		Revision:      snapshot.Revision,
		Observed:      snapshot.PairingStats.Observed,
		EarlyPanes:    snapshot.TrafficStats.Subjects.PanesEarly,
		FinalRequests: finalRequests(snapshot),
	})
	if s.clock != nil {
		s.clock.now = s.clock.now.Add(s.takes)
	}
	if s.afterPublish != nil {
		s.afterPublish(snapshot)
	}
	return nil
}

func finalRequests(snapshot *topology.Snapshot) uint64 {
	var requests uint64
	for i := range snapshot.Edges {
		requests += snapshot.Edges[i].Final.Requests
	}
	return requests
}

func releasedCount(deliveries []*processingtest.Delivery) int {
	var released int
	for _, watched := range deliveries {
		released += watched.Released()
	}
	return released
}

type scenario struct {
	nextTrace uint64
}

func (s *scenario) pair(t *testing.T) *processingtest.Delivery {
	t.Helper()
	created := processingtest.NewDelivery(processingtest.Pairs(t, s.nextTrace, 1))
	s.nextTrace++
	return created
}

// timedPair ends at the start of the run plus at, so traffic gives it a window and Fire a live pane.
func (s *scenario) timedPair(t *testing.T, at time.Duration) *processingtest.Delivery {
	t.Helper()
	created := processingtest.NewDelivery(processingtest.TimedPairs(t, s.nextTrace, 1, telemetrytest.Epoch().Add(at)))
	s.nextTrace++
	return created
}

func (s *scenario) callerHalf(t *testing.T) *processingtest.Delivery {
	t.Helper()
	created := processingtest.NewDelivery(processingtest.CallerHalf(t, s.nextTrace))
	s.nextTrace++
	return created
}

func iterationOf(deliveries ...*processingtest.Delivery) answer {
	return answer{
		deliveries: deliveries,
	}
}

func waitedFor(waited time.Duration, deliveries ...*processingtest.Delivery) answer {
	return answer{
		deliveries: deliveries,
		waited:     waited,
	}
}

func deliveriesOf(answers []answer) []*processingtest.Delivery {
	var all []*processingtest.Delivery
	for _, next := range answers {
		all = append(all, next.deliveries...)
	}
	return all
}

func cadence(maxHeld int64) processing.Cadence {
	return processing.Cadence{
		FirePeriod:   firePeriod,
		MaxHeldBytes: maxHeld,
	}
}

type runArgs struct {
	answers []answer
	store   *recordingStore
	maxHeld int64
}

func run(ctx context.Context, t *testing.T, args *runArgs) (*scriptedFeed, error) {
	t.Helper()
	clock := &fakeClock{now: telemetrytest.Epoch()}
	feed := &scriptedFeed{clock: clock, answers: args.answers}
	args.store.watched = deliveriesOf(args.answers)
	if args.store.takes > 0 {
		args.store.clock = clock
	}
	pipeline, err := processing.NewPipeline(feed, processingtest.NewBuilder(t), args.store, clock,
		cadence(cmp.Or(args.maxHeld, roomyHeld)))
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	return feed, pipeline.Run(ctx)
}

func TestNewPipelineRejectsAnInvalidCadence(t *testing.T) {
	tests := []struct {
		Name    string
		Args    processing.Cadence
		WantErr error
	}{
		{Name: "valid", Args: cadence(1)},
		{Name: "zero fire period", Args: processing.Cadence{MaxHeldBytes: 1}, WantErr: processing.ErrInvalidCadence},
		{
			Name:    "negative fire period",
			Args:    processing.Cadence{FirePeriod: -firePeriod, MaxHeldBytes: 1},
			WantErr: processing.ErrInvalidCadence,
		},
		{Name: "no room to hold a batch", Args: cadence(0), WantErr: processing.ErrInvalidCadence},
		{Name: "negative held bytes", Args: cadence(-1), WantErr: processing.ErrInvalidCadence},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := processing.NewPipeline(&scriptedFeed{}, processingtest.NewBuilder(t), &recordingStore{},
				&fakeClock{}, tt.Args)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("NewPipeline = %v, want %v", err, tt.WantErr)
			}
		})
	}
	if _, err := processing.NewPipeline(&scriptedFeed{}, processingtest.NewBuilder(t), &recordingStore{}, nil,
		cadence(1)); !errors.Is(err, processing.ErrInvalidCadence) {
		t.Errorf("NewPipeline with a nil clock = %v, want ErrInvalidCadence", err)
	}
}

func TestReceiveWaitsAtMostUntilTheNextFire(t *testing.T) {
	tests := []struct {
		Name string
		Args runArgs
		Want []time.Duration
	}{
		{
			Name: "the wait shrinks to the fire and starts over after it",
			Args: runArgs{
				answers: []answer{
					waitedFor(shortWait),
					waitedFor(restOfPeriod),
					waitedFor(firePeriod + shortWait),
				},
				store: &recordingStore{},
			},
			Want: []time.Duration{firePeriod, restOfPeriod, firePeriod, firePeriod},
		},
		{
			Name: "a publish longer than the period leaves no wait",
			Args: runArgs{
				answers: []answer{
					waitedFor(firePeriod),
				},
				store: &recordingStore{takes: longPublish},
			},
			Want: []time.Duration{firePeriod, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			feed, err := run(t.Context(), t, &tt.Args)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			waits := make([]time.Duration, 0, len(feed.calls))
			for _, call := range feed.calls {
				if call.Limit != processing.MaxBatchesPerIteration {
					t.Errorf("Receive limit %d, want %d", call.Limit, processing.MaxBatchesPerIteration)
				}
				waits = append(waits, call.MaxWait)
			}
			if diff := gocmp.Diff(tt.Want, waits); diff != "" {
				t.Errorf("Receive waits (-want +got):\n%s", diff)
			}
		})
	}
}

// TestPublishesOnlyOnStructureFireAndDrain: calls on a known edge wait for the next Fire, which
// shows them with their early panes; the drain publishes what is still held.
func TestPublishesOnlyOnStructureFireAndDrain(t *testing.T) {
	var batches scenario
	tick := firePeriod / 4
	answers := []answer{
		waitedFor(tick, batches.timedPair(t, 0)),
		waitedFor(tick, batches.timedPair(t, tick)),
		waitedFor(tick, batches.timedPair(t, 2*tick)),
		waitedFor(tick),
		waitedFor(tick, batches.timedPair(t, 4*tick)),
	}
	store := &recordingStore{}
	if _, err := run(t.Context(), t, &runArgs{answers: answers, store: store}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{Calls: 1, Released: 0, Revision: 1, Observed: 2},
		{Calls: 3, Released: 1, Revision: 2, Observed: 6, EarlyPanes: 2},
		{Calls: 4, Released: 3, Revision: 3, Observed: 8, EarlyPanes: 2},
	}
	if diff := gocmp.Diff(want, store.published); diff != "" {
		t.Errorf("published (-want +got):\n%s", diff)
	}
	if released := releasedCount(deliveriesOf(answers)); released != len(deliveriesOf(answers)) {
		t.Errorf("released %d of %d deliveries after the drain", released, len(deliveriesOf(answers)))
	}
}

// TestAHeartbeatClosesWindowsWithoutItsOwnPublish: a pulse is a journal record, not a reason to
// publish; the windows it closes reach readers with the next Fire, within one period.
func TestAHeartbeatClosesWindowsWithoutItsOwnPublish(t *testing.T) {
	var batches scenario
	tick := firePeriod / 4
	answers := []answer{
		waitedFor(tick, batches.timedPair(t, 0)),
		waitedFor(tick, processingtest.NewDelivery(batch.Heartbeat(telemetrytest.Epoch().Add(heartbeatAhead)))),
		waitedFor(firePeriod - 2*tick),
	}
	store := &recordingStore{}
	if _, err := run(t.Context(), t, &runArgs{answers: answers, store: store}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{Calls: 1, Released: 0, Revision: 1, Observed: 2},
		{Calls: 1, Released: 1, Revision: 2, Observed: 2, FinalRequests: 1},
		{Calls: 1, Released: 2, Revision: 2, Observed: 2, FinalRequests: 1},
	}
	if diff := gocmp.Diff(want, store.published); diff != "" {
		t.Errorf("published: the edge, then the Fire with the closed window, then the drain (-want +got):\n%s", diff)
	}
}

func TestAClockThatWentBackStillFires(t *testing.T) {
	var batches scenario
	answers := []answer{
		waitedFor(0, batches.timedPair(t, 0)),
		waitedFor(-time.Hour, batches.timedPair(t, 0)),
		waitedFor(firePeriod / 2),
	}
	store := &recordingStore{}
	if _, err := run(t.Context(), t, &runArgs{answers: answers, store: store}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{Calls: 1, Released: 0, Revision: 1, Observed: 2},
		{Calls: 2, Released: 1, Revision: 2, Observed: 4, EarlyPanes: 2},
		{Calls: 2, Released: 2, Revision: 2, Observed: 4, EarlyPanes: 2},
	}
	if diff := gocmp.Diff(want, store.published); diff != "" {
		t.Errorf("published: the edge, a Fire after the jump back, the drain (-want +got):\n%s", diff)
	}
}

func TestHeldBytesForceAPublishBeforeTheFire(t *testing.T) {
	var batches scenario
	first, second, third := batches.pair(t), batches.pair(t), batches.pair(t)
	answers := []answer{iterationOf(first), iterationOf(second), iterationOf(third)}
	store := &recordingStore{}
	args := runArgs{answers: answers, store: store, maxHeld: second.Bytes() + third.Bytes()}
	if _, err := run(t.Context(), t, &args); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{Calls: 1, Released: 0, Revision: 1, Observed: 2},
		{Calls: 3, Released: 1, Revision: 2, Observed: 6},
		{Calls: 3, Released: 3, Revision: 2, Observed: 6},
	}
	if diff := gocmp.Diff(want, store.published); diff != "" {
		t.Errorf("published: the structure, then the held bytes, then the drain (-want +got):\n%s", diff)
	}
}

func TestPublishPrecedesEveryRelease(t *testing.T) {
	var batches scenario
	iteration := iterationOf(batches.pair(t), batches.pair(t), batches.pair(t))
	store := &recordingStore{}
	if _, err := run(t.Context(), t, &runArgs{answers: []answer{iteration}, store: store}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{Calls: 3, Released: 0, Revision: 1, Observed: 6},
		{Calls: 3, Released: 3, Revision: 1, Observed: 6},
	}
	if diff := gocmp.Diff(want, store.published); diff != "" {
		t.Errorf("published (-want +got):\n%s", diff)
	}
	for index, delivered := range iteration.deliveries {
		if got := delivered.Released(); got != 1 {
			t.Errorf("delivery %d released %d times, want 1", index, got)
		}
	}
}

func TestSnapshotWithoutGraphChangeKeepsRevisionAndUpdatesStats(t *testing.T) {
	var batches scenario
	answers := []answer{
		iterationOf(batches.pair(t)),
		iterationOf(batches.callerHalf(t)),
	}
	store := &recordingStore{}
	if _, err := run(t.Context(), t, &runArgs{answers: answers, store: store}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{Calls: 1, Released: 0, Revision: 1, Observed: 2},
		{Calls: 1, Released: 1, Revision: 1, Observed: 3},
	}
	if diff := gocmp.Diff(want, store.published); diff != "" {
		t.Errorf("published: the edge, then the drain with the half (-want +got):\n%s", diff)
	}
}

type outcome struct {
	Released  int
	Published int
}

func TestRunStopsWithTheRightOutcome(t *testing.T) {
	type args struct {
		answers       func(t *testing.T) []answer
		publishErr    error
		failFrom      int
		cancelOnStore bool
	}
	single := func(t *testing.T) []answer {
		var batches scenario
		return []answer{iterationOf(batches.pair(t))}
	}
	tests := []struct {
		Name    string
		Args    args
		Want    outcome
		WantErr error
	}{
		{
			Name: "closed feed ends with the drain",
			Args: args{answers: single},
			Want: outcome{Released: 1, Published: 2},
		},
		{
			Name: "feed failure ignores deliveries returned with it",
			Args: args{
				answers: func(t *testing.T) []answer {
					var batches scenario
					failed := iterationOf(batches.pair(t))
					failed.err = errFeedBroken
					return []answer{failed}
				},
			},
			WantErr: errFeedBroken,
		},
		{
			Name: "feed reporting canceled while the run is live is a failure",
			Args: args{
				answers: func(*testing.T) []answer {
					return []answer{{err: context.Canceled}}
				},
			},
			WantErr: context.Canceled,
		},
		{
			Name: "empty answer publishes nothing and the run goes on",
			Args: args{
				answers: func(t *testing.T) []answer {
					var batches scenario
					return []answer{iterationOf(), iterationOf(batches.pair(t))}
				},
			},
			Want: outcome{Released: 1, Published: 2},
		},
		{
			Name: "run canceled during publish keeps the iteration unreleased",
			Args: args{answers: single, cancelOnStore: true},
		},
		{
			Name:    "publish failure keeps the iteration unreleased",
			Args:    args{answers: single, publishErr: errPublishBroken},
			WantErr: errPublishBroken,
		},
		{
			Name:    "publish deadline is a failure, not a stop",
			Args:    args{answers: single, publishErr: context.DeadlineExceeded},
			WantErr: context.DeadlineExceeded,
		},
		{
			Name: "failed drain keeps what it held",
			Args: args{
				answers: func(t *testing.T) []answer {
					var batches scenario
					return []answer{iterationOf(batches.pair(t)), iterationOf(batches.pair(t))}
				},
				publishErr: errPublishBroken,
				failFrom:   1,
			},
			Want:    outcome{Released: 1, Published: 1},
			WantErr: errPublishBroken,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			answers := tt.Args.answers(t)
			store := &recordingStore{fail: tt.Args.publishErr, failFrom: tt.Args.failFrom}
			if tt.Args.cancelOnStore {
				store.cancel = cancel
			}
			_, err := run(ctx, t, &runArgs{answers: answers, store: store})
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("Run = %v, want %v", err, tt.WantErr)
			}
			got := outcome{Released: releasedCount(deliveriesOf(answers)), Published: len(store.published)}
			if diff := gocmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
		})
	}
}

func newBlockedPipeline(t *testing.T, feed *blockingFeed) *processing.Pipeline {
	t.Helper()
	return processingtest.NewPipeline(t, feed, processingtest.NewBuilder(t), &recordingStore{},
		processingtest.StoppedClock{})
}

func startBlocked(t *testing.T, pipeline *processing.Pipeline, feed *blockingFeed) (context.CancelFunc, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	var result error
	var running sync.WaitGroup
	running.Go(func() {
		result = pipeline.Run(ctx)
	})
	<-feed.entered
	return cancel, func() error {
		running.Wait()
		return result
	}
}

func TestCancelWhileWaitingIsANormalStop(t *testing.T) {
	feed := &blockingFeed{entered: make(chan struct{})}
	cancel, wait := startBlocked(t, newBlockedPipeline(t, feed), feed)
	cancel()
	if err := wait(); err != nil {
		t.Errorf("Run after cancel = %v, want nil", err)
	}
}

func TestConcurrentSecondRunIsRejected(t *testing.T) {
	feed := &blockingFeed{entered: make(chan struct{})}
	pipeline := newBlockedPipeline(t, feed)
	cancel, wait := startBlocked(t, pipeline, feed)
	second := pipeline.Run(t.Context())
	cancel()
	if err := wait(); err != nil {
		t.Errorf("first Run = %v, want nil after cancel", err)
	}
	if !errors.Is(second, processing.ErrAlreadyStarted) {
		t.Errorf("second Run = %v, want ErrAlreadyStarted", second)
	}
}

func TestSecondRunAfterExitIsRejected(t *testing.T) {
	var batches scenario
	feed := &scriptedFeed{clock: &fakeClock{}}
	store := &recordingStore{}
	pipeline := processingtest.NewPipeline(t, feed, processingtest.NewBuilder(t), store, feed.clock)
	if err := pipeline.Run(t.Context()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	drained := len(store.published)
	late := batches.pair(t)
	feed.answers = []answer{iterationOf(late)}
	if err := pipeline.Run(t.Context()); !errors.Is(err, processing.ErrAlreadyStarted) {
		t.Fatalf("second Run = %v, want ErrAlreadyStarted", err)
	}
	if late.Released() != 0 || len(store.published) != drained {
		t.Errorf("second Run touched the feed: released %d, published %d more", late.Released(),
			len(store.published)-drained)
	}
}

func TestPublishedSnapshotIsACopy(t *testing.T) {
	var batches scenario
	answers := []answer{
		iterationOf(batches.pair(t)),
		iterationOf(batches.pair(t)),
		iterationOf(batches.callerHalf(t)),
	}
	store := &recordingStore{afterPublish: corrupt}
	if _, err := run(t.Context(), t, &runArgs{answers: answers, store: store}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := make([]uint64, 0, len(store.published))
	for _, snapshot := range store.published {
		calls = append(calls, snapshot.Calls)
	}
	if diff := gocmp.Diff([]uint64{1, 2}, calls); diff != "" {
		t.Errorf("calls per snapshot after readers corrupted earlier ones (-want +got):\n%s", diff)
	}
}

func corrupt(snapshot *topology.Snapshot) {
	for index := range snapshot.Edges {
		snapshot.Edges[index].Calls = 0
	}
	for index := range snapshot.Nodes {
		snapshot.Nodes[index].UnansweredCalls = 0
	}
}
