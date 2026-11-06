package processing_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

var (
	errFeedBroken    = errors.New("feed broken")
	errPublishBroken = errors.New("publish broken")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type answer struct {
	deliveries []*processingtest.Delivery
	err        error
}

type scriptedFeed struct {
	answers []answer
	limits  []int
}

func (f *scriptedFeed) Receive(_ context.Context, limit int) ([]processing.Delivery, error) {
	f.limits = append(f.limits, limit)
	if len(f.answers) == 0 {
		return nil, processing.ErrFeedClosed
	}
	next := f.answers[0]
	f.answers = f.answers[1:]
	return asDeliveries(next.deliveries), next.err
}

type blockingFeed struct {
	entered chan struct{}
}

func (f *blockingFeed) Receive(ctx context.Context, _ int) ([]processing.Delivery, error) {
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
	Calls    uint64
	Released int
	Revision uint64
	Observed uint64
}

type recordingStore struct {
	watched      []*processingtest.Delivery
	fail         error
	cancel       context.CancelFunc
	published    []published
	afterPublish func(snapshot *topology.Snapshot)
}

func (s *recordingStore) Publish(ctx context.Context, snapshot *topology.Snapshot) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.fail != nil {
		return s.fail
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	s.published = append(s.published, published{
		Calls:    processingtest.TotalCalls(snapshot),
		Released: releasedCount(s.watched),
		Revision: snapshot.Revision,
		Observed: snapshot.PairingStats.Observed,
	})
	if s.afterPublish != nil {
		s.afterPublish(snapshot)
	}
	return nil
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

func deliveriesOf(answers []answer) []*processingtest.Delivery {
	var all []*processingtest.Delivery
	for _, next := range answers {
		all = append(all, next.deliveries...)
	}
	return all
}

func run(ctx context.Context, t *testing.T, answers []answer, store *recordingStore) (*scriptedFeed, error) {
	t.Helper()
	feed := &scriptedFeed{
		answers: answers,
	}
	store.watched = deliveriesOf(answers)
	return feed, processing.NewPipeline(feed, processingtest.NewBuilder(t), store).Run(ctx)
}

func TestIterationPublishesEveryBatchBeforeReleasingAny(t *testing.T) {
	var batches scenario
	iteration := iterationOf(batches.pair(t), batches.pair(t), batches.pair(t))
	store := &recordingStore{}
	feed, err := run(t.Context(), t, []answer{
		iteration,
	}, store)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{
			Calls:    3,
			Released: 0,
			Revision: 1,
			Observed: 6,
		},
	}
	if diff := cmp.Diff(want, store.published); diff != "" {
		t.Errorf("published (-want +got):\n%s", diff)
	}
	for index, delivered := range iteration.deliveries {
		if got := delivered.Released(); got != 1 {
			t.Errorf("delivery %d released %d times, want 1", index, got)
		}
	}
	wantLimits := []int{
		processing.MaxBatchesPerIteration,
		processing.MaxBatchesPerIteration,
	}
	if diff := cmp.Diff(wantLimits, feed.limits); diff != "" {
		t.Errorf("Receive limits (-want +got):\n%s", diff)
	}
}

func TestIterationWithoutGraphChangeKeepsRevisionAndUpdatesStats(t *testing.T) {
	var batches scenario
	answers := []answer{
		iterationOf(batches.pair(t)),
		iterationOf(batches.callerHalf(t)),
	}
	store := &recordingStore{}
	if _, err := run(t.Context(), t, answers, store); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []published{
		{
			Calls:    1,
			Released: 0,
			Revision: 1,
			Observed: 2,
		},
		{
			Calls:    1,
			Released: 1,
			Revision: 1,
			Observed: 3,
		},
	}
	if diff := cmp.Diff(want, store.published); diff != "" {
		t.Errorf("published (-want +got):\n%s", diff)
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
		cancelOnStore bool
	}
	single := func(t *testing.T) []answer {
		var batches scenario
		return []answer{
			iterationOf(batches.pair(t)),
		}
	}
	tests := []struct {
		Name    string
		Args    args
		Want    outcome
		WantErr error
	}{
		{
			Name: "closed feed ends the drain after the last iteration",
			Args: args{
				answers: single,
			},
			Want: outcome{
				Released:  1,
				Published: 1,
			},
		},
		{
			Name: "feed failure ignores deliveries returned with it",
			Args: args{
				answers: func(t *testing.T) []answer {
					var batches scenario
					failed := iterationOf(batches.pair(t))
					failed.err = errFeedBroken
					return []answer{
						failed,
					}
				},
			},
			WantErr: errFeedBroken,
		},
		{
			Name: "feed reporting canceled while the run is live is a failure",
			Args: args{
				answers: func(*testing.T) []answer {
					return []answer{
						{
							err: context.Canceled,
						},
					}
				},
			},
			WantErr: context.Canceled,
		},
		{
			Name: "empty answer breaks the feed contract",
			Args: args{
				answers: func(*testing.T) []answer {
					return []answer{
						iterationOf(),
					}
				},
			},
			WantErr: processing.ErrEmptyReceive,
		},
		{
			Name: "run canceled during publish keeps the iteration unreleased",
			Args: args{
				answers:       single,
				cancelOnStore: true,
			},
		},
		{
			Name: "publish failure keeps the iteration unreleased",
			Args: args{
				answers:    single,
				publishErr: errPublishBroken,
			},
			WantErr: errPublishBroken,
		},
		{
			Name: "publish deadline is a failure, not a stop",
			Args: args{
				answers:    single,
				publishErr: context.DeadlineExceeded,
			},
			WantErr: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			answers := tt.Args.answers(t)
			store := &recordingStore{
				fail: tt.Args.publishErr,
			}
			if tt.Args.cancelOnStore {
				store.cancel = cancel
			}
			_, err := run(ctx, t, answers, store)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("Run = %v, want %v", err, tt.WantErr)
			}
			got := outcome{
				Released:  releasedCount(deliveriesOf(answers)),
				Published: len(store.published),
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("outcome (-want +got):\n%s", diff)
			}
		})
	}
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
	feed := &blockingFeed{
		entered: make(chan struct{}),
	}
	cancel, wait := startBlocked(t, processing.NewPipeline(feed, processingtest.NewBuilder(t), &recordingStore{}), feed)
	cancel()
	if err := wait(); err != nil {
		t.Errorf("Run after cancel = %v, want nil", err)
	}
}

func TestConcurrentSecondRunIsRejected(t *testing.T) {
	feed := &blockingFeed{
		entered: make(chan struct{}),
	}
	pipeline := processing.NewPipeline(feed, processingtest.NewBuilder(t), &recordingStore{})
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
	feed := &scriptedFeed{}
	store := &recordingStore{}
	pipeline := processing.NewPipeline(feed, processingtest.NewBuilder(t), store)
	if err := pipeline.Run(t.Context()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	late := batches.pair(t)
	feed.answers = []answer{
		iterationOf(late),
	}
	if err := pipeline.Run(t.Context()); !errors.Is(err, processing.ErrAlreadyStarted) {
		t.Fatalf("second Run = %v, want ErrAlreadyStarted", err)
	}
	if late.Released() != 0 || len(store.published) != 0 {
		t.Errorf("second Run touched the feed: released %d, published %d", late.Released(), len(store.published))
	}
}

func TestPublishedSnapshotIsACopy(t *testing.T) {
	var batches scenario
	answers := []answer{
		iterationOf(batches.pair(t)),
		iterationOf(batches.pair(t)),
		iterationOf(batches.callerHalf(t)),
	}
	store := &recordingStore{
		afterPublish: corrupt,
	}
	if _, err := run(t.Context(), t, answers, store); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := make([]uint64, 0, len(store.published))
	for _, snapshot := range store.published {
		calls = append(calls, snapshot.Calls)
	}
	if diff := cmp.Diff([]uint64{1, 2, 2}, calls); diff != "" {
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
