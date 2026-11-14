package retry_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	attemptBudget = time.Second
	firstWait     = 100 * time.Millisecond
	maxWait       = 300 * time.Millisecond
	gap           = time.Second
)

var (
	errTemporary = errors.New("temporary")
	errFinal     = errors.New("final")
)

func policy() *retry.Policy {
	return &retry.Policy{
		Attempt:   attemptBudget,
		FirstWait: firstWait,
		MaxWait:   maxWait,
		Retryable: func(err error) bool { return errors.Is(err, errTemporary) },
	}
}

// answers replies with each error in turn and keeps when each attempt began and how long it could run.
type answers struct {
	replies   []error
	started   []time.Duration
	deadlines []time.Duration
	origin    time.Time
}

func (a *answers) call(attempt context.Context) error {
	a.started = append(a.started, time.Since(a.origin))
	deadline, _ := attempt.Deadline()
	a.deadlines = append(a.deadlines, time.Until(deadline))
	reply := a.replies[0]
	a.replies = a.replies[1:]
	return reply
}

func TestDoRetriesOnlyWhatIsRetryableWithAGrowingGap(t *testing.T) {
	tests := []struct {
		name        string
		replies     []error
		wantErr     error
		wantStarted []time.Duration
	}{
		{name: "the first attempt succeeds", replies: []error{nil}, wantStarted: []time.Duration{0}},
		{name: "a final error is not retried", replies: []error{errFinal}, wantErr: errFinal,
			wantStarted: []time.Duration{0}},
		{
			name:    "temporary errors back off to the cap",
			replies: []error{errTemporary, errTemporary, errTemporary, nil},
			wantStarted: []time.Duration{0, firstWait, firstWait + 2*firstWait,
				firstWait + 2*firstWait + maxWait},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := &answers{replies: tt.replies, origin: time.Now()}

				err := retry.Do(t.Context(), policy(), calls.call)

				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Do = %v, want %v", err, tt.wantErr)
				}
				if diff := cmp.Diff(tt.wantStarted, calls.started); diff != "" {
					t.Errorf("attempt starts mismatch (-want +got):\n%s", diff)
				}
				for index, budget := range calls.deadlines {
					if budget != attemptBudget {
						t.Errorf("attempt %d had %s, want %s", index, budget, attemptBudget)
					}
				}
			})
		})
	}
}

func TestDoGivesUpWithTheLastErrorWhenItsContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), firstWait/2)
		defer cancel()
		calls := &answers{replies: []error{errTemporary, errTemporary}, origin: time.Now()}

		err := retry.Do(ctx, policy(), calls.call)

		if !errors.Is(err, errTemporary) || !errors.Is(err, context.DeadlineExceeded) || len(calls.started) != 1 {
			t.Errorf("Do = %v after %d attempts, want the temporary error and the deadline after one", err,
				len(calls.started))
		}
	})
}

func TestSleepWaitsOrEndsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		if err := retry.Sleep(t.Context(), gap); err != nil || time.Since(started) != gap {
			t.Errorf("Sleep = %v after %s, want nil after %s", err, time.Since(started), gap)
		}
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(gap/2, cancel)
		started = time.Now()

		err := retry.Sleep(ctx, gap)

		if !errors.Is(err, context.Canceled) || time.Since(started) != gap/2 {
			t.Errorf("Sleep = %v after %s, want Canceled at %s", err, time.Since(started), gap/2)
		}
	})
}

func TestSleepOnAnEndedContextNeverReportsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := retry.Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("Sleep on a canceled context = %v, want Canceled", err)
	}
}
