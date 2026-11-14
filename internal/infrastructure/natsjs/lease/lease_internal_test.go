package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	productionTTL      = 25 * time.Second
	productionRenew    = 2500 * time.Millisecond
	productionRetry    = time.Second
	productionAttempt  = time.Second
	productionLost     = 20 * time.Second
	testMaxBytes       = 1 << 30
	testWindow         = 5 * time.Minute
	firstVersion       = 1
	onePartition       = 1
	oneReplica         = 1
	firstRevision      = 1
	instance           = "processor-0"
	foreignInstance    = "processor-1/other"
	justBefore         = 500 * time.Millisecond
	staleOwnRevision   = 5
	renewalsInAMinute  = 25
	renewalsAfterAdopt = 12
	longerMaxAge       = 30 * time.Second
)

func productionOptions() *Options {
	return &Options{
		Topology: natsjs.Topology{
			Partitions:           onePartition,
			Replicas:             oneReplica,
			MaxBytesPerPartition: testMaxBytes,
			DuplicateWindow:      testWindow,
			Compression:          natsjs.CompressionNone,
			ConfigVersion:        firstVersion,
		},
		Instance:    instance,
		TTL:         productionTTL,
		RenewEvery:  productionRenew,
		RetryEvery:  productionRetry,
		AttemptTime: productionAttempt,
		LostAfter:   productionLost,
		Logger:      slog.New(slog.DiscardHandler),
	}
}

func claimed(t *testing.T, store *fakeStore) *Lease {
	t.Helper()
	return claimedWith(t, store, productionOptions())
}

func claimedWith(t *testing.T, store *fakeStore, options *Options) *Lease {
	t.Helper()
	lease, err := claim(t.Context(), store, store.maxAge, options)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return lease
}

// running starts Run and returns how to stop it: cancel its context and wait for what it returned.
func running(t *testing.T, lease *Lease) (stop func() error, returned <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- lease.Run(ctx) }()
	return func() error {
		cancel()
		return <-done
	}, done
}

func lost(lease *Lease) bool {
	select {
	case <-lease.Lost():
		return true
	default:
		return false
	}
}

func TestARenewingHolderStaysValidAndKeepsItsKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		lease := claimed(t, store)
		stop, _ := running(t, lease)

		time.Sleep(time.Minute + time.Second)
		synctest.Wait()

		want := Stats{Held: true, Epoch: firstRevision, Renewals: renewalsInAMinute, RenewAge: time.Second}
		if diff := cmp.Diff(want, lease.Stats()); diff != "" {
			t.Errorf("stats (-want +got):\n%s", diff)
		}
		if held, found := store.held(); !found || !bytes.Equal(held.value, lease.token) {
			t.Errorf("key holds %q (found %t), want the lease's token", held.value, found)
		}
		if err := stop(); err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

// TestALeaseThatCannotRenewIsInvalidAfterLostAfterAndLostAfterTheTTL: whatever keeps renewals from
// succeeding — a KV that never answers, a gone bucket, another holder, a stale read of an own write —
// Valid ends LostAfter after the last successful renewal was sent and the lease is lost at the TTL,
// when the key has expired on the server.
func TestALeaseThatCannotRenewIsInvalidAfterLostAfterAndLostAfterTheTTL(t *testing.T) {
	tests := []struct {
		Name    string
		Arrange func(store *fakeStore, lease *Lease)
	}{
		{Name: "the KV never answers", Arrange: func(store *fakeStore, _ *Lease) { store.failAlways(hang) }},
		{Name: "the bucket is gone", Arrange: func(store *fakeStore, _ *Lease) { store.failAlways(noStream) }},
		{Name: "another processor holds the key", Arrange: func(store *fakeStore, _ *Lease) {
			store.takeOver([]byte(foreignInstance))
		}},
		{Name: "a stale read shows an own write over another holder", Arrange: func(store *fakeStore, lease *Lease) {
			store.takeOver([]byte(foreignInstance))
			store.readStale(entry{value: lease.token, revision: staleOwnRevision})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := newFakeStore(productionTTL)
				lease := claimed(t, store)
				tt.Arrange(store, lease)
				_, returned := running(t, lease)

				time.Sleep(productionLost - justBefore)
				validBefore := lease.Valid()
				time.Sleep(justBefore)
				validAt := lease.Valid()
				time.Sleep(productionTTL - productionLost - justBefore)
				synctest.Wait()
				lostBefore := lost(lease)
				time.Sleep(justBefore)
				synctest.Wait()

				got := []bool{validBefore, validAt, lostBefore, lost(lease)}
				if diff := cmp.Diff([]bool{true, false, false, true}, got); diff != "" {
					t.Errorf("valid before, valid at LostAfter, lost before, lost at the TTL (-want +got):\n%s", diff)
				}
				if err := <-returned; !errors.Is(err, ErrLeaseLost) {
					t.Errorf("Run = %v, want ErrLeaseLost", err)
				}
				if renewals := lease.Stats().Renewals; renewals != 1 {
					t.Errorf("renewals = %d, want only the claim", renewals)
				}
			})
		})
	}
}

func TestARenewalThatLandedWithItsAnswerLostIsAdoptedNotTakenForALoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		lease := claimed(t, store)
		store.failNext(applyAndHang)
		stop, _ := running(t, lease)

		time.Sleep(30 * time.Second)
		synctest.Wait()

		want := Stats{
			Held:          true,
			Epoch:         firstRevision,
			Renewals:      renewalsAfterAdopt,
			RenewFailures: 2,
			LostWrites:    1,
			RenewAge:      justBefore,
		}
		if diff := cmp.Diff(want, lease.Stats()); diff != "" {
			t.Errorf("stats (-want +got):\n%s", diff)
		}
		if lost(lease) {
			t.Error("the lease was lost over a renewal whose answer was lost")
		}
		if err := stop(); err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func TestStoppingRunReleasesTheKeyAndEndsValidAtOnce(t *testing.T) {
	tests := []struct {
		Name    string
		Arrange func(store *fakeStore)
	}{
		{Name: "after a renewal", Arrange: func(*fakeStore) {}},
		{Name: "while a renewal that landed waits for its lost answer", Arrange: func(store *fakeStore) {
			store.failNext(applyAndHang)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := newFakeStore(productionTTL)
				lease := claimed(t, store)
				tt.Arrange(store)
				stop, _ := running(t, lease)
				time.Sleep(productionRenew + justBefore)

				err := stop()

				if err != nil {
					t.Errorf("Run: %v", err)
				}
				if lease.Valid() {
					t.Error("the lease is still valid after Run returned")
				}
				if held, found := store.held(); found {
					t.Errorf("key still holds revision %d after the release", held.revision)
				}
			})
		})
	}
}

func TestAClaimWhoseCreateLandedWithItsAnswerLostTakesTheKeyWithoutWaitingForTheTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		store.failNext(applyAndHang)
		start := time.Now()

		lease := claimed(t, store)

		want := Stats{Held: true, Epoch: 2, Renewals: 1, LostWrites: 1}
		if diff := cmp.Diff(want, lease.Stats()); diff != "" {
			t.Errorf("stats (-want +got):\n%s", diff)
		}
		if waited := time.Since(start); waited != productionAttempt {
			t.Errorf("claimed after %s, want one attempt of %s", waited, productionAttempt)
		}
	})
}

func TestAClaimWaitsForTheOtherHolderAndTakesAHigherEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		other := store.takeOver([]byte(foreignInstance))
		start := time.Now()

		lease := claimed(t, store)

		if waited := time.Since(start); waited != productionTTL {
			t.Errorf("claimed after %s, want when the other key expires, %s", waited, productionTTL)
		}
		if lease.Epoch <= other {
			t.Errorf("epoch %d, want above the other holder's %d", lease.Epoch, other)
		}
	})
}

func TestAClaimEndsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		store.takeOver([]byte(foreignInstance))
		ctx, cancel := context.WithTimeout(t.Context(), productionLost)
		defer cancel()

		_, err := claim(ctx, store, productionTTL, productionOptions())

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("claim = %v, want the context's deadline", err)
		}
	})
}

func TestOptionsRefuseTimingsThatCannotKeepOneHolder(t *testing.T) {
	tests := []struct {
		Name    string
		Args    func(*Options)
		WantErr error
	}{
		{Name: "production timings", Args: func(*Options) {}},
		{Name: "no instance", Args: func(o *Options) { o.Instance = "" }, WantErr: ErrInvalidOptions},
		{Name: "invalid topology", Args: func(o *Options) { o.Topology.Replicas = 2 }, WantErr: ErrInvalidOptions},
		{Name: "partition outside the topology", Args: func(o *Options) { o.Partition = onePartition },
			WantErr: ErrInvalidOptions},
		{Name: "zero timing", Args: func(o *Options) { o.RetryEvery = 0 }, WantErr: ErrInvalidOptions},
		{Name: "attempt longer than the retry", Args: func(o *Options) { o.AttemptTime = 2 * time.Second },
			WantErr: ErrInvalidOptions},
		{Name: "key outlives Valid by less than the clock margin", Args: func(o *Options) { o.TTL = 24 * time.Second },
			WantErr: ErrInvalidOptions},
		{Name: "valid rides out less than an election", Args: func(o *Options) { o.RenewEvery = 4 * time.Second },
			WantErr: ErrInvalidOptions},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			options := productionOptions()
			tt.Args(options)

			err := options.validate()

			if !errors.Is(err, tt.WantErr) {
				t.Errorf("validate = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

// TestValidEndsBeforeTheReleaseDeletesTheKey: a delete that hangs must not leave the lease valid —
// whatever checks Valid while the release waits would act after the key may already be gone.
func TestValidEndsBeforeTheReleaseDeletesTheKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		lease := claimed(t, store)
		stop, _ := running(t, lease)
		time.Sleep(productionRenew + justBefore)
		store.failNext(hang)
		stopped := make(chan error, 1)

		go func() { stopped <- stop() }()
		synctest.Wait()
		validWhileDeleting := lease.Valid()
		_, keyThere := store.held()
		err := <-stopped

		if validWhileDeleting || !keyThere {
			t.Errorf("while the delete hangs: valid %t, key there %t; want invalid with the key still there",
				validWhileDeleting, keyThere)
		}
		if err != nil || lease.Stats().ReleaseFailures != 1 {
			t.Errorf("Run = %v with %d release failures, want nil and the failed delete counted", err,
				lease.Stats().ReleaseFailures)
		}
	})
}

func TestAStaleReadOfAnOlderOwnWriteIsNotAdopted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		lease := claimed(t, store)
		stop, _ := running(t, lease)
		time.Sleep(productionRenew + justBefore)
		store.failNext(applyAndHang)
		store.readStaleOnce(entry{value: lease.token, revision: firstRevision})

		time.Sleep(30 * time.Second)
		synctest.Wait()

		if stats := lease.Stats(); stats.LostWrites != 1 || !stats.Held {
			t.Errorf("lost writes %d, held %t; want only the landed renewal adopted and the lease held",
				stats.LostWrites, stats.Held)
		}
		if err := stop(); err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func TestALeaseIsRunOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lease := claimed(t, newFakeStore(productionTTL))
		stop, _ := running(t, lease)
		synctest.Wait()

		second := lease.Run(t.Context())

		if !errors.Is(second, ErrAlreadyRunning) {
			t.Errorf("second Run = %v, want ErrAlreadyRunning", second)
		}
		if err := stop(); err != nil {
			t.Errorf("first Run: %v", err)
		}
	})
}

// TestAClaimThatEndsAfterItsCreateLandedDeletesTheKey: a create whose answer was lost when ctx ended
// would otherwise block the partition for a TTL on behalf of a process that never got the lease.
func TestAClaimThatEndsAfterItsCreateLandedDeletesTheKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		store.failNext(applyAndHang)
		ctx, cancel := context.WithTimeout(t.Context(), justBefore)
		defer cancel()

		_, err := claim(ctx, store, productionTTL, productionOptions())

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("claim = %v, want the context's deadline", err)
		}
		if held, found := store.held(); found {
			t.Errorf("key still holds revision %d after the claim gave up", held.revision)
		}
	})
}

type logged struct {
	Level   string
	Message string `json:"msg"`
}

// recorder keeps what the lease logs, written by Run's goroutine and read by the test.
type recorder struct {
	lock   sync.Mutex
	output bytes.Buffer
}

func (r *recorder) Write(line []byte) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.output.Write(line)
}

func (r *recorder) logged(t *testing.T) []logged {
	t.Helper()
	r.lock.Lock()
	defer r.lock.Unlock()
	var records []logged
	for line := range bytes.Lines(r.output.Bytes()) {
		var record logged
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// TestAKVOutageAroundLostAfterKeepsTheLease: a KV that stops answering for less than LostAfter leaves
// the lease valid; one for longer suspends it — side effects stop — and the first renewal after the
// outage resumes it with the same epoch, because the key was never expired or taken.
func TestAKVOutageAroundLostAfterKeepsTheLease(t *testing.T) {
	tests := []struct {
		Name string
		Args time.Duration
		Want []logged
	}{
		{Name: "shorter than LostAfter", Args: 10 * time.Second},
		{Name: "longer than LostAfter", Args: 22 * time.Second, Want: []logged{
			{Level: "WARN", Message: "lease.suspended"},
			{Level: "INFO", Message: "lease.resumed"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := newFakeStore(productionTTL)
				logs := &recorder{}
				options := productionOptions()
				options.Logger = slog.New(slog.NewJSONHandler(logs, nil))
				lease := claimedWith(t, store, options)
				store.failAlways(hang)
				stop, _ := running(t, lease)

				time.Sleep(tt.Args)
				validInOutage := lease.Valid()
				store.failAlways(answer)
				time.Sleep(productionRenew + productionRetry)
				synctest.Wait()

				if want := tt.Args < productionLost; validInOutage != want {
					t.Errorf("valid at the end of the outage = %t, want %t", validInOutage, want)
				}
				if !lease.Valid() || lost(lease) || lease.Epoch != firstRevision {
					t.Errorf("after the outage: valid %t, lost %t, epoch %d; want valid with the first epoch",
						lease.Valid(), lost(lease), lease.Epoch)
				}
				if diff := cmp.Diff(tt.Want, logs.logged(t)); diff != "" {
					t.Errorf("logs (-want +got):\n%s", diff)
				}
				if err := stop(); err != nil {
					t.Errorf("Run: %v", err)
				}
			})
		})
	}
}

func TestALongerBucketMaxAgeIsTheThresholdOfLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(longerMaxAge)
		lease := claimed(t, store)
		store.failAlways(hang)
		_, returned := running(t, lease)

		time.Sleep(longerMaxAge - justBefore)
		synctest.Wait()
		lostBefore := lost(lease)
		err := <-returned

		if lostBefore || !errors.Is(err, ErrLeaseLost) {
			t.Errorf("lost before the bucket's max age %t, Run = %v; want lost only at %s", lostBefore, err,
				longerMaxAge)
		}
	})
}

// TestAClaimRetakesItsOwnKeyAfterItsUpdateFailed: the create landed with its answer lost, and the
// update that would have taken it failed; the next attempt must take the key at the revision it
// already knows instead of waiting for its own key to expire.
func TestAClaimRetakesItsOwnKeyAfterItsUpdateFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore(productionTTL)
		store.failNext(applyAndHang, answer, hang)
		start := time.Now()

		lease := claimed(t, store)

		if waited := time.Since(start); waited != 2*productionAttempt || lease.Epoch != 2 {
			t.Errorf("claimed after %s with epoch %d, want after two attempts with the update's epoch 2", waited,
				lease.Epoch)
		}
	})
}

// TestValidCountsFromWhenTheRenewalWasSentNotAnswered: the server extends the key from the write, which
// happens no later than the answer arrives; counting from the answer would stretch Valid past the key
// by the answer's delay.
func TestValidCountsFromWhenTheRenewalWasSentNotAnswered(t *testing.T) {
	tests := []struct {
		Name     string
		Args     func(store *fakeStore)
		WantSent time.Duration
	}{
		{Name: "a slow create", Args: func(store *fakeStore) { store.failNext(slowAnswer) }},
		{Name: "a slow renewal", Args: func(store *fakeStore) { store.failNext(answer, slowAnswer) },
			WantSent: productionRenew},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := newFakeStore(productionTTL)
				tt.Args(store)
				start := time.Now()
				lease := claimed(t, store)
				stop, _ := running(t, lease)
				time.Sleep(time.Until(start.Add(tt.WantSent + slowAnswerDelay)))
				synctest.Wait()
				store.failAlways(hang)

				time.Sleep(time.Until(start.Add(tt.WantSent + productionLost - justBefore)))
				validBefore := lease.Valid()
				time.Sleep(justBefore)
				validAt := lease.Valid()

				if diff := cmp.Diff([]bool{true, false}, []bool{validBefore, validAt}); diff != "" {
					t.Errorf("valid just before and at LostAfter after the send (-want +got):\n%s", diff)
				}
				if err := stop(); err != nil {
					t.Errorf("Run: %v", err)
				}
			})
		})
	}
}
