package tokens

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	reload      = 15 * time.Second
	retryEvery  = 2 * time.Second
	checkTime   = 2 * time.Second
	rereadTime  = 5 * time.Second
	staleAfter  = 45 * time.Second
	visibleBy   = 22 * time.Second
	phaseStep   = 500 * time.Millisecond
	manyGrants  = 40
	epochOffset = time.Hour
)

var errLostCheck = errors.New("check lost")

type world struct {
	mu             sync.Mutex
	created        time.Time
	lastSeq        uint64
	keys           map[credential.ID]uint64
	grants         map[credential.ID]credential.Grant
	config         jetstream.StreamConfig
	stateFault     []error
	readFault      []error
	stateDelay     time.Duration
	rereadDelay    time.Duration
	override       *bucketState
	rereadOverride *bucketState
	invalid        uint64
	onReread       func(w *world)
	stateCalls     []time.Duration
	readCalls      []time.Duration
}

func newWorld() *world {
	return &world{
		created: time.Now().Add(-epochOffset),
		keys:    map[credential.ID]uint64{},
		grants:  map[credential.ID]credential.Grant{},
		config:  desiredConfig(threeReplicas),
	}
}

func (w *world) issue(grant *credential.Grant) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastSeq++
	w.keys[grant.ID] = w.lastSeq
	w.grants[grant.ID] = *grant
}

func (w *world) revoke(id credential.ID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.keys, id)
	delete(w.grants, id)
}

func (w *world) stateLocked() bucketState {
	if w.override != nil {
		return *w.override
	}
	first := w.lastSeq + 1
	for _, seq := range w.keys {
		first = min(first, seq)
	}
	return bucketState{
		LastSeq:  w.lastSeq,
		FirstSeq: first,
		Msgs:     uint64(len(w.keys)),
		Created:  w.created,
	}
}

func (w *world) answer(ctx context.Context, delay time.Duration, faults *[]error) error {
	if deadline, _ := ctx.Deadline(); !time.Now().Add(delay).Before(deadline) {
		<-ctx.Done()
		return fmt.Errorf("request: %w", nats.ErrTimeout)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return fmt.Errorf("request: %w", ctx.Err())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(*faults) == 0 {
		return nil
	}
	fault := (*faults)[0]
	*faults = (*faults)[1:]
	return fault
}

func (w *world) readState(ctx context.Context) (bucketState, configVerdict, error) {
	w.mu.Lock()
	w.stateCalls = append(w.stateCalls, budgetOf(ctx))
	delay := w.stateDelay
	w.mu.Unlock()
	if err := w.answer(ctx, delay, &w.stateFault); err != nil {
		return bucketState{}, configVerdict{}, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stateLocked(), verdictOf(&w.config), nil
}

func (w *world) readEntries(ctx context.Context) (bucketEntries, configVerdict, error) {
	w.mu.Lock()
	w.readCalls = append(w.readCalls, budgetOf(ctx))
	delay, onReread := w.rereadDelay, w.onReread
	state := w.stateLocked()
	if w.rereadOverride != nil {
		state = *w.rereadOverride
	}
	grants := maps.Clone(w.grants)
	w.mu.Unlock()
	if err := w.answer(ctx, delay, &w.readFault); err != nil {
		return bucketEntries{}, configVerdict{}, err
	}
	if onReread != nil {
		onReread(w)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return bucketEntries{
		State:   state,
		Grants:  grants,
		Invalid: w.invalid,
	}, verdictOf(&w.config), nil
}

func budgetOf(ctx context.Context) time.Duration {
	deadline, _ := ctx.Deadline()
	return time.Until(deadline)
}

func (w *world) change(apply func(w *world)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	apply(w)
}

type running struct {
	directory *Directory
	cancel    context.CancelFunc
	done      chan error
}

func testOptions() *DirectoryOptions {
	return &DirectoryOptions{
		ReloadInterval: reload,
		RetryEvery:     retryEvery,
		CheckTime:      checkTime,
		RereadTime:     rereadTime,
	}
}

func start(t *testing.T, w *world) *running {
	t.Helper()
	directory, err := newDirectory(w.readState, w.readEntries, testOptions())
	if err != nil {
		t.Fatalf("newDirectory: %v", err)
	}
	if err := directory.load(t.Context()); err != nil {
		t.Fatalf("load: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	r := &running{
		directory: directory,
		cancel:    cancel,
		done:      make(chan error, 1),
	}
	go func() {
		r.done <- directory.Run(ctx)
	}()
	t.Cleanup(func() {
		r.stop(t)
	})
	return r
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	if err := <-r.done; err != nil {
		t.Errorf("Run = %v, want nil on the cancel of its own context", err)
	}
	r.done <- nil
}

func (r *running) has(id credential.ID) bool {
	_, found := r.directory.current.Load().grants[id]
	return found
}

func (r *running) age() time.Duration {
	return time.Since(r.directory.current.Load().checkedAt)
}

func waitUntil(t *testing.T, limit time.Duration, done func() bool) time.Duration {
	t.Helper()
	started := time.Now()
	for !done() {
		if time.Since(started) > limit {
			t.Fatalf("condition not met within %s", limit)
		}
		time.Sleep(phaseStep / 5)
	}
	return time.Since(started)
}

func seeded(t *testing.T, count int) (*world, []credential.Grant) {
	t.Helper()
	w := newWorld()
	grants := make([]credential.Grant, 0, count)
	for seed := range count {
		grant := testGrant(t, byte(seed+1))
		w.issue(&grant)
		grants = append(grants, grant)
	}
	return w, grants
}

func TestARevokedGrantIsGoneFromTheSnapshotWithin22s(t *testing.T) {
	for offset := time.Duration(0); offset < reload; offset += phaseStep {
		t.Run(offset.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, grants := seeded(t, 2)
				w.stateDelay, w.rereadDelay = checkTime-time.Millisecond, rereadTime-time.Millisecond
				r := start(t, w)
				time.Sleep(offset)
				w.revoke(grants[0].ID)
				took := waitUntil(t, time.Minute, func() bool { return !r.has(grants[0].ID) })
				if took > visibleBy || !r.has(grants[1].ID) {
					t.Errorf("revocation took %s (limit %s); other grant kept: %v", took, visibleBy, r.has(grants[1].ID))
				}
			})
		})
	}
}

func TestANewGrantAppearsInTheSnapshotWithin22s(t *testing.T) {
	for offset := time.Duration(0); offset < reload; offset += phaseStep {
		t.Run(offset.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, _ := seeded(t, 1)
				w.stateDelay, w.rereadDelay = checkTime-time.Millisecond, rereadTime-time.Millisecond
				r := start(t, w)
				time.Sleep(offset)
				grant := testGrant(t, 9)
				w.issue(&grant)
				if took := waitUntil(t, time.Minute, func() bool { return r.has(grant.ID) }); took > visibleBy {
					t.Errorf("issue took %s, limit %s", took, visibleBy)
				}
			})
		})
	}
}

func TestEachReadGetsItsOwnBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 1)
		r := start(t, w)
		w.revoke(grants[0].ID)
		time.Sleep(reload + time.Second)
		r.stop(t)
		w.mu.Lock()
		defer w.mu.Unlock()
		if diff := gocmp.Diff([]time.Duration{checkTime}, w.stateCalls); diff != "" {
			t.Errorf("check budgets (-want +got):\n%s", diff)
		}
		if diff := gocmp.Diff([]time.Duration{rereadTime, rereadTime}, w.readCalls); diff != "" {
			t.Errorf("reread budgets (-want +got):\n%s", diff)
		}
	})
}

func TestAnAnswerExactlyAtTheDeadlineIsAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		r := start(t, w)
		w.change(func(w *world) { w.stateDelay = checkTime })
		time.Sleep(reload + checkTime + time.Millisecond)
		stats := r.directory.Stats()
		if stats.Checks != 1 || stats.CheckFailures != 1 || r.age() < reload+checkTime {
			t.Errorf("stats %+v, age %s; want one failed check and no refresh", stats, r.age())
		}
	})
}

func TestAFailedRereadDoesNotRefreshTheDirectory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 2)
		r := start(t, w)
		w.revoke(grants[0].ID)
		w.change(func(w *world) { w.readFault = []error{errLostCheck} })
		time.Sleep(reload + time.Second)
		stats := r.directory.Stats()
		if !r.has(grants[0].ID) || r.age() < reload || stats.RereadFailures != 1 || stats.CheckFailures != 1 {
			t.Errorf("after a failed reread: stats %+v, age %s, revoked still listed %v", stats, r.age(), r.has(grants[0].ID))
		}
		time.Sleep(retryEvery)
		if r.has(grants[0].ID) || r.age() > retryEvery+time.Second {
			t.Errorf("the retry did not reread: age %s", r.age())
		}
	})
}

func TestASingleLostCheckDoesNotMakeTheDirectoryStale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		r := start(t, w)
		w.change(func(w *world) { w.stateFault = []error{errLostCheck} })
		oldest := time.Duration(0)
		for range 4 * int(reload/phaseStep) {
			time.Sleep(phaseStep)
			oldest = max(oldest, r.age())
		}
		if oldest > reload+retryEvery+phaseStep {
			t.Errorf("one lost check aged the directory to %s", oldest)
		}
	})
}

func TestALeaderlessAnswerDoesNotRefreshTheDirectory(t *testing.T) {
	tests := []struct {
		name  string
		fault error
		count func(DirectoryStats) uint64
	}{
		{
			name:  "no leader",
			fault: ErrNoLeader,
			count: func(s DirectoryStats) uint64 { return s.LeaderlessChecks },
		},
		{
			name:  "no quorum",
			fault: ErrNoQuorum,
			count: func(s DirectoryStats) uint64 { return s.QuorumlessChecks },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, _ := seeded(t, 1)
				r := start(t, w)
				w.change(func(w *world) { w.stateFault = slices.Repeat([]error{tt.fault}, 100) })
				time.Sleep(staleAfter + time.Second)
				stats := r.directory.Stats()
				if r.age() <= staleAfter || tt.count(stats) == 0 || tt.count(stats) != stats.CheckFailures {
					t.Errorf("age %s, stats %+v; want a stale directory and every failure counted", r.age(), stats)
				}
			})
		})
	}
}

func TestARestoreThatOnlyUndoesARevokeIsARegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 3)
		r := start(t, w)
		w.revoke(grants[1].ID)
		time.Sleep(reload + time.Second)
		w.change(func(w *world) {
			w.keys[grants[1].ID] = 2
			w.grants[grants[1].ID] = grants[1]
		})
		time.Sleep(reload + time.Second)
		stats := r.directory.Stats()
		if !stats.Regressed || stats.RegressedChecks == 0 || r.has(grants[1].ID) {
			t.Errorf("stats %+v, resurrected grant listed %v", stats, r.has(grants[1].ID))
		}
		time.Sleep(staleAfter)
		if r.age() <= staleAfter {
			t.Errorf("a regressed directory still refreshes: age %s", r.age())
		}
	})
}

func TestAPublishHidingARestoreIsARegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 3)
		r := start(t, w)
		w.revoke(grants[1].ID)
		time.Sleep(reload + time.Second)
		w.change(func(w *world) {
			w.keys[grants[1].ID] = 2
			w.grants[grants[1].ID] = grants[1]
		})
		fresh := testGrant(t, 9)
		w.issue(&fresh)
		time.Sleep(reload + time.Second)
		if stats := r.directory.Stats(); !stats.Regressed || r.has(grants[1].ID) {
			t.Errorf("Msgs grew by 2 while LastSeq grew by 1: stats %+v", stats)
		}
	})
}

func TestANewIssueDoesNotHealARegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 2)
		r := start(t, w)
		w.change(func(w *world) { w.lastSeq-- })
		time.Sleep(reload + time.Second)
		for seed := range 5 {
			grant := testGrant(t, byte(20+seed))
			w.issue(&grant)
		}
		time.Sleep(3 * reload)
		if stats := r.directory.Stats(); !stats.Regressed || r.age() <= 3*reload || !r.has(grants[0].ID) {
			t.Errorf("stats %+v, age %s; want a sticky regression", stats, r.age())
		}
	})
}

func TestANewEpochClearsTheRegressionAndRaisesTheAlert(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 2)
		r := start(t, w)
		w.change(func(w *world) { w.lastSeq-- })
		time.Sleep(reload + time.Second)
		reissued := testGrant(t, 30)
		w.change(func(w *world) {
			w.created = time.Now()
			w.lastSeq = 0
			clear(w.keys)
			clear(w.grants)
		})
		w.issue(&reissued)
		time.Sleep(retryEvery + time.Second)
		stats := r.directory.Stats()
		if stats.Regressed || stats.EpochChanges != 1 || r.has(grants[0].ID) || !r.has(reissued.ID) {
			t.Errorf("stats %+v; want the new epoch read and the alert counted", stats)
		}
	})
}

func TestARestoreAfterAMissingBucketIsARegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 3)
		r := start(t, w)
		w.revoke(grants[1].ID)
		time.Sleep(reload + time.Second)
		w.change(func(w *world) { w.stateFault = []error{ErrBucketMissing} })
		time.Sleep(reload + time.Second)
		w.change(func(w *world) {
			w.keys[grants[1].ID] = 2
			w.grants[grants[1].ID] = grants[1]
		})
		time.Sleep(reload + retryEvery)
		if stats := r.directory.Stats(); !stats.Regressed || r.has(grants[1].ID) {
			t.Errorf("stats %+v; a restore with the same Created after a missing bucket must stay a regression", stats)
		}
	})
}

func TestABucketRecreatedAfterMissingIsAnEpochChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 2)
		r := start(t, w)
		w.change(func(w *world) { w.stateFault = []error{ErrBucketMissing} })
		time.Sleep(reload + time.Second)
		w.change(func(w *world) {
			w.created = time.Now()
			w.lastSeq = 0
			clear(w.keys)
			clear(w.grants)
		})
		fresh := testGrant(t, 40)
		w.issue(&fresh)
		time.Sleep(reload + retryEvery)
		if stats := r.directory.Stats(); stats.EpochChanges != 1 || stats.Regressed || !r.has(fresh.ID) || r.has(grants[0].ID) {
			t.Errorf("stats %+v; a recreated bucket must raise the epoch alert", stats)
		}
	})
}

func TestAnEpochChangeDuringARereadIsNoRegression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 3)
		r := start(t, w)
		w.revoke(grants[0].ID)
		w.change(func(w *world) {
			w.rereadOverride = &bucketState{
				LastSeq:  1,
				FirstSeq: 1,
				Msgs:     1,
				Created:  time.Now(),
			}
		})
		time.Sleep(reload + time.Second)
		if stats := r.directory.Stats(); stats.Regressed || stats.EpochChanges != 1 {
			t.Errorf("stats %+v; a new epoch seen by the reread is an epoch change, not a regression", stats)
		}
	})
}

func TestAStopIsNoFailedCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		w.stateDelay = checkTime / 2
		r := start(t, w)
		time.Sleep(reload + checkTime/4)
		r.stop(t)
		if stats := r.directory.Stats(); stats.CheckFailures != 0 {
			t.Errorf("a check cut by the stop counted as a failure: %+v", stats)
		}
	})
}

func TestLeaderWithQuorum(t *testing.T) {
	peer := func(active time.Duration, offline, pending bool) *jetstream.PeerInfo {
		return &jetstream.PeerInfo{
			Active:  active,
			Offline: offline,
			Pending: pending,
		}
	}
	led := func(peers ...*jetstream.PeerInfo) *jetstream.ClusterInfo {
		return &jetstream.ClusterInfo{
			Leader:   "n0",
			Replicas: peers,
		}
	}
	fresh := time.Second
	tests := []struct {
		name     string
		replicas int
		cluster  *jetstream.ClusterInfo
		wantErr  error
	}{
		{
			name:     "no cluster info",
			replicas: 1,
			wantErr:  ErrNoLeader,
		},
		{
			name:     "no leader",
			replicas: 3,
			cluster:  &jetstream.ClusterInfo{},
			wantErr:  ErrNoLeader,
		},
		{
			name:     "a single server",
			replicas: 1,
			cluster: &jetstream.ClusterInfo{
				Leader: "n0",
			},
		},
		{
			name: "two fresh peers", replicas: 3,
			cluster: led(peer(fresh, false, false), peer(fresh, false, false)),
		},
		{
			name: "one fresh peer is a quorum of three", replicas: 3,
			cluster: led(peer(fresh, false, false), peer(time.Hour, false, false)),
		},
		{
			name: "peers silent past the freshness", replicas: 3, wantErr: ErrNoQuorum,
			cluster: led(peer(peerFreshness+time.Nanosecond, false, false), nil),
		},
		{
			name: "a peer exactly at the freshness counts", replicas: 3,
			cluster: led(peer(peerFreshness, false, false)),
		},
		{
			name: "a peer never heard from", replicas: 3, wantErr: ErrNoQuorum,
			cluster: led(peer(0, false, false)),
		},
		{
			name: "an offline peer", replicas: 3, wantErr: ErrNoQuorum,
			cluster: led(peer(fresh, true, false)),
		},
		{
			name: "a pending peer", replicas: 3, wantErr: ErrNoQuorum,
			cluster: led(peer(fresh, false, true)),
		},
		{
			name: "five replicas need two fresh peers", replicas: 5, wantErr: ErrNoQuorum,
			cluster: led(peer(fresh, false, false)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := jetstream.StreamConfig{
				Name:     streamName,
				Replicas: tt.replicas,
			}
			info := &jetstream.StreamInfo{
				Config:  config,
				Cluster: tt.cluster,
			}
			if err := leaderWithQuorum(info); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestADriftedBucketDoesNotRefreshTheDirectory(t *testing.T) {
	drifts := map[string]func(c *jetstream.StreamConfig){
		"sources": func(c *jetstream.StreamConfig) {
			c.Sources = []*jetstream.StreamSource{{
				Name: "X",
			}}
		},
		"max msgs":  func(c *jetstream.StreamConfig) { c.MaxMsgs = -1 },
		"max age":   func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
		"retention": func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy },
	}
	for name, drift := range drifts {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, _ := seeded(t, 1)
				r := start(t, w)
				w.change(func(w *world) { drift(&w.config) })
				time.Sleep(staleAfter + time.Second)
				stats := r.directory.Stats()
				if r.age() <= staleAfter || stats.DriftedChecks == 0 || stats.DriftedChecks != stats.CheckFailures {
					t.Errorf("age %s, stats %+v", r.age(), stats)
				}
			})
		})
	}
}

func TestAnUnknownFieldDifferenceDoesNotStopIngest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		r := start(t, w)
		w.change(func(w *world) { w.config.NoAck = true })
		time.Sleep(staleAfter + time.Second)
		if stats := r.directory.Stats(); r.age() > reload || !stats.ConfigDrift || stats.CheckFailures != 0 {
			t.Errorf("age %s, stats %+v; want fresh and the drift alerted", r.age(), stats)
		}
	})
}

func TestADirectoryRunsOnAnIrreversibleBucket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		w.config.DenyPurge = true
		r := start(t, w)
		time.Sleep(staleAfter + time.Second)
		if stats := r.directory.Stats(); r.age() > reload || !stats.RevokeBlocked || stats.CheckFailures != 0 {
			t.Errorf("age %s, stats %+v", r.age(), stats)
		}
	})
}

func TestAPurgeWithoutANewSequenceIsReread(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 3)
		r := start(t, w)
		w.revoke(grants[2].ID)
		time.Sleep(reload + time.Second)
		if r.has(grants[2].ID) || r.directory.Stats().Rereads != 2 {
			t.Errorf("a purge of the last key changed only Msgs and was not reread: %+v", r.directory.Stats())
		}
	})
}

func TestAWriteDuringARereadIsPickedUpNextCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, grants := seeded(t, 2)
		r := start(t, w)
		late := testGrant(t, 50)
		w.revoke(grants[0].ID)
		w.change(func(w *world) {
			w.onReread = func(w *world) {
				w.onReread = nil
				w.issue(&late)
			}
		})
		time.Sleep(reload + time.Second)
		if r.has(late.ID) {
			t.Fatal("a grant written after the reread's info was listed by that reread")
		}
		time.Sleep(reload)
		if !r.has(late.ID) {
			t.Error("the next check did not pick up the write")
		}
	})
}

func TestChecksKeepAFixedPhase(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		w.stateDelay = checkTime - time.Millisecond
		started := time.Now()
		r := start(t, w)
		var checked []time.Duration
		for range 4 {
			before := r.directory.Stats().Checks
			waitUntil(t, reload+checkTime, func() bool { return r.directory.Stats().Checks > before })
			checked = append(checked, time.Since(started).Round(time.Second))
		}
		want := []time.Duration{reload, 2 * reload, 3 * reload, 4 * reload}
		if diff := gocmp.Diff(want, checked); diff != "" {
			t.Errorf("check times drift (-want +got):\n%s", diff)
		}
	})
}

func TestAMassRevokeIsReported(t *testing.T) {
	tests := []struct {
		name    string
		revoke  int
		total   int
		wantOne bool
	}{
		{
			name:   "a few revokes",
			revoke: 3,
		},
		{
			name:   "the last token alone",
			revoke: 1,
			total:  1,
		},
		{
			name:    "half and one more",
			revoke:  manyGrants/2 + 1,
			wantOne: true,
		},
		{
			name:    "everything",
			revoke:  manyGrants,
			wantOne: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				total := cmp.Or(tt.total, manyGrants)
				w, grants := seeded(t, total)
				r := start(t, w)
				for _, grant := range grants[:tt.revoke] {
					w.revoke(grant.ID)
				}
				time.Sleep(reload + time.Second)
				stats := r.directory.Stats()
				if got := stats.MassRevokes == 1; got != tt.wantOne || stats.Tokens != total-tt.revoke {
					t.Errorf("stats %+v; want mass revoke %v", stats, tt.wantOne)
				}
			})
		})
	}
}

func TestRunTwiceIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 1)
		r := start(t, w)
		synctest.Wait()
		if err := r.directory.Run(t.Context()); !errors.Is(err, ErrAlreadyRunning) {
			t.Errorf("second Run = %v, want ErrAlreadyRunning", err)
		}
	})
}

func TestAFailedFirstLoadOpensNothing(t *testing.T) {
	tests := []struct {
		name    string
		fault   error
		config  func(c *jetstream.StreamConfig)
		wantErr error
	}{
		{
			name:    "a missing bucket",
			fault:   ErrBucketMissing,
			config:  func(*jetstream.StreamConfig) {},
			wantErr: ErrBucketMissing,
		},
		{
			name:    "a leaderless bucket",
			fault:   ErrNoLeader,
			config:  func(*jetstream.StreamConfig) {},
			wantErr: ErrNoLeader,
		},
		{
			name:    "a bucket injecting grants",
			config:  func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
			wantErr: ErrBucketDrift,
		},
		{
			name:   "an irreversible bucket opens",
			config: func(c *jetstream.StreamConfig) { c.Sealed = true },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, _ := seeded(t, 1)
			if tt.fault != nil {
				w.readFault = []error{tt.fault}
			}
			tt.config(&w.config)
			directory, err := newDirectory(w.readState, w.readEntries, testOptions())
			if err != nil {
				t.Fatalf("newDirectory: %v", err)
			}
			if err := directory.load(t.Context()); !errors.Is(err, tt.wantErr) {
				t.Fatalf("load = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestWaitsForTable(t *testing.T) {
	tests := []struct {
		name string
		args error
		want bool
	}{
		{
			name: "a missing bucket",
			args: fmt.Errorf("open: %w", ErrBucketMissing),
			want: true,
		},
		{
			name: "no leader",
			args: ErrNoLeader,
			want: true,
		},
		{
			name: "no quorum",
			args: ErrNoQuorum,
			want: true,
		},
		{
			name: "no responders",
			args: nats.ErrNoResponders,
			want: true,
		},
		{
			name: "a timeout",
			args: context.DeadlineExceeded,
			want: true,
		},
		{
			name: "a grant injecting drift",
			args: ErrBucketDrift,
		},
		{
			name: "too many keys",
			args: ErrTooManyKeys,
		},
		{
			name: "nothing",
			args: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WaitsFor(tt.args); got != tt.want {
				t.Errorf("WaitsFor(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestRetryMayNotExceedReload(t *testing.T) {
	tests := []struct {
		name    string
		change  func(o *DirectoryOptions)
		wantErr error
	}{
		{
			name:   "the defaults",
			change: func(*DirectoryOptions) {},
		},
		{
			name:   "retry equal to reload",
			change: func(o *DirectoryOptions) { o.RetryEvery = o.ReloadInterval },
		},
		{
			name:    "retry past reload",
			change:  func(o *DirectoryOptions) { o.RetryEvery = o.ReloadInterval + 1 },
			wantErr: ErrInvalidOptions,
		},
		{
			name:    "zero reread",
			change:  func(o *DirectoryOptions) { o.RereadTime = 0 },
			wantErr: ErrInvalidOptions,
		},
		{
			name:    "negative check",
			change:  func(o *DirectoryOptions) { o.CheckTime = -1 },
			wantErr: ErrInvalidOptions,
		},
		{
			name:    "reads as long as the reload",
			change:  func(o *DirectoryOptions) { o.RereadTime = o.ReloadInterval - o.CheckTime },
			wantErr: ErrInvalidOptions,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := testOptions()
			tt.change(options)
			if _, err := newDirectory(newWorld().readState, newWorld().readEntries, options); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
	if _, err := newDirectory(newWorld().readState, newWorld().readEntries, nil); !errors.Is(err, ErrInvalidOptions) {
		t.Errorf("nil options: err = %v", err)
	}
}

func TestTheHorizonIsTwoReloadsAndBothReads(t *testing.T) {
	if got, want := testOptions().Horizon(), 37*time.Second; got != want {
		t.Errorf("Horizon = %s, want %s", got, want)
	}
}

func TestRolledBack(t *testing.T) {
	covered := bucketState{
		LastSeq:  10,
		FirstSeq: 3,
		Msgs:     5,
		Created:  issuedAt,
	}
	tests := []struct {
		name  string
		state bucketState
		want  bool
	}{
		{
			name:  "the same",
			state: covered,
		},
		{
			name: "a new issue",
			state: bucketState{
				LastSeq:  11,
				FirstSeq: 3,
				Msgs:     6,
				Created:  issuedAt,
			},
		},
		{
			name: "a revoke",
			state: bucketState{
				LastSeq:  10,
				FirstSeq: 3,
				Msgs:     4,
				Created:  issuedAt,
			},
		},
		{
			name: "a revoke of the first",
			state: bucketState{
				LastSeq:  10,
				FirstSeq: 5,
				Msgs:     4,
				Created:  issuedAt,
			},
		},
		{
			name: "a lower last sequence",
			state: bucketState{
				LastSeq:  9,
				FirstSeq: 3,
				Msgs:     5,
				Created:  issuedAt,
			},
			want: true,
		},
		{
			name: "a lower first sequence",
			state: bucketState{
				LastSeq:  10,
				FirstSeq: 2,
				Msgs:     5,
				Created:  issuedAt,
			},
			want: true,
		},
		{
			name: "more messages at the same sequence",
			state: bucketState{
				LastSeq:  10,
				FirstSeq: 3,
				Msgs:     6,
				Created:  issuedAt,
			},
			want: true,
		},
		{
			name: "two more messages after one issue",
			state: bucketState{
				LastSeq:  11,
				FirstSeq: 3,
				Msgs:     7,
				Created:  issuedAt,
			},
			want: true,
		},
		{
			name: "nothing covered yet",
			state: bucketState{
				LastSeq: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := covered
			if tt.name == "nothing covered yet" {
				base = bucketState{}
			}
			if got := rolledBack(tt.state, base); got != tt.want {
				t.Errorf("rolledBack = %v, want %v", got, tt.want)
			}
		})
	}
}

type recordedLogs struct {
	mu       sync.Mutex
	messages []string
}

func (r *recordedLogs) Enabled(context.Context, slog.Level) bool {
	return true
}

func (r *recordedLogs) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, record.Message)
	return nil
}

func (r *recordedLogs) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *recordedLogs) WithGroup(string) slog.Handler {
	return r
}

func (r *recordedLogs) has(message string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.messages, message)
}

func TestAFirstLoadOfMostlyInvalidEntriesIsAnError(t *testing.T) {
	tests := []struct {
		name    string
		grants  int
		invalid uint64
		want    bool
	}{
		{
			name:    "only invalid entries",
			grants:  0,
			invalid: 3,
			want:    true,
		},
		{
			name:    "as many invalid as grants",
			grants:  2,
			invalid: 2,
			want:    true,
		},
		{
			name:    "fewer invalid than grants",
			grants:  3,
			invalid: 1,
		},
		{
			name:   "no invalid entries",
			grants: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, _ := seeded(t, tt.grants)
			w.invalid = tt.invalid
			logs := &recordedLogs{}
			options := testOptions()
			options.Logger = slog.New(logs)
			directory, err := newDirectory(w.readState, w.readEntries, options)
			if err != nil {
				t.Fatalf("newDirectory: %v", err)
			}
			if err := directory.load(t.Context()); err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := logs.has("tokens.mostly_invalid_entries"); got != tt.want {
				t.Errorf("error logged = %v, want %v", got, tt.want)
			}
		})
	}
}
