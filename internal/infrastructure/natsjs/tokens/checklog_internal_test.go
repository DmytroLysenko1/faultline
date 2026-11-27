package tokens

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
)

const (
	checkFailedEvent     = "tokens.check_failed"
	checksOnAFrozenClock = 8
)

type jsonLogs struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (j *jsonLogs) Write(p []byte) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.buffer.Write(p)
}

type warned struct {
	Reason     string
	Suppressed float64
	At         time.Duration
}

func (j *jsonLogs) warnings(t *testing.T, event string, began time.Time) []warned {
	t.Helper()
	j.mu.Lock()
	defer j.mu.Unlock()
	var found []warned
	scanner := bufio.NewScanner(bytes.NewReader(j.buffer.Bytes()))
	for scanner.Scan() {
		record := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if record["msg"] != event {
			continue
		}
		logged, err := time.Parse(time.RFC3339Nano, record["time"].(string))
		if err != nil {
			t.Fatalf("time: %v", err)
		}
		reason, _ := record["reason"].(string)
		suppressed, _ := record["suppressed"].(float64)
		found = append(found, warned{
			Reason:     reason,
			Suppressed: suppressed,
			At:         logged.Sub(began).Round(time.Second),
		})
	}
	return found
}

func startLogged(t *testing.T, w *world, logs *jsonLogs, clock clock) (*running, time.Time) {
	t.Helper()
	options := testOptions()
	options.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	options.Clock = clock
	directory, err := newDirectory(w.readState, w.readEntries, options)
	if err != nil {
		t.Fatalf("newDirectory: %v", err)
	}
	began := time.Now()
	if err := directory.load(t.Context()); err != nil {
		t.Fatalf("load: %v", err)
	}
	r := &running{
		directory: directory,
		done:      make(chan error, 1),
	}
	ctx, cancel := context.WithCancel(t.Context())
	r.cancel = cancel
	go func() {
		r.done <- directory.Run(ctx)
	}()
	t.Cleanup(func() {
		r.stop(t)
	})
	return r, began
}

func TestFailedChecksWarnOncePerReasonPerInterval(t *testing.T) {
	tests := []struct {
		name string
		args []error
		want []warned
	}{
		{
			name: "one reason repeating every retry",
			args: slices.Repeat([]error{ErrNoLeader}, 12),
			want: []warned{
				{
					Reason: "leaderless",
					At:     15 * time.Second,
				},
				{
					Reason:     "leaderless",
					Suppressed: 4,
					At:         25 * time.Second,
				},
				{
					Reason:     "leaderless",
					Suppressed: 4,
					At:         35 * time.Second,
				},
			},
		},
		{
			name: "each reason has its own interval",
			args: []error{ErrNoLeader, ErrNoQuorum, ErrBucketDrift, errLostCheck, ErrNoLeader, ErrNoQuorum},
			want: []warned{
				{
					Reason: "leaderless",
					At:     15 * time.Second,
				},
				{
					Reason: "quorumless",
					At:     17 * time.Second,
				},
				{
					Reason: "drifted",
					At:     19 * time.Second,
				},
				{
					Reason: "unavailable",
					At:     21 * time.Second,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, _ := seeded(t, 1)
				w.change(func(w *world) {
					w.stateFault = slices.Clone(tt.args)
				})
				logs := &jsonLogs{}
				_, began := startLogged(t, w, logs, nil)
				time.Sleep(reload + time.Duration(len(tt.args))*retryEvery + reload)
				synctest.Wait()
				if diff := gocmp.Diff(tt.want, logs.warnings(t, checkFailedEvent, began)); diff != "" {
					t.Errorf("check failure warnings (-want +got):\n%s", diff)
				}
			})
		})
	}
}

type recovery struct {
	Suppressed map[string]any
	At         time.Duration
}

func (j *jsonLogs) records(t *testing.T, event string) []map[string]any {
	t.Helper()
	j.mu.Lock()
	defer j.mu.Unlock()
	var found []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(j.buffer.Bytes()))
	for scanner.Scan() {
		record := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if record["msg"] == event {
			found = append(found, record)
		}
	}
	return found
}

func (j *jsonLogs) recoveries(t *testing.T, began time.Time) []recovery {
	t.Helper()
	var found []recovery
	for _, record := range j.records(t, "tokens.check_recovered") {
		logged, err := time.Parse(time.RFC3339Nano, record["time"].(string))
		if err != nil {
			t.Fatalf("time: %v", err)
		}
		suppressed, _ := record["suppressed"].(map[string]any)
		found = append(found, recovery{
			Suppressed: suppressed,
			At:         logged.Sub(began).Round(time.Second),
		})
	}
	return found
}

func TestTheFirstFreshCheckReportsTheSuppressedFailures(t *testing.T) {
	type args struct {
		faults []error
		issued bool
	}
	tests := []struct {
		name string
		args args
		want []recovery
	}{
		{
			name: "one reason repeating every retry",
			args: args{
				faults: slices.Repeat([]error{ErrNoLeader}, 12),
			},
			want: []recovery{
				{
					Suppressed: map[string]any{
						"leaderless": float64(1),
					},
					At: 39 * time.Second,
				},
			},
		},
		{
			name: "a grant issued during the failures",
			args: args{
				faults: slices.Repeat([]error{ErrNoLeader}, 12),
				issued: true,
			},
			want: []recovery{
				{
					Suppressed: map[string]any{
						"leaderless": float64(1),
					},
					At: 39 * time.Second,
				},
			},
		},
		{
			name: "each reason has its own interval",
			args: args{
				faults: []error{ErrNoLeader, ErrNoQuorum, ErrBucketDrift, errLostCheck, ErrNoLeader, ErrNoQuorum},
			},
			want: []recovery{
				{
					Suppressed: map[string]any{
						"leaderless": float64(1),
						"quorumless": float64(1),
					},
					At: 27 * time.Second,
				},
			},
		},
		{
			name: "every failure warned",
			args: args{
				faults: []error{ErrNoLeader},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, _ := seeded(t, 1)
				w.change(func(w *world) {
					w.stateFault = slices.Clone(tt.args.faults)
				})
				logs := &jsonLogs{}
				r, began := startLogged(t, w, logs, nil)
				issued := testGrant(t, 2)
				if tt.args.issued {
					w.issue(&issued)
				}
				time.Sleep(reload + time.Duration(len(tt.args.faults))*retryEvery + 2*reload)
				synctest.Wait()
				if diff := gocmp.Diff([]any{tt.want, tt.args.issued}, []any{
					logs.recoveries(t, began),
					r.has(issued.ID),
				}); diff != "" {
					t.Errorf("recoveries and the issued grant (-want +got):\n%s", diff)
				}
			})
		})
	}
}

type frozenClock struct {
	at time.Time
}

func (c frozenClock) Now() time.Time {
	return c.at
}

func TestTheDirectoryKeepsTimeByItsOwnClock(t *testing.T) {
	type want struct {
		Warnings  int
		Reason    any
		Checks    uint64
		CheckedAt bool
	}
	tests := []struct {
		name string
		args time.Duration
		want want
	}{
		{
			name: "a clock an hour behind",
			args: -epochOffset,
			want: want{
				Warnings:  1,
				Reason:    "leaderless",
				Checks:    7,
				CheckedAt: true,
			},
		},
		{
			name: "a clock an hour ahead",
			args: epochOffset,
			want: want{
				Warnings:  1,
				Reason:    "leaderless",
				Checks:    7,
				CheckedAt: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, _ := seeded(t, 1)
				w.change(func(w *world) {
					w.stateFault = slices.Repeat([]error{ErrNoLeader}, 6)
					w.parkAfter = checksOnAFrozenClock
					w.unpark = make(chan struct{})
				})
				logs := &jsonLogs{}
				frozen := time.Now().Add(tt.args)
				r, _ := startLogged(t, w, logs, frozenClock{
					at: frozen,
				})
				t.Cleanup(func() {
					close(w.unpark)
				})
				time.Sleep(reload + 6*retryEvery + reload/2)
				synctest.Wait()

				warned := append(logs.records(t, checkFailedEvent), map[string]any{})
				if diff := gocmp.Diff(tt.want, want{
					Warnings:  len(warned) - 1,
					Reason:    warned[0]["reason"],
					Checks:    r.directory.Stats().Checks,
					CheckedAt: r.directory.Snapshot(t.Context()).CheckedAt.Equal(frozen),
				}); diff != "" {
					t.Errorf("a directory on a frozen clock (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestTheFirstLoadLogsTheBucketEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := seeded(t, 2)
		logs := &jsonLogs{}
		startLogged(t, w, logs, nil)

		epochs := logs.records(t, "tokens.bucket_epoch")
		if len(epochs) != 1 {
			t.Fatalf("bucket epoch logged %d times, want once", len(epochs))
		}
		created, err := time.Parse(time.RFC3339Nano, epochs[0]["created"].(string))
		if err != nil {
			t.Fatalf("created: %v", err)
		}
		got := []any{
			created.Equal(w.created),
			epochs[0]["last_sequence"],
		}
		if diff := gocmp.Diff([]any{true, float64(2)}, got); diff != "" {
			t.Errorf("bucket epoch (-want +got):\n%s", diff)
		}
	})
}
