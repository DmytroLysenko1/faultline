package matcher_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
)

type batchStep struct {
	At    time.Duration
	Spans []span.Span
}

type matchOutcome struct {
	Calls          []call.Call
	ExpiredCallers []call.Unanswered
	Stats          matcher.Stats
}

func runMatcher(t *testing.T, limits matcher.Limits, steps []batchStep) matchOutcome {
	t.Helper()
	pairing, err := matcher.New(limits)
	if err != nil {
		t.Fatalf("newCallMatcher: %v", err)
	}
	var outcome matchOutcome
	for _, step := range steps {
		now := telemetrytest.Epoch().Add(step.At)
		outcome.ExpiredCallers = append(outcome.ExpiredCallers, pairing.Expire(now)...)
		for _, span := range step.Spans {
			if matched, ok := pairing.Match(&span, now); ok {
				outcome.Calls = append(outcome.Calls, matched)
			}
		}
	}
	outcome.Stats = pairing.Stats
	return outcome
}

func testLimits() matcher.Limits {
	return matcher.Limits{
		TTL:           10 * time.Second,
		MaxPending:    2,
		TombstoneTTL:  60 * time.Second,
		MaxTombstones: 2,
	}
}

func TestCallMatcher(t *testing.T) {
	failedDeadline := map[string]string{
		"rpc.response.status_code": "DEADLINE_EXCEEDED",
	}
	okGRPC := map[string]string{
		"rpc.response.status_code": "OK",
	}
	tests := []struct {
		Name    string
		Args    func(t *testing.T) (matcher.Limits, []batchStep)
		Want    matchOutcome
		WantErr error
	}{
		{
			Name: "client then server",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
							telemetrytest.Server(t, 1, 2, "cart", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed: 2,
					Matched:  1,
				},
			},
		},
		{
			Name: "server then client in different batches",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Server(t, 1, 2, "cart", nil),
						},
					},
					{
						At: 3 * time.Second,
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed: 2,
					Matched:  1,
				},
			},
		},
		{
			Name: "same caller half twice is a duplicate",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Stats: matcher.Stats{
					Observed:  2,
					Duplicate: 1,
					Pending:   1,
				},
			},
		},
		{
			Name: "second server span with the same parent is a duplicate",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Server(t, 1, 2, "cart", nil),
							telemetrytest.Server(t, 1, 3, "cart", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Stats: matcher.Stats{
					Observed:  2,
					Duplicate: 1,
					Pending:   1,
				},
			},
		},
		{
			Name: "retried batch with both halves inside the tombstone window",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				pair := []span.Span{
					telemetrytest.Client(t, 1, 1, "frontend", nil),
					telemetrytest.Server(t, 1, 2, "cart", nil),
				}
				return testLimits(), []batchStep{
					{
						Spans: pair,
					},
					{
						At:    12 * time.Second,
						Spans: pair,
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed:  4,
					Matched:   1,
					Duplicate: 2,
				},
			},
		},
		{
			Name: "retry after the tombstone window counts again (best effort)",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				pair := []span.Span{
					telemetrytest.Client(t, 1, 1, "frontend", nil),
					telemetrytest.Server(t, 1, 2, "cart", nil),
				}
				return testLimits(), []batchStep{
					{
						Spans: pair,
					},
					{
						At:    60 * time.Second,
						Spans: pair,
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed: 4,
					Matched:  2,
				},
			},
		},
		{
			Name: "half still waits just before ttl",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
					{
						At: 10*time.Second - time.Nanosecond,
					},
				}
			},
			Want: matchOutcome{
				Stats: matcher.Stats{
					Observed: 1,
					Pending:  1,
				},
			},
		},
		{
			Name: "caller without answer expires at ttl and keeps its failure",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", map[string]string{
								"rpc.response.status_code": "UNAVAILABLE",
							}),
						},
					},
					{
						At: 10 * time.Second,
					},
				}
			},
			Want: matchOutcome{
				ExpiredCallers: []call.Unanswered{
					{
						Service: telemetrytest.Service("frontend"),
						Failed:  true,
					},
				},
				Stats: matcher.Stats{
					Observed:      1,
					Expired:       1,
					ExpiredCaller: 1,
				},
			},
		},
		{
			Name: "retry of an expired caller is a duplicate, not a second unanswered call",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
					{
						At: 10 * time.Second,
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
					{
						At: 30 * time.Second,
					},
				}
			},
			Want: matchOutcome{
				ExpiredCallers: []call.Unanswered{
					{
						Service: telemetrytest.Service("frontend"),
					},
				},
				Stats: matcher.Stats{
					Observed:      2,
					Duplicate:     1,
					Expired:       1,
					ExpiredCaller: 1,
				},
			},
		},
		{
			Name: "late server after its caller expired waits and expires on its own",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
					{
						At: 10 * time.Second,
						Spans: []span.Span{
							telemetrytest.Server(t, 1, 2, "cart", nil),
						},
					},
					{
						At: 20 * time.Second,
					},
				}
			},
			Want: matchOutcome{
				ExpiredCallers: []call.Unanswered{
					{
						Service: telemetrytest.Service("frontend"),
					},
				},
				Stats: matcher.Stats{
					Observed:      2,
					Expired:       2,
					ExpiredCaller: 1,
				},
			},
		},
		{
			Name: "full buffer evicts the oldest half, not the newest",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
							telemetrytest.Client(t, 2, 1, "checkout", nil),
							telemetrytest.Client(t, 3, 1, "cart", nil),
						},
					},
					{
						At: time.Second,
						Spans: []span.Span{
							telemetrytest.Server(t, 2, 2, "payment", nil),
							telemetrytest.Server(t, 1, 2, "currency", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("checkout"),
						Callee: telemetrytest.Service("payment"),
					},
				},
				Stats: matcher.Stats{
					Observed:      5,
					Matched:       1,
					Evicted:       1,
					EvictedCaller: 1,
					Pending:       2,
				},
			},
		},
		{
			Name: "second half pairs with a full buffer without evicting",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
							telemetrytest.Client(t, 2, 1, "checkout", nil),
							telemetrytest.Server(t, 1, 2, "cart", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed: 3,
					Matched:  1,
					Pending:  1,
				},
			},
		},
		{
			Name: "evicted callee does not count as an evicted caller",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Server(t, 1, 2, "cart", nil),
							telemetrytest.Client(t, 2, 1, "checkout", nil),
							telemetrytest.Client(t, 3, 1, "frontend", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Stats: matcher.Stats{
					Observed: 3,
					Evicted:  1,
					Pending:  2,
				},
			},
		},
		{
			Name: "root server, database client and internal span",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
								Kind:    kind.Server,
								Trace:   1,
								SpanID:  1,
								Service: "frontend",
							}),
							telemetrytest.Client(t, 1, 2, "cart", map[string]string{
								"db.system": "redis",
							}),
							telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
								Kind:    kind.Internal,
								Trace:   1,
								SpanID:  3,
								Service: "cart",
							}),
						},
					},
				}
			},
			Want: matchOutcome{
				Stats: matcher.Stats{
					Observed:   2,
					Unpairable: 2,
				},
			},
		},
		{
			Name: "self-loop is a real call",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "cart", nil),
							telemetrytest.Server(t, 1, 2, "cart", nil),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("cart"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed: 2,
					Matched:  1,
				},
			},
		},
		{
			Name: "caller gave up while callee answered OK: the call failed",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "recommendation", failedDeadline),
							telemetrytest.Server(t, 1, 2, "flagd", okGRPC),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("recommendation"),
						Callee: telemetrytest.Service("flagd"),
						Failed: true,
					},
				},
				Stats: matcher.Stats{
					Observed: 2,
					Matched:  1,
				},
			},
		},
		{
			Name: "callee answered 503 while caller saw OK: the call failed",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Server(t, 1, 2, "payment", map[string]string{
								"http.response.status_code": "503",
							}),
							telemetrytest.Client(t, 1, 1, "checkout", okGRPC),
						},
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("checkout"),
						Callee: telemetrytest.Service("payment"),
						Failed: true,
					},
				},
				Stats: matcher.Stats{
					Observed: 2,
					Matched:  1,
				},
			},
		},
		{
			Name: "time going back does not shorten the wait of a new half",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				return testLimits(), []batchStep{
					{
						Spans: []span.Span{
							telemetrytest.Client(t, 1, 1, "frontend", nil),
						},
					},
					{
						At: 8 * time.Second,
					},
					{
						At: 2 * time.Second,
						Spans: []span.Span{
							telemetrytest.Client(t, 2, 1, "checkout", nil),
						},
					},
					{
						At: 12 * time.Second,
					},
				}
			},
			Want: matchOutcome{
				ExpiredCallers: []call.Unanswered{
					{
						Service: telemetrytest.Service("frontend"),
					},
				},
				Stats: matcher.Stats{
					Observed:      2,
					Expired:       1,
					ExpiredCaller: 1,
					Pending:       1,
				},
			},
		},
		{
			Name: "full tombstones evict the oldest, and its retry counts again",
			Args: func(t *testing.T) (matcher.Limits, []batchStep) {
				first := []span.Span{
					telemetrytest.Client(t, 1, 1, "frontend", nil),
					telemetrytest.Server(t, 1, 2, "cart", nil),
				}
				return testLimits(), []batchStep{
					{
						Spans: first,
					},
					{
						At: time.Second,
						Spans: []span.Span{
							telemetrytest.Client(t, 2, 1, "checkout", nil),
							telemetrytest.Server(t, 2, 2, "payment", nil),
						},
					},
					{
						At: 2 * time.Second,
						Spans: []span.Span{
							telemetrytest.Client(t, 3, 1, "checkout", nil),
							telemetrytest.Server(t, 3, 2, "shipping", nil),
						},
					},
					{
						At:    3 * time.Second,
						Spans: first,
					},
				}
			},
			Want: matchOutcome{
				Calls: []call.Call{
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
					{
						Caller: telemetrytest.Service("checkout"),
						Callee: telemetrytest.Service("payment"),
					},
					{
						Caller: telemetrytest.Service("checkout"),
						Callee: telemetrytest.Service("shipping"),
					},
					{
						Caller: telemetrytest.Service("frontend"),
						Callee: telemetrytest.Service("cart"),
					},
				},
				Stats: matcher.Stats{
					Observed:         8,
					Matched:          4,
					TombstoneEvicted: 2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			limits, steps := tt.Args(t)
			got := runMatcher(t, limits, steps)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("outcome mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewCallMatcherRejectsInvalidLimits(t *testing.T) {
	tests := []struct {
		Name    string
		Args    func(*matcher.Limits)
		Want    string
		WantErr error
	}{
		{
			Name: "zero ttl",
			Args: func(l *matcher.Limits) {
				l.TTL = 0
			},
			Want:    "invalid matcher limits: pairing ttl 0s must be positive",
			WantErr: matcher.ErrInvalidLimits,
		},
		{
			Name: "zero max pending",
			Args: func(l *matcher.Limits) {
				l.MaxPending = 0
			},
			Want:    "invalid matcher limits: max pending 0 must be positive",
			WantErr: matcher.ErrInvalidLimits,
		},
		{
			Name: "tombstone ttl below pairing ttl",
			Args: func(l *matcher.Limits) {
				l.TombstoneTTL = 5 * time.Second
			},
			Want:    "invalid matcher limits: tombstone ttl 5s is below pairing ttl 10s",
			WantErr: matcher.ErrInvalidLimits,
		},
		{
			Name: "zero max tombstones",
			Args: func(l *matcher.Limits) {
				l.MaxTombstones = 0
			},
			Want:    "invalid matcher limits: invalid tombstone limit: max tombstones 0 must be positive",
			WantErr: matcher.ErrInvalidLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			limits := testLimits()
			tt.Args(&limits)
			_, err := matcher.New(limits)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
