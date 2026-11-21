package tokens

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
)

const (
	codeAccountLimits      = 10002
	codeClusterUnavailable = 10008
	codeTooManyRequests    = 10167
	codeTooLarge           = 10054
	codeStoreFailed        = 10077
	codeWrongLast          = 10071
	codeWrongLastConst     = 10164
	codeSealed             = 10109
	codePurgeFailed        = 10110
	codeInProcess          = 10158
	codeOther              = 10999
	budget                 = 7 * time.Second
	attemptOverhead        = 50 * time.Millisecond
)

func apiErr(code jetstream.ErrorCode, description string) error {
	return fmt.Errorf("publish: %w", &jetstream.APIError{ErrorCode: code, Description: description})
}

type ruleResult struct {
	Move     move
	Err      error
	IfAbsent move
}

func resultOf(step next) ruleResult {
	return ruleResult{Move: step.move, Err: step.err, IfAbsent: step.ifAbsent}
}

func TestRegisterRules(t *testing.T) {
	type args struct {
		duplicate bool
		err       error
	}
	tests := []struct {
		name string
		args args
		want ruleResult
	}{
		{name: "a new message is done", args: args{}, want: ruleResult{Move: moveDone}},
		{
			name: "a duplicate is settled and absent fails",
			args: args{duplicate: true},
			want: ruleResult{Move: moveSettle, IfAbsent: moveFail},
		},
		{name: "no responders pause", args: args{err: nats.ErrNoResponders}, want: ruleResult{Move: moveRetry}},
		{
			name: "a reconnecting client without a buffer never sent it",
			args: args{err: fmt.Errorf("publish: %w", nats.ErrReconnectBufExceeded)},
			want: ruleResult{Move: moveRetry},
		},
		{
			name: "a timeout is settled",
			args: args{err: context.DeadlineExceeded},
			want: ruleResult{Move: moveSettle, IfAbsent: moveRetry},
		},
		{
			name: "a lost connection is settled",
			args: args{err: nats.ErrDisconnected},
			want: ruleResult{Move: moveSettle, IfAbsent: moveRetry},
		},
		{
			name: "an in process reply is settled",
			args: args{err: apiErr(codeInProcess, "duplicate in process")},
			want: ruleResult{Move: moveSettle, IfAbsent: moveRetry},
		},
		{
			name: "a taken key is settled",
			args: args{err: apiErr(codeWrongLast, "wrong last sequence: 3")},
			want: ruleResult{Move: moveSettle, IfAbsent: moveRetry},
		},
		{
			name: "the constant form of a taken key is settled",
			args: args{err: apiErr(codeWrongLastConst, "wrong last sequence")},
			want: ruleResult{Move: moveSettle, IfAbsent: moveRetry},
		},
		{
			name: "a full bucket is the token limit",
			args: args{err: apiErr(codeStoreFailed, "maximum messages exceeded")},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrTokenLimit},
		},
		{
			name: "a too large value proves no record",
			args: args{err: apiErr(codeTooLarge, "message size exceeds maximum allowed")},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrStoreUnavailable},
		},
		{
			name: "a sealed stream proves no record",
			args: args{err: apiErr(codeSealed, "stream is sealed")},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrStoreUnavailable},
		},
		{
			name: "account limits prove no record",
			args: args{err: apiErr(codeAccountLimits, "resource limits exceeded for account")},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrStoreUnavailable},
		},
		{
			name: "a server without a meta leader may still have stored it",
			args: args{err: apiErr(codeClusterUnavailable, "JetStream system temporarily unavailable")},
			want: ruleResult{Move: moveSettle, IfAbsent: moveRetry},
		},
		{
			name: "a publish dropped by the stream queue was never stored",
			args: args{err: apiErr(codeTooManyRequests, "too many requests")},
			want: ruleResult{Move: moveRetry},
		},
		{
			name: "a publish refused by a non-leader was never stored",
			args: args{err: apiErr(codeNotLeader, "not leader")},
			want: ruleResult{Move: moveRetry},
		},
		{
			name: "a canceled publish is unknown",
			args: args{err: context.Canceled},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrOutcomeUnknown},
		},
		{
			name: "an unexpected answer after sending is unknown",
			args: args{err: apiErr(codeOther, "something new")},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrOutcomeUnknown},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resultOf(registerRules(tt.args.duplicate, tt.args.err))
			if diff := cmp.Diff(tt.want, got, cmp.Comparer(errors.Is)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRegisterSettled(t *testing.T) {
	type args struct {
		state    keyState
		ifAbsent move
	}
	tests := []struct {
		name string
		args args
		want ruleResult
	}{
		{name: "own record is done", args: args{state: keyOwn, ifAbsent: moveFail}, want: ruleResult{Move: moveDone}},
		{
			name: "a foreign record is a taken id",
			args: args{state: keyForeign, ifAbsent: moveRetry},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrIDTaken},
		},
		{
			name: "a duplicate without a key is unknown at once",
			args: args{state: keyAbsent, ifAbsent: moveFail},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrOutcomeUnknown},
		},
		{
			name: "an unreadable duplicate is unknown at once",
			args: args{state: keyUnreadable, ifAbsent: moveFail},
			want: ruleResult{Move: moveFail, Err: tokenadmin.ErrOutcomeUnknown},
		},
		{
			name: "a lost reply without a key retries",
			args: args{state: keyAbsent, ifAbsent: moveRetry},
			want: ruleResult{Move: moveRetry},
		},
		{
			name: "an unreadable key retries later",
			args: args{state: keyUnreadable, ifAbsent: moveRetry},
			want: ruleResult{Move: moveRetry},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resultOf(registerSettled(tt.args.state, tt.args.ifAbsent))
			if diff := cmp.Diff(tt.want, got, cmp.Comparer(errors.Is)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRevokeRules(t *testing.T) {
	type args struct {
		purged     uint64
		sentBefore bool
		err        error
	}
	type want struct {
		Revocation tokenadmin.Revocation
		Step       ruleResult
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "a purged key is revoked",
			args: args{purged: 1},
			want: want{Revocation: tokenadmin.Revoked, Step: ruleResult{Move: moveDone}},
		},
		{
			name: "nothing purged at once is nothing to revoke",
			args: args{},
			want: want{Revocation: tokenadmin.NothingToRevoke, Step: ruleResult{Move: moveDone}},
		},
		{
			name: "nothing purged after a sent attempt is absent",
			args: args{sentBefore: true},
			want: want{Revocation: tokenadmin.Absent, Step: ruleResult{Move: moveDone}},
		},
		{
			name: "a sealed stream refuses the purge",
			args: args{err: apiErr(codeSealed, "stream is sealed")},
			want: want{Step: ruleResult{Move: moveFail, Err: tokenadmin.ErrRevokeBlocked}},
		},
		{
			name: "a reconnecting client never sent the purge",
			args: args{err: fmt.Errorf("purge: %w", nats.ErrReconnectBufExceeded)},
			want: want{Step: ruleResult{Move: moveRetry}},
		},
		{
			name: "a deleted bucket purged nothing",
			args: args{err: apiErr(jetstream.JSErrCodeStreamNotFound, "stream not found")},
			want: want{Step: ruleResult{Move: moveFail, Err: tokenadmin.ErrStoreUnavailable}},
		},
		{
			name: "a denied purge is blocked",
			args: args{err: apiErr(codePurgeFailed, "stream purge not permitted")},
			want: want{Step: ruleResult{Move: moveFail, Err: tokenadmin.ErrRevokeBlocked}},
		},
		{
			name: "a non-leader answer may follow an applied purge",
			args: args{err: apiErr(codeClusterUnavailable, "JetStream system temporarily unavailable")},
			want: want{Step: ruleResult{Move: moveSettle, IfAbsent: moveRetry}},
		},
		{
			name: "a timeout is settled",
			args: args{err: nats.ErrTimeout},
			want: want{Step: ruleResult{Move: moveSettle, IfAbsent: moveRetry}},
		},
		{name: "no responders pause", args: args{err: nats.ErrNoResponders}, want: want{Step: ruleResult{Move: moveRetry}}},
		{
			name: "anything else is unknown",
			args: args{err: apiErr(codeOther, "new")},
			want: want{Step: ruleResult{Move: moveFail, Err: tokenadmin.ErrOutcomeUnknown}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			revocation, step := revokeRules(tt.args.purged, tt.args.sentBefore, tt.args.err)
			got := want{Revocation: revocation, Step: resultOf(step)}
			if diff := cmp.Diff(tt.want, got, cmp.Comparer(errors.Is)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRevokeSettled(t *testing.T) {
	revocation, step := revokeSettled(keyAbsent)
	if revocation != tokenadmin.Absent || step.move != moveDone {
		t.Errorf("absent key = %v, %v; want Absent, done", revocation, step.move)
	}
	for _, state := range []keyState{keyOwn, keyForeign, keyUnreadable} {
		if _, step := revokeSettled(state); step.move != moveRetry {
			t.Errorf("state %d moves %v, want retry", state, step.move)
		}
	}
}

type scripted struct {
	steps    []next
	sent     []bool
	calls    int
	calledAt []time.Duration
	start    time.Time
}

func (s *scripted) try(ctx context.Context, _ bool) (next, bool, error) {
	index := min(s.calls, len(s.steps)-1)
	s.calls++
	s.calledAt = append(s.calledAt, time.Since(s.start))
	if deadline, bounded := ctx.Deadline(); !bounded || (s.calls > 1 && time.Until(deadline) < writeAttempt) {
		panic("a repeated attempt started without room for its write")
	}
	return s.steps[index], s.sent[index], errors.New("attempt failed")
}

func TestASpentBudgetEndsWithoutAnotherRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		script := &scripted{steps: []next{{move: moveRetry}}, sent: []bool{false}, start: time.Now()}
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		defer cancel()
		err := untilSettled(ctx, script.try)
		want := []time.Duration{0, deliveryPause, 2 * deliveryPause, 3 * deliveryPause, 4 * deliveryPause, 5 * deliveryPause}
		if diff := cmp.Diff(want, script.calledAt); diff != "" {
			t.Errorf("attempt times (-want +got):\n%s", diff)
		}
		if !errors.Is(err, tokenadmin.ErrStoreUnavailable) {
			t.Errorf("err = %v, want ErrStoreUnavailable for attempts never delivered", err)
		}
		if ended := time.Since(script.start); ended != 5*deliveryPause {
			t.Errorf("returned after %s; the last attempt leaves no room, so no pause follows it", ended)
		}
	})
}

func TestASentAttemptNeverEndsAsStoreUnavailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		script := &scripted{
			steps: []next{{move: moveRetry}, {move: moveRetry}},
			sent:  []bool{true, false},
			start: time.Now(),
		}
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		defer cancel()
		err := untilSettled(ctx, script.try)
		if !errors.Is(err, tokenadmin.ErrOutcomeUnknown) || errors.Is(err, tokenadmin.ErrStoreUnavailable) {
			t.Errorf("err = %v, want only ErrOutcomeUnknown once an attempt was sent", err)
		}
	})
}

func TestAFailureKeepsItsCause(t *testing.T) {
	cause := errors.New("server said no")
	err := untilSettled(contextWithBudget(t), func(context.Context, bool) (next, bool, error) {
		return fail(tokenadmin.ErrIDTaken), true, cause
	})
	if !errors.Is(err, tokenadmin.ErrIDTaken) || !errors.Is(err, cause) {
		t.Errorf("err = %v, want ErrIDTaken joined with its cause", err)
	}
}

func TestACanceledCallerStartsNoAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	cancel()
	calls := 0
	err := untilSettled(ctx, func(context.Context, bool) (next, bool, error) {
		calls++
		return done(), true, nil
	})
	if calls != 0 || !errors.Is(err, tokenadmin.ErrStoreUnavailable) {
		t.Errorf("calls = %d, err = %v; want none and ErrStoreUnavailable", calls, err)
	}
}

func TestBoundedGivesADeadlineOnlyWhenMissing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unbounded, cancel := bounded(t.Context())
		defer cancel()
		deadline, _ := unbounded.Deadline()
		if got := time.Until(deadline); got != defaultBudget {
			t.Errorf("default budget = %s, want %s", got, defaultBudget)
		}
		limited, cancelLimited := context.WithTimeout(t.Context(), time.Second)
		defer cancelLimited()
		kept, cancelKept := bounded(limited)
		defer cancelKept()
		if got, _ := kept.Deadline(); time.Until(got) != time.Second {
			t.Errorf("caller deadline replaced: %s left", time.Until(got))
		}
	})
}

func contextWithBudget(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	t.Cleanup(cancel)
	return ctx
}

func TestAProofOfNoRecordAfterASentAttemptIsUnknown(t *testing.T) {
	tests := []struct {
		name       string
		step       next
		sentBefore bool
		want       error
	}{
		{name: "a full bucket at once", step: fail(tokenadmin.ErrTokenLimit), want: tokenadmin.ErrTokenLimit},
		{
			name:       "a full bucket after a sent attempt",
			step:       fail(tokenadmin.ErrTokenLimit),
			sentBefore: true,
			want:       tokenadmin.ErrOutcomeUnknown,
		},
		{
			name:       "account limits after a sent attempt",
			step:       fail(tokenadmin.ErrStoreUnavailable),
			sentBefore: true,
			want:       tokenadmin.ErrOutcomeUnknown,
		},
		{name: "a taken id stays taken", step: fail(tokenadmin.ErrIDTaken), sentBefore: true, want: tokenadmin.ErrIDTaken},
		{name: "a success stays a success", step: done(), sentBefore: true},
		{
			name:       "a refused record after a sent attempt keeps its cause",
			step:       fail(errors.Join(tokenadmin.ErrStoreUnavailable, ErrUnacceptedGrant)),
			sentBefore: true,
			want:       ErrUnacceptedGrant,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := provenAfterSending(tt.step, tt.sentBefore)
			if !errors.Is(got.err, tt.want) || (tt.want == nil) != (got.err == nil) {
				t.Errorf("err = %v, want %v", got.err, tt.want)
			}
			if tt.sentBefore && provesNoRecord(got.err) {
				t.Errorf("err = %v proves no record after an attempt that was sent", got.err)
			}
		})
	}
}

func TestStoreErrorsAlwaysCarryATokenadminMeaning(t *testing.T) {
	meanings := []error{
		tokenadmin.ErrIDTaken, tokenadmin.ErrTokenLimit, tokenadmin.ErrOutcomeUnknown,
		tokenadmin.ErrStoreUnavailable, tokenadmin.ErrRevokeBlocked,
	}
	carries := func(err error) bool {
		return slices.ContainsFunc(meanings, func(meaning error) bool { return errors.Is(err, meaning) })
	}
	var failures []next
	for _, rule := range registerReasons {
		failures = append(failures, rule)
	}
	for _, rule := range revokeReasons {
		failures = append(failures, rule)
	}
	failures = append(failures,
		registerRules(false, apiErr(codeOther, "new")),
		registerSettled(keyForeign, moveRetry),
		registerSettled(keyAbsent, moveFail),
		revokeError(apiErr(codePurgeFailed, "denied")),
	)
	for _, step := range failures {
		if step.move == moveFail && !carries(step.err) {
			t.Errorf("a failure without a tokenadmin meaning: %v", step.err)
		}
	}
	for _, sent := range []bool{true, false} {
		if err := endOfBudget(sent); !carries(err) {
			t.Errorf("endOfBudget(%v) = %v", sent, err)
		}
	}
}

func TestAFirstAttemptRunsEvenOnAShortBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), writeAttempt/2)
		defer cancel()
		calls := 0
		err := untilSettled(ctx, func(context.Context, bool) (next, bool, error) {
			calls++
			return retryLater(), true, nil
		})
		if calls != 1 || !errors.Is(err, tokenadmin.ErrOutcomeUnknown) {
			t.Errorf("calls = %d, err = %v; want one attempt and an unknown outcome", calls, err)
		}
	})
}

func TestACompensationBudgetFitsTwoAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const compensation = 6 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), compensation)
		defer cancel()
		var started []time.Duration
		begin := time.Now()
		err := untilSettled(ctx, func(ctx context.Context, _ bool) (next, bool, error) {
			started = append(started, time.Since(begin))
			attempt, cancelAttempt := context.WithTimeout(ctx, writeAttempt+settleRead+attemptOverhead)
			defer cancelAttempt()
			<-attempt.Done()
			return retryLater(), true, nil
		})
		want := []time.Duration{0, writeAttempt + settleRead + attemptOverhead + deliveryPause}
		if diff := cmp.Diff(want, started); diff != "" || !errors.Is(err, tokenadmin.ErrOutcomeUnknown) {
			t.Errorf("attempts (-want +got):\n%s; err %v", diff, err)
		}
	})
}

func TestOnlyAStoredAttemptCountsAsSent(t *testing.T) {
	tests := []struct {
		name string
		args error
		want bool
	}{
		{name: "a reconnecting client", args: fmt.Errorf("publish: %w", nats.ErrReconnectBufExceeded)},
		{name: "no responders", args: nats.ErrNoResponders},
		{name: "a full stream queue", args: apiErr(codeTooManyRequests, "too many requests")},
		{name: "a non-leader refusal", args: apiErr(codeNotLeader, "not leader")},
		{name: "a timeout", args: context.DeadlineExceeded, want: true},
		{name: "a meta group without a leader", args: apiErr(codeClusterUnavailable, "unavailable"), want: true},
		{name: "a stored message", args: nil, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := attemptSent(tt.args); got != tt.want {
				t.Errorf("attemptSent = %v, want %v", got, tt.want)
			}
		})
	}
}
