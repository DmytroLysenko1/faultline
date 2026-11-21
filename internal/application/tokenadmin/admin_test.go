package tokenadmin_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	testTenant     = "shop"
	otherTenant    = "bank"
	settle         = 7 * time.Second
	compensation   = 6 * time.Second
	writeAttempt   = 2 * time.Second
	settleRead     = time.Second
	deliveryPause  = time.Second
	callerBudget   = 3 * time.Second
	tokenBytes     = credential.IDLength + credential.SecretBytes
	firstIssuedID  = credential.ID("abcdefghijkl")
	bubbleYear     = 2000
	issuedAtOffset = time.Hour
)

var (
	errAnnounce   = errors.New("stderr closed")
	errShow       = errors.New("output file write failed")
	errDisconnect = errors.New("connection lost after the publish")
	errEntropy    = errors.New("entropy source failed")
)

var (
	bubbleStart = time.Date(bubbleYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	issuedAt    = bubbleStart.Add(issuedAtOffset)
)

type fakeClock struct{}

func (fakeClock) Now() time.Time {
	return issuedAt
}

type registerFault struct {
	err         error
	stores      bool
	waitsForEnd bool
}

type revokeFault struct {
	err             error
	answer          tokenadmin.Revocation
	losesFirstReply bool
}

type fakeStore struct {
	grants           map[credential.ID]credential.Grant
	register         registerFault
	revoke           revokeFault
	registerEndedAt  time.Time
	registerDeadline time.Time
	revokeDeadline   time.Time
	revokeCanceled   bool
	listDeadline     time.Time
	listErr          error
}

func newFakeStore(seeded ...credential.Grant) *fakeStore {
	store := &fakeStore{grants: map[credential.ID]credential.Grant{}, registerEndedAt: bubbleStart}
	for _, grant := range seeded {
		store.grants[grant.ID] = grant
	}
	return store
}

func (s *fakeStore) Register(ctx context.Context, grant credential.Grant) error {
	s.registerDeadline, _ = ctx.Deadline()
	if s.register.waitsForEnd {
		<-ctx.Done()
		s.registerEndedAt = s.registerDeadline
	}
	if _, taken := s.grants[grant.ID]; taken {
		return tokenadmin.ErrIDTaken
	}
	if s.register.err == nil || s.register.stores {
		s.grants[grant.ID] = grant
	}
	return s.register.err
}

func (s *fakeStore) Revoke(ctx context.Context, id credential.ID) (tokenadmin.Revocation, error) {
	s.revokeDeadline, _ = ctx.Deadline()
	s.revokeCanceled = ctx.Err() != nil
	if s.revoke.err != nil {
		return 0, s.revoke.err
	}
	if s.revoke.losesFirstReply && !s.retryFits() {
		return 0, tokenadmin.ErrOutcomeUnknown
	}
	_, present := s.grants[id]
	delete(s.grants, id)
	return s.revocation(present), nil
}

func (s *fakeStore) retryFits() bool {
	retryStarts := s.registerEndedAt.Add(writeAttempt + settleRead + deliveryPause)
	return s.revokeDeadline.Sub(retryStarts) >= writeAttempt
}

func (s *fakeStore) revocation(present bool) tokenadmin.Revocation {
	if s.revoke.answer != 0 {
		return s.revoke.answer
	}
	if present {
		return tokenadmin.Revoked
	}
	return tokenadmin.NothingToRevoke
}

func (s *fakeStore) List(ctx context.Context) ([]credential.Grant, error) {
	s.listDeadline, _ = ctx.Deadline()
	if s.listErr != nil {
		return nil, s.listErr
	}
	return slices.Collect(maps.Values(s.grants)), nil
}

func (s *fakeStore) storedIDs() []credential.ID {
	return slices.Sorted(maps.Keys(s.grants))
}

type showing struct {
	Token       credential.IngestToken
	StoredByNow bool
	CallerLive  bool
}

type fakeOutput struct {
	store                *fakeStore
	announceErr          error
	showErr              error
	cancel               context.CancelFunc
	announced            []credential.ID
	storedBeforeAnnounce bool
	announcedTenants     []string
	shown                []showing
}

func (o *fakeOutput) Announce(_ context.Context, id credential.ID, tenant string) error {
	_, stored := o.store.grants[id]
	o.storedBeforeAnnounce = o.storedBeforeAnnounce || stored
	o.announced = append(o.announced, id)
	o.announcedTenants = append(o.announcedTenants, tenant)
	return o.announceErr
}

func (o *fakeOutput) Show(ctx context.Context, token credential.IngestToken) error {
	if o.cancel != nil {
		o.cancel()
	}
	if o.showErr != nil {
		return o.showErr
	}
	_, stored := o.store.grants[token.ID]
	o.shown = append(o.shown, showing{Token: token, StoredByNow: stored, CallerLive: ctx.Err() == nil})
	return nil
}

type fixture struct {
	admin  *tokenadmin.Admin
	store  *fakeStore
	output *fakeOutput
	random *bytes.Reader
}

func entropyBytes() []byte {
	stream := make([]byte, tokenBytes)
	for index := range stream {
		stream[index] = byte(index)
	}
	return stream
}

func newFixture(t *testing.T, store *fakeStore, budget tokenadmin.Budget) fixture {
	t.Helper()
	output := &fakeOutput{store: store}
	random := bytes.NewReader(entropyBytes())
	admin, err := tokenadmin.NewAdmin(store, random, fakeClock{}, output, budget)
	if err != nil {
		t.Fatalf("NewAdmin: %v", err)
	}
	return fixture{admin: admin, store: store, output: output, random: random}
}

func defaultBudget() tokenadmin.Budget {
	return tokenadmin.Budget{Settle: settle, Compensation: compensation}
}

func seededGrant(t *testing.T) credential.Grant {
	t.Helper()
	_, grant, err := credential.Generate(bytes.NewReader(entropyBytes()), otherTenant, issuedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return grant
}

func expectErrors(t *testing.T, err error, want ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want %v", want)
	}
	for _, target := range want {
		if !errors.Is(err, target) {
			t.Errorf("err = %v, want it to carry %v", err, target)
		}
	}
}

func expectOnlyTheLiveTokenMeaning(t *testing.T, err error) {
	t.Helper()
	for _, sentinel := range []error{
		tokenadmin.ErrOutcomeUnknown, tokenadmin.ErrStoreUnavailable, tokenadmin.ErrRevokeBlocked,
		tokenadmin.ErrIDTaken, tokenadmin.ErrTokenLimit, tokenadmin.ErrNotShown,
	} {
		if errors.Is(err, sentinel) {
			t.Errorf("err = %v also matches %v: a caller checking it first would misread a live token", err, sentinel)
		}
	}
}

func TestIssueShowsTheTokenOnceAfterItIsStored(t *testing.T) {
	f := newFixture(t, newFakeStore(), defaultBudget())

	id, err := f.admin.Issue(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if len(f.output.shown) != 1 {
		t.Fatalf("%d tokens shown, want exactly one", len(f.output.shown))
	}
	shown := f.output.shown[0]
	want := credential.Grant{ID: id, Tenant: testTenant, Digest: shown.Token.Digest(), IssuedAt: issuedAt}
	if diff := cmp.Diff(map[credential.ID]credential.Grant{id: want}, f.store.grants); diff != "" {
		t.Errorf("stored grants (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]credential.ID{id}, f.output.announced); diff != "" || f.output.storedBeforeAnnounce {
		t.Errorf("announced %v (already stored: %v), want the id alone and before storing",
			f.output.announced, f.output.storedBeforeAnnounce)
	}
	if diff := cmp.Diff([]string{testTenant}, f.output.announcedTenants); diff != "" {
		t.Errorf("announced tenants (-want +got):\n%s", diff)
	}
	if shown.Token.ID != id || !shown.StoredByNow || !shown.CallerLive {
		t.Errorf("shown %+v for id %s, want the issued token, already stored, on the caller's live context", shown, id)
	}
}

func TestIssueStoresNothingWhenTheIDCannotBeAnnounced(t *testing.T) {
	f := newFixture(t, newFakeStore(), defaultBudget())
	f.output.announceErr = errAnnounce

	id, err := f.admin.Issue(t.Context(), testTenant)

	expectErrors(t, err, tokenadmin.ErrNotAnnounced, errAnnounce)
	if id != "" || len(f.store.grants) != 0 || len(f.output.shown) != 0 {
		t.Errorf("id %q, %d grants stored, %d tokens shown; want none of them", id, len(f.store.grants), len(f.output.shown))
	}
}

func TestIssueRevokesATokenItCouldNotShow(t *testing.T) {
	f := newFixture(t, newFakeStore(), defaultBudget())
	f.output.showErr = errShow

	id, err := f.admin.Issue(t.Context(), testTenant)

	expectErrors(t, err, tokenadmin.ErrNotShown, errShow)
	if errors.Is(err, tokenadmin.ErrLiveTokenNotShown) {
		t.Errorf("err = %v reports a revoked token as live", err)
	}
	if id != "" || len(f.store.grants) != 0 {
		t.Errorf("id %q and grants %v left after a failed show, want none", id, f.store.grants)
	}
}

func TestShowGetsTheCallersContextAfterASlowRegister(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore()
		store.register = registerFault{waitsForEnd: true}
		f := newFixture(t, store, defaultBudget())

		id, err := f.admin.Issue(t.Context(), testTenant)
		if err != nil {
			t.Fatalf("Issue after a register that used the whole settle budget: %v", err)
		}

		if len(f.output.shown) != 1 || !f.output.shown[0].CallerLive || f.output.shown[0].Token.ID != id {
			t.Errorf("shown %+v, want the token shown once on the caller's live context", f.output.shown)
		}
	})
}

func TestAnUnknownOutcomeIsReportedAsPossiblyLive(t *testing.T) {
	type args struct {
		fault registerFault
	}
	tests := []struct {
		name    string
		args    args
		want    []credential.ID
		wantErr error
	}{
		{
			name:    "written before the reply was lost",
			args:    args{fault: registerFault{err: tokenadmin.ErrOutcomeUnknown, stores: true}},
			want:    nil,
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
		{
			name:    "never written",
			args:    args{fault: registerFault{err: tokenadmin.ErrOutcomeUnknown}},
			want:    nil,
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
		{
			name:    "an error without a meaning",
			args:    args{fault: registerFault{err: errDisconnect, stores: true}},
			want:    nil,
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
		{
			name:    "unknown outcome next to a store failure",
			args:    args{fault: registerFault{err: errors.Join(tokenadmin.ErrOutcomeUnknown, tokenadmin.ErrStoreUnavailable)}},
			want:    nil,
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, newFakeStore(), defaultBudget())
			f.store.register = tt.args.fault

			id, err := f.admin.Issue(t.Context(), testTenant)

			expectErrors(t, err, tt.wantErr)
			expectOnlyTheLiveTokenMeaning(t, err)
			if diff := cmp.Diff(tt.want, f.store.storedIDs()); diff != "" {
				t.Errorf("grants after compensation (-want +got):\n%s", diff)
			}
			if id != "" || len(f.output.shown) != 0 || !strings.Contains(err.Error(), string(f.output.announced[0])) {
				t.Errorf("id %q, %d shown, err %q; want no id, nothing shown, the announced id in the error",
					id, len(f.output.shown), err)
			}
		})
	}
}

func TestAFailedCompensationIsReportedAsLive(t *testing.T) {
	type args struct {
		revoke revokeFault
	}
	tests := []struct {
		name    string
		args    args
		want    []credential.ID
		wantErr error
	}{
		{
			name:    "store refuses the revocation",
			args:    args{revoke: revokeFault{err: tokenadmin.ErrRevokeBlocked}},
			want:    []credential.ID{firstIssuedID},
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
		{
			name:    "revocation outcome unknown",
			args:    args{revoke: revokeFault{err: tokenadmin.ErrOutcomeUnknown}},
			want:    []credential.ID{firstIssuedID},
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
		{
			name:    "key absent after a sent attempt",
			args:    args{revoke: revokeFault{answer: tokenadmin.Absent}},
			want:    nil,
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
		{
			name:    "nothing to revoke right after the write",
			args:    args{revoke: revokeFault{answer: tokenadmin.NothingToRevoke}},
			want:    nil,
			wantErr: tokenadmin.ErrLiveTokenNotShown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, newFakeStore(), defaultBudget())
			f.output.showErr = errShow
			f.store.revoke = tt.args.revoke

			_, err := f.admin.Issue(t.Context(), testTenant)

			expectErrors(t, err, tt.wantErr)
			expectOnlyTheLiveTokenMeaning(t, err)
			expectCauseTexts(t, err, errShow, tt.args.revoke.err)
			if diff := cmp.Diff(tt.want, f.store.storedIDs()); diff != "" {
				t.Errorf("grants (-want +got):\n%s", diff)
			}
		})
	}
}

func expectCauseTexts(t *testing.T, err error, causes ...error) {
	t.Helper()
	for _, cause := range causes {
		if cause != nil && !strings.Contains(err.Error(), cause.Error()) {
			t.Errorf("err %q does not tell the operator %q", err, cause)
		}
	}
}

func TestCompensationSurvivesACanceledCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, newFakeStore(), defaultBudget())
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f.output.cancel = cancel
		f.output.showErr = context.Canceled

		_, err := f.admin.Issue(ctx, testTenant)

		expectErrors(t, err, tokenadmin.ErrNotShown)
		if f.store.revokeCanceled || !f.store.revokeDeadline.Equal(bubbleStart.Add(compensation)) {
			t.Errorf("compensation canceled %v with deadline %v, want live until %v",
				f.store.revokeCanceled, f.store.revokeDeadline, bubbleStart.Add(compensation))
		}
		if len(f.store.grants) != 0 {
			t.Errorf("grants %v left after a canceled command, want none", f.store.grants)
		}
	})
}

func TestCompensationBudgetFitsOneRetry(t *testing.T) {
	type args struct {
		compensation time.Duration
	}
	tests := []struct {
		name    string
		args    args
		want    []credential.ID
		wantErr error
	}{
		{name: "the default budget", args: args{compensation: compensation}, want: nil},
		{name: "a budget of five seconds", args: args{compensation: 5 * time.Second}, want: []credential.ID{firstIssuedID}},
		{name: "a budget of a single attempt", args: args{compensation: writeAttempt}, want: []credential.ID{firstIssuedID}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := newFakeStore()
				store.register = registerFault{err: tokenadmin.ErrOutcomeUnknown, stores: true, waitsForEnd: true}
				store.revoke = revokeFault{losesFirstReply: true}
				f := newFixture(t, store, tokenadmin.Budget{Settle: settle, Compensation: tt.args.compensation})
				ctx, cancel := context.WithTimeout(t.Context(), callerBudget)
				defer cancel()

				_, err := f.admin.Issue(ctx, testTenant)

				expectErrors(t, err, tokenadmin.ErrLiveTokenNotShown)
				if diff := cmp.Diff(tt.want, f.store.storedIDs()); diff != "" {
					t.Errorf("grants after compensation (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestIssueReportsATakenIDWithoutRetrying(t *testing.T) {
	seeded := seededGrant(t)
	onlyFirst := []credential.ID{firstIssuedID}
	type args struct {
		seeded []credential.Grant
		fault  registerFault
	}
	tests := []struct {
		name    string
		args    args
		want    []credential.ID
		wantErr error
	}{
		{name: "id taken", args: args{seeded: []credential.Grant{seeded}}, want: onlyFirst, wantErr: tokenadmin.ErrIDTaken},
		{
			name:    "token limit",
			args:    args{fault: registerFault{err: tokenadmin.ErrTokenLimit}},
			want:    nil,
			wantErr: tokenadmin.ErrTokenLimit,
		},
		{
			name:    "store unavailable before sending",
			args:    args{fault: registerFault{err: tokenadmin.ErrStoreUnavailable}},
			want:    nil,
			wantErr: tokenadmin.ErrStoreUnavailable,
		},
		{
			name:    "store unavailable is trusted to mean nothing was written",
			args:    args{fault: registerFault{err: tokenadmin.ErrStoreUnavailable, stores: true}},
			want:    []credential.ID{firstIssuedID},
			wantErr: tokenadmin.ErrStoreUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore(tt.args.seeded...)
			store.register = tt.args.fault
			f := newFixture(t, store, defaultBudget())
			f.random.Reset(append(entropyBytes(), entropyBytes()...))

			_, err := f.admin.Issue(t.Context(), testTenant)

			expectErrors(t, err, tt.wantErr)
			if errors.Is(err, tokenadmin.ErrLiveTokenNotShown) || len(f.output.shown) != 0 || !f.store.revokeDeadline.IsZero() {
				t.Errorf("err = %v, %d shown, revoked by %v; want a plain refusal without a compensation",
					err, len(f.output.shown), f.store.revokeDeadline)
			}
			if diff := cmp.Diff(tt.want, f.store.storedIDs()); diff != "" {
				t.Errorf("grants (-want +got):\n%s", diff)
			}
			if f.random.Len() != tokenBytes {
				t.Errorf("%d random bytes left, want %d: Issue generated a second token", f.random.Len(), tokenBytes)
			}
		})
	}
}

func TestRevokeReportsEachRevocation(t *testing.T) {
	absent := revokeFault{answer: tokenadmin.Absent}
	blocked := revokeFault{err: tokenadmin.ErrRevokeBlocked}
	seeded := []credential.Grant{seededGrant(t)}
	type args struct {
		seeded []credential.Grant
		fault  revokeFault
	}
	tests := []struct {
		name    string
		args    args
		want    tokenadmin.Revocation
		wantErr error
	}{
		{name: "revoked", args: args{seeded: seeded}, want: tokenadmin.Revoked},
		{name: "absent after a sent attempt", args: args{fault: absent}, want: tokenadmin.Absent},
		{name: "nothing to revoke", want: tokenadmin.NothingToRevoke, wantErr: tokenadmin.ErrUnknownToken},
		{name: "blocked", args: args{seeded: seeded, fault: blocked}, wantErr: tokenadmin.ErrRevokeBlocked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore(tt.args.seeded...)
			store.revoke = tt.args.fault
			f := newFixture(t, store, defaultBudget())

			got, err := f.admin.Revoke(t.Context(), firstIssuedID)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
			if _, present := f.store.grants[firstIssuedID]; present && tt.wantErr == nil {
				t.Errorf("grant %s still stored after %v", firstIssuedID, got)
			}
		})
	}
}

func TestRevokeRefusesAMalformedIDBeforeTheStore(t *testing.T) {
	onlyFirst := []credential.ID{firstIssuedID}
	unknown := tokenadmin.ErrUnknownToken
	type args struct {
		id credential.ID
	}
	tests := []struct {
		name    string
		args    args
		want    []credential.ID
		wantErr error
	}{
		{name: "every subject", args: args{id: "*"}, want: onlyFirst, wantErr: unknown},
		{name: "every subject below", args: args{id: ">"}, want: onlyFirst, wantErr: unknown},
		{name: "a dotted subject", args: args{id: "abcdef.ghijk"}, want: onlyFirst, wantErr: unknown},
		{name: "empty", want: onlyFirst, wantErr: unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, newFakeStore(seededGrant(t)), defaultBudget())

			got, err := f.admin.Revoke(t.Context(), tt.args.id)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, f.store.storedIDs()); diff != "" || got != 0 || !f.store.revokeDeadline.IsZero() {
				t.Errorf("revocation %v, store reached %v, grants (-want +got):\n%s",
					got, !f.store.revokeDeadline.IsZero(), diff)
			}
		})
	}
}

func TestRegisterRunsUnderTheSettleDeadline(t *testing.T) {
	type args struct {
		callerBudget time.Duration
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr error
	}{
		{name: "caller without a deadline", want: bubbleStart.Add(settle)},
		{name: "caller with a shorter deadline", args: args{callerBudget: callerBudget}, want: bubbleStart.Add(callerBudget)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, newFakeStore(), defaultBudget())
				ctx, cancel := withOptionalTimeout(t.Context(), tt.args.callerBudget)
				defer cancel()

				_, err := f.admin.Issue(ctx, testTenant)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if diff := cmp.Diff(tt.want, f.store.registerDeadline); diff != "" {
					t.Errorf("register deadline (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestRevokeAndListRunUnderTheSettleDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, newFakeStore(seededGrant(t)), defaultBudget())

		if _, err := f.admin.List(t.Context()); err != nil {
			t.Fatalf("List: %v", err)
		}
		if _, err := f.admin.Revoke(t.Context(), firstIssuedID); err != nil {
			t.Fatalf("Revoke: %v", err)
		}

		want := []time.Time{bubbleStart.Add(settle), bubbleStart.Add(settle)}
		if diff := cmp.Diff(want, []time.Time{f.store.listDeadline, f.store.revokeDeadline}); diff != "" {
			t.Errorf("list and revoke deadlines (-want +got):\n%s", diff)
		}
	})
}

func withOptionalTimeout(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if budget == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
}

func TestListShowsTheStoredGrants(t *testing.T) {
	grant := seededGrant(t)
	f := newFixture(t, newFakeStore(grant), defaultBudget())

	got, err := f.admin.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if diff := cmp.Diff([]credential.Grant{grant}, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestNewAdminRefusesAZeroBudget(t *testing.T) {
	invalid := tokenadmin.ErrInvalidBudget
	type args struct {
		budget tokenadmin.Budget
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr error
	}{
		{name: "both set", args: args{budget: defaultBudget()}, want: true},
		{name: "zero budget", args: args{}, wantErr: invalid},
		{name: "zero settle", args: args{budget: tokenadmin.Budget{Compensation: compensation}}, wantErr: invalid},
		{name: "zero compensation", args: args{budget: tokenadmin.Budget{Settle: settle}}, wantErr: invalid},
		{
			name:    "negative compensation",
			args:    args{budget: tokenadmin.Budget{Settle: settle, Compensation: -compensation}},
			wantErr: invalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			output := &fakeOutput{store: store}
			admin, err := tokenadmin.NewAdmin(store, bytes.NewReader(nil), fakeClock{}, output, tt.args.budget)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, admin != nil); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewAdminRefusesAMissingDependency(t *testing.T) {
	store := newFakeStore()
	output := &fakeOutput{store: store}
	random := bytes.NewReader(nil)
	type args struct {
		build func() (*tokenadmin.Admin, error)
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{name: "everything", args: args{build: func() (*tokenadmin.Admin, error) {
			return tokenadmin.NewAdmin(store, random, fakeClock{}, output, defaultBudget())
		}}, want: ""},
		{name: "no store", want: "grant store", args: args{build: func() (*tokenadmin.Admin, error) {
			return tokenadmin.NewAdmin(nil, random, fakeClock{}, output, defaultBudget())
		}}, wantErr: tokenadmin.ErrMissingDependency},
		{name: "no randomness", want: "random reader", args: args{build: func() (*tokenadmin.Admin, error) {
			return tokenadmin.NewAdmin(store, nil, fakeClock{}, output, defaultBudget())
		}}, wantErr: tokenadmin.ErrMissingDependency},
		{name: "no clock", want: "clock", args: args{build: func() (*tokenadmin.Admin, error) {
			return tokenadmin.NewAdmin(store, random, nil, output, defaultBudget())
		}}, wantErr: tokenadmin.ErrMissingDependency},
		{name: "no output", want: "token output", args: args{build: func() (*tokenadmin.Admin, error) {
			return tokenadmin.NewAdmin(store, random, fakeClock{}, nil, defaultBudget())
		}}, wantErr: tokenadmin.ErrMissingDependency},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admin, err := tt.args.build()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, missingName(admin, err)); diff != "" {
				t.Errorf("missing dependency (-want +got):\n%s", diff)
			}
		})
	}
}

func missingName(admin *tokenadmin.Admin, err error) string {
	if admin != nil {
		return ""
	}
	name, _, _ := strings.Cut(err.Error(), ":")
	return name
}

func TestIssueStopsWhenRandomnessFails(t *testing.T) {
	store := newFakeStore()
	output := &fakeOutput{store: store}
	admin, err := tokenadmin.NewAdmin(store, iotest.ErrReader(errEntropy), fakeClock{}, output, defaultBudget())
	if err != nil {
		t.Fatalf("NewAdmin: %v", err)
	}

	id, err := admin.Issue(t.Context(), testTenant)

	expectErrors(t, err, credential.ErrNoRandomness, errEntropy)
	if id != "" || len(output.announced) != 0 || len(store.grants) != 0 || len(output.shown) != 0 {
		t.Errorf("id %q, announced %v, stored %v, shown %d; want nothing at all",
			id, output.announced, store.grants, len(output.shown))
	}
}

func TestListReportsAStoreFailure(t *testing.T) {
	store := newFakeStore(seededGrant(t))
	store.listErr = tokenadmin.ErrStoreUnavailable
	f := newFixture(t, store, defaultBudget())

	got, err := f.admin.List(t.Context())

	if !errors.Is(err, tokenadmin.ErrStoreUnavailable) {
		t.Fatalf("err = %v, want %v", err, tokenadmin.ErrStoreUnavailable)
	}
	if got != nil {
		t.Errorf("List returned %v next to an error, want nil", got)
	}
}
