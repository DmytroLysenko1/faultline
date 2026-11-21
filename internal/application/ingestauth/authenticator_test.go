package ingestauth_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	tenant       = "shop"
	otherTenant  = "bank"
	maxStaleness = 45 * time.Second
	checkHorizon = 37 * time.Second
	tokenBytes   = credential.IDLength + credential.SecretBytes
)

var checkedAt = time.Date(2026, time.November, 21, 12, 0, 0, 0, time.UTC)

type fakeDirectory struct {
	snapshot ingestauth.Snapshot
}

func (d *fakeDirectory) Snapshot(context.Context) ingestauth.Snapshot {
	return d.snapshot
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

type issued struct {
	raw   string
	token credential.IngestToken
	grant credential.Grant
}

func issue(t *testing.T, seed byte, owner string) issued {
	t.Helper()
	token, grant, err := credential.Generate(bytes.NewReader(bytes.Repeat([]byte{seed}, tokenBytes)), owner, checkedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return issued{
		raw:   credential.Reveal(token),
		token: token,
		grant: grant,
	}
}

func policy() ingestauth.Policy {
	return ingestauth.Policy{
		Tenant:       tenant,
		MaxStaleness: maxStaleness,
		CheckHorizon: checkHorizon,
	}
}

func newAuthenticator(t *testing.T, grants credential.Grants, age time.Duration) *ingestauth.Authenticator {
	t.Helper()
	directory := &fakeDirectory{snapshot: ingestauth.Snapshot{Grants: grants, CheckedAt: checkedAt}}
	authenticator, err := ingestauth.NewAuthenticator(directory, &fakeClock{now: checkedAt.Add(age)}, policy())
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	return authenticator
}

func grantsOf(tokens ...issued) credential.Grants {
	grants := credential.Grants{}
	for i := range tokens {
		grants[tokens[i].grant.ID] = tokens[i].grant
	}
	return grants
}

func withSecretOf(owner, secretSource issued) string {
	return credential.Prefix + string(owner.grant.ID) + secretSource.raw[len(credential.Prefix)+credential.IDLength:]
}

func TestAuthenticateTable(t *testing.T) {
	shop := issue(t, 1, tenant)
	stranger := issue(t, 2, tenant)
	bank := issue(t, 3, otherTenant)
	grants := grantsOf(shop, bank)
	type args struct {
		token string
		age   time.Duration
	}
	type want struct {
		Rejection ingestauth.RejectionError
		Stats     ingestauth.Stats
	}
	rejected := func(reason ingestauth.Reason, id credential.ID, age time.Duration) want {
		stats := ingestauth.Stats{Age: age}
		stats.Rejected[reason] = 1
		return want{Rejection: ingestauth.RejectionError{Reason: reason, TokenID: id}, Stats: stats}
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr error
	}{
		{
			name: "a granted token of the tenant is accepted",
			args: args{token: shop.raw, age: time.Second},
			want: want{Stats: ingestauth.Stats{Accepted: 1, Age: time.Second}},
		},
		{
			name:    "garbage is malformed and names no id",
			args:    args{token: "Bearer nonsense", age: time.Second},
			want:    rejected(ingestauth.ReasonMalformed, "", time.Second),
			wantErr: ingestauth.ErrUnauthenticated,
		},
		{
			name:    "an empty token is malformed",
			args:    args{token: "", age: time.Second},
			want:    rejected(ingestauth.ReasonMalformed, "", time.Second),
			wantErr: ingestauth.ErrUnauthenticated,
		},
		{
			name:    "a token without a grant is unknown",
			args:    args{token: stranger.raw, age: time.Second},
			want:    rejected(ingestauth.ReasonUnknown, stranger.grant.ID, time.Second),
			wantErr: ingestauth.ErrUnauthenticated,
		},
		{
			name:    "a known id with another secret is a mismatch",
			args:    args{token: withSecretOf(shop, stranger), age: time.Second},
			want:    rejected(ingestauth.ReasonMismatch, shop.grant.ID, time.Second),
			wantErr: ingestauth.ErrUnauthenticated,
		},
		{
			name:    "a token of another tenant is refused",
			args:    args{token: bank.raw, age: time.Second},
			want:    rejected(ingestauth.ReasonTenant, bank.grant.ID, time.Second),
			wantErr: ingestauth.ErrUnauthenticated,
		},
		{
			name:    "a stale directory refuses even a granted token",
			args:    args{token: shop.raw, age: maxStaleness + time.Nanosecond},
			want:    want{Stats: ingestauth.Stats{StaleRejections: 1, Age: maxStaleness + time.Nanosecond}},
			wantErr: ingestauth.ErrDirectoryStale,
		},
		{
			name: "a directory exactly at the limit is fresh",
			args: args{token: shop.raw, age: maxStaleness},
			want: want{Stats: ingestauth.Stats{Accepted: 1, Age: maxStaleness}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authenticator := newAuthenticator(t, grants, tt.args.age)
			err := authenticator.Authenticate(t.Context(), tt.args.token)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			rejection, _ := errors.AsType[ingestauth.RejectionError](err)
			got := want{Rejection: rejection, Stats: authenticator.Stats(t.Context())}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUnknownIDAndWrongSecretLookTheSame(t *testing.T) {
	shop := issue(t, 1, tenant)
	stranger := issue(t, 2, tenant)
	authenticator := newAuthenticator(t, grantsOf(shop), time.Second)
	unknown := authenticator.Authenticate(t.Context(), stranger.raw)
	mismatch := authenticator.Authenticate(t.Context(), withSecretOf(shop, stranger))
	foreign := newAuthenticator(t, grantsOf(issue(t, 3, otherTenant)), time.Second).
		Authenticate(t.Context(), credential.Reveal(issue(t, 3, otherTenant).token))
	got := []string{unknown.Error(), mismatch.Error(), foreign.Error()}
	rejected := ingestauth.ErrUnauthenticated.Error()
	want := []string{rejected, rejected, rejected}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("rejection texts differ (-want +got):\n%s", diff)
	}
	for _, err := range []error{unknown, mismatch, foreign} {
		if errors.Is(err, ingestauth.ErrDirectoryStale) || !errors.Is(err, ingestauth.ErrUnauthenticated) {
			t.Errorf("err = %v, want only ErrUnauthenticated", err)
		}
	}
}

func TestAStaleDirectoryRejectsEveryoneAsUnavailable(t *testing.T) {
	shop := issue(t, 1, tenant)
	stranger := issue(t, 2, tenant)
	tests := []struct {
		name      string
		checkedAt time.Time
		age       time.Duration
	}{
		{name: "never checked", checkedAt: time.Time{}, age: 0},
		{name: "past the staleness limit", checkedAt: checkedAt, age: maxStaleness + time.Millisecond},
		{name: "an hour old", checkedAt: checkedAt, age: time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			directory := &fakeDirectory{snapshot: ingestauth.Snapshot{Grants: grantsOf(shop), CheckedAt: tt.checkedAt}}
			clock := &fakeClock{now: checkedAt.Add(tt.age)}
			authenticator, err := ingestauth.NewAuthenticator(directory, clock, policy())
			if err != nil {
				t.Fatalf("NewAuthenticator: %v", err)
			}
			for _, token := range []string{shop.raw, stranger.raw, "garbage"} {
				if err := authenticator.Authenticate(t.Context(), token); !errors.Is(err, ingestauth.ErrDirectoryStale) {
					t.Errorf("Authenticate = %v, want ErrDirectoryStale", err)
				}
			}
			if authenticator.Fresh(t.Context()) {
				t.Error("Fresh = true for a stale directory")
			}
			stats := authenticator.Stats(t.Context())
			if diff := cmp.Diff(ingestauth.Stats{StaleRejections: 3, Age: stats.Age}, stats); diff != "" {
				t.Errorf("stats mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestANegativeAgeIsStale(t *testing.T) {
	shop := issue(t, 1, tenant)
	authenticator := newAuthenticator(t, grantsOf(shop), -time.Nanosecond)
	if err := authenticator.Authenticate(t.Context(), shop.raw); !errors.Is(err, ingestauth.ErrDirectoryStale) {
		t.Fatalf("Authenticate = %v, want ErrDirectoryStale", err)
	}
	if authenticator.Fresh(t.Context()) {
		t.Error("Fresh = true for a check from the future")
	}
}

func TestFreshFollowsTheDirectory(t *testing.T) {
	shop := issue(t, 1, tenant)
	directory := &fakeDirectory{snapshot: ingestauth.Snapshot{Grants: grantsOf(shop), CheckedAt: checkedAt}}
	clock := &fakeClock{now: checkedAt.Add(maxStaleness)}
	authenticator, err := ingestauth.NewAuthenticator(directory, clock, policy())
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	got := []bool{authenticator.Fresh(t.Context())}
	clock.now = clock.now.Add(time.Nanosecond)
	got = append(got, authenticator.Fresh(t.Context()))
	directory.snapshot.CheckedAt = clock.now
	got = append(got, authenticator.Fresh(t.Context()))
	if diff := cmp.Diff([]bool{true, false, true}, got); diff != "" {
		t.Errorf("Fresh mismatch (-want +got):\n%s", diff)
	}
}

func TestPolicyMustCoverTheCheckHorizon(t *testing.T) {
	tests := []struct {
		name    string
		args    ingestauth.Policy
		wantErr error
	}{
		{name: "the defaults", args: policy()},
		{
			name: "horizon equal to staleness",
			args: ingestauth.Policy{Tenant: tenant, MaxStaleness: time.Minute, CheckHorizon: time.Minute},
		},
		{
			name:    "no tenant",
			args:    ingestauth.Policy{MaxStaleness: maxStaleness, CheckHorizon: checkHorizon},
			wantErr: ingestauth.ErrInvalidPolicy,
		},
		{
			name:    "zero horizon",
			args:    ingestauth.Policy{Tenant: tenant, MaxStaleness: maxStaleness},
			wantErr: ingestauth.ErrInvalidPolicy,
		},
		{
			name:    "negative horizon",
			args:    ingestauth.Policy{Tenant: tenant, MaxStaleness: maxStaleness, CheckHorizon: -time.Second},
			wantErr: ingestauth.ErrInvalidPolicy,
		},
		{
			name:    "horizon past staleness",
			args:    ingestauth.Policy{Tenant: tenant, MaxStaleness: checkHorizon, CheckHorizon: maxStaleness},
			wantErr: ingestauth.ErrInvalidPolicy,
		},
		{
			name:    "staleness past the revocation limit",
			args:    ingestauth.Policy{Tenant: tenant, MaxStaleness: time.Minute + time.Nanosecond, CheckHorizon: checkHorizon},
			wantErr: ingestauth.ErrInvalidPolicy,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authenticator, err := ingestauth.NewAuthenticator(&fakeDirectory{}, &fakeClock{}, tt.args)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if (authenticator == nil) != (tt.wantErr != nil) {
				t.Errorf("authenticator = %v with err %v", authenticator, err)
			}
		})
	}
}

func TestNewAuthenticatorRefusesAMissingDependency(t *testing.T) {
	_, noDirectory := ingestauth.NewAuthenticator(nil, &fakeClock{}, policy())
	_, noClock := ingestauth.NewAuthenticator(&fakeDirectory{}, nil, policy())
	for _, err := range []error{noDirectory, noClock} {
		if !errors.Is(err, ingestauth.ErrMissingDependency) {
			t.Errorf("err = %v, want ErrMissingDependency", err)
		}
	}
}

func TestReasonNames(t *testing.T) {
	got := []string{}
	for reason := range ingestauth.Reason(ingestauth.ReasonCount + 1) {
		got = append(got, reason.String())
	}
	want := []string{"malformed", "unknown", "mismatch", "tenant", "malformed"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func FuzzAuthenticate(f *testing.F) {
	seed := bytes.Repeat([]byte{7}, tokenBytes)
	token, grant, err := credential.Generate(bytes.NewReader(seed), tenant, checkedAt)
	if err != nil {
		f.Fatalf("Generate: %v", err)
	}
	raw := credential.Reveal(token)
	f.Add(raw)
	f.Add(raw[:len(raw)-1] + "A")
	f.Add("")
	snapshot := ingestauth.Snapshot{Grants: credential.Grants{grant.ID: grant}, CheckedAt: checkedAt}
	directory := &fakeDirectory{snapshot: snapshot}
	authenticator, err := ingestauth.NewAuthenticator(directory, &fakeClock{now: checkedAt}, policy())
	if err != nil {
		f.Fatalf("NewAuthenticator: %v", err)
	}
	f.Fuzz(func(t *testing.T, candidate string) {
		err := authenticator.Authenticate(t.Context(), candidate)
		if err == nil {
			if candidate != raw {
				t.Fatalf("accepted %q", candidate)
			}
			return
		}
		if _, ok := errors.AsType[ingestauth.RejectionError](err); !ok || err.Error() != ingestauth.ErrUnauthenticated.Error() {
			t.Fatalf("Authenticate(%q) = %v", candidate, err)
		}
	})
}
