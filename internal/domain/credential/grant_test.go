package credential_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

func otherSecret() []byte {
	return []byte(strings.Repeat("o", credential.SecretBytes))
}

func TestGrantsAdmitTable(t *testing.T) {
	token, grant := generated(t, sequentialID(), markerSecret(), testTenant)
	stranger, _ := generated(t, []byte("zzzzzzzzzzzz"), markerSecret(), testTenant)
	guess := parsed(t, token.ID, otherSecret())
	foreign, foreignGrant := generated(t, []byte("mmmmmmmmmmmm"), otherSecret(), otherTenant)
	foreignGuess := parsed(t, foreign.ID, markerSecret())
	grants := credential.Grants{grant.ID: grant, foreignGrant.ID: foreignGrant}
	zeroSecret := credential.IngestToken{ID: token.ID}
	misfiled := credential.Grants{stranger.ID: grant}
	type args struct {
		grants credential.Grants
		token  credential.IngestToken
	}
	tests := []struct {
		name    string
		args    args
		want    credential.Grant
		wantErr error
	}{
		{name: "own token", args: args{grants: grants, token: token}, want: grant},
		{name: "unknown id, known secret", args: args{grants: grants, token: stranger}, wantErr: credential.ErrUnknownGrant},
		{name: "empty directory", args: args{grants: credential.Grants{}, token: token}, wantErr: credential.ErrUnknownGrant},
		{name: "nil directory", args: args{token: token}, wantErr: credential.ErrUnknownGrant},
		{name: "filed under another id", args: args{grants: misfiled, token: stranger}, wantErr: credential.ErrUnknownGrant},
		{name: "wrong secret", args: args{grants: grants, token: guess}, wantErr: credential.ErrDigestMismatch},
		{name: "another tenant", args: args{grants: grants, token: foreign}, wantErr: credential.ErrForeignTenant},
		{name: "foreign wrong secret", args: args{grants: grants, token: foreignGuess}, wantErr: credential.ErrDigestMismatch},
		{name: "zero secret", args: args{grants: grants, token: zeroSecret}, wantErr: credential.ErrDigestMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.args.grants.Admit(tt.args.token, testTenant)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAGuessLearnsNeitherTenantNorGrant(t *testing.T) {
	token, grant := generated(t, sequentialID(), markerSecret(), testTenant)
	grants := credential.Grants{grant.ID: grant}
	unknown := credential.IngestToken{ID: "zzzzzzzzzzzz", Secret: token.Secret}
	wrong := parsed(t, token.ID, otherSecret())

	for _, guess := range []credential.IngestToken{unknown, wrong} {
		got, err := grants.Admit(guess, otherTenant)
		if err == nil {
			t.Fatalf("Admit(%v) admitted a guess", guess)
		}
		if diff := cmp.Diff(credential.Grant{}, got, byText()); diff != "" {
			t.Errorf("Admit(%v) handed out grant data (-want +got):\n%s", guess, diff)
		}
		if errors.Is(err, credential.ErrForeignTenant) {
			t.Errorf("Admit(%v) = %v: a guess learned the tenant before the secret matched", guess, err)
		}
		for _, leak := range []string{string(grant.ID), grant.Tenant} {
			if strings.Contains(err.Error(), leak) {
				t.Errorf("Admit(%v) error %q names %q", guess, err, leak)
			}
		}
	}
}

func TestNewGrantRefusesAnInvalidGrant(t *testing.T) {
	_, valid := generated(t, sequentialID(), markerSecret(), testTenant)
	invalid := credential.ErrInvalidGrant
	type args struct {
		id     credential.ID
		tenant string
		digest credential.Digest
	}
	tests := []struct {
		name    string
		args    args
		want    credential.Grant
		wantErr error
	}{
		{name: "valid", args: args{id: valid.ID, tenant: testTenant, digest: valid.Digest}, want: valid},
		{name: "malformed id", args: args{id: "ABCDEFGHIJKL", tenant: testTenant, digest: valid.Digest}, wantErr: invalid},
		{name: "empty id", args: args{tenant: testTenant, digest: valid.Digest}, wantErr: invalid},
		{name: "no tenant", args: args{id: valid.ID, digest: valid.Digest}, wantErr: invalid},
		{name: "zero digest", args: args{id: valid.ID, tenant: testTenant}, wantErr: invalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := credential.NewGrant(tt.args.id, tt.args.tenant, tt.args.digest, issuedAt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAnEmptySecretMatchesNoDigest(t *testing.T) {
	token, grant := generated(t, sequentialID(), markerSecret(), testTenant)
	withoutDigest := credential.Grants{token.ID: {ID: token.ID, Tenant: testTenant}}
	type args struct {
		grants credential.Grants
		token  credential.IngestToken
	}
	tests := []struct {
		name    string
		args    args
		want    credential.Grant
		wantErr error
	}{
		{name: "grant without a digest", args: args{grants: withoutDigest, token: token}, wantErr: credential.ErrUnknownGrant},
		{
			name:    "token without a secret, grant without a digest",
			args:    args{grants: withoutDigest, token: credential.IngestToken{ID: token.ID}},
			wantErr: credential.ErrUnknownGrant,
		},
		{
			name:    "token without a secret",
			args:    args{grants: credential.Grants{grant.ID: grant}, token: credential.IngestToken{ID: token.ID}},
			wantErr: credential.ErrDigestMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.args.grants.Admit(tt.args.token, testTenant)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
	empty, other := credential.IngestToken{}.Digest(), credential.IngestToken{ID: token.ID}.Digest()
	if empty.Equal(other) || empty.Equal(grant.Digest) {
		t.Errorf("the digest of an empty secret matches a digest")
	}
}

func TestParseDigestAcceptsOnlyAFullNonZeroDigest(t *testing.T) {
	_, grant := generated(t, sequentialID(), markerSecret(), testTenant)
	text := digestText(grant.Digest)
	type args struct {
		text string
	}
	tests := []struct {
		name    string
		args    args
		want    credential.Digest
		wantErr error
	}{
		{name: "the stored form", args: args{text: text}, want: grant.Digest},
		{name: "not hex", args: args{text: strings.Repeat("z", len(text))}, wantErr: credential.ErrInvalidGrant},
		{name: "short", args: args{text: text[2:]}, wantErr: credential.ErrInvalidGrant},
		{name: "zero", args: args{text: strings.Repeat("0", len(text))}, wantErr: credential.ErrInvalidGrant},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := credential.ParseDigest(tt.args.text)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), tt.args.text) {
				t.Errorf("err %q repeats the rejected input", err)
			}
			if diff := cmp.Diff(tt.want, got, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDigestEqualComparesEveryByte(t *testing.T) {
	_, grant := generated(t, sequentialID(), markerSecret(), testTenant)
	text := digestText(grant.Digest)
	lastSymbol := "0"
	if strings.HasSuffix(text, lastSymbol) {
		lastSymbol = "1"
	}
	almost, err := credential.ParseDigest(text[:len(text)-1] + lastSymbol)
	if err != nil {
		t.Fatalf("ParseDigest: %v", err)
	}
	same, err := credential.ParseDigest(text)
	if err != nil {
		t.Fatalf("ParseDigest: %v", err)
	}

	if grant.Digest.Equal(almost) || !grant.Digest.Equal(same) {
		t.Errorf("Equal(last symbol differs) = %v, Equal(same text) = %v; want false and true",
			grant.Digest.Equal(almost), grant.Digest.Equal(same))
	}
}
