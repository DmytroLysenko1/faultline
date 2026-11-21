package credential_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

var errTransient = errors.New("entropy source busy")

type generatedPair struct {
	Token credential.IngestToken
	Grant credential.Grant
}

func TestGenerateUsesTheInjectedRandomness(t *testing.T) {
	secret := markerSecret()
	wrapping := []byte{0, 25, 26, 31, 32, 57, 58, 63, 224, 230, 250, 255}
	type args struct {
		random io.Reader
		tenant string
	}
	tests := []struct {
		name    string
		args    args
		want    generatedPair
		wantErr error
	}{
		{
			name: "sequential symbols",
			args: args{random: randomness(sequentialID(), secret), tenant: testTenant},
			want: pairOf(t, "abcdefghijkl", secret, testTenant),
		},
		{
			name: "every byte keeps its low five bits",
			args: args{random: randomness(wrapping, secret), tenant: testTenant},
			want: pairOf(t, "az27az27ag27", secret, testTenant),
		},
		{
			name:    "no id bytes",
			args:    args{random: bytes.NewReader(nil), tenant: testTenant},
			wantErr: credential.ErrNoRandomness,
		},
		{
			name:    "short id",
			args:    args{random: bytes.NewReader(sequentialID()[1:]), tenant: testTenant},
			wantErr: credential.ErrNoRandomness,
		},
		{
			name:    "short secret",
			args:    args{random: bytes.NewReader(append(sequentialID(), secret[1:]...)), tenant: testTenant},
			wantErr: credential.ErrNoRandomness,
		},
		{name: "no tenant", args: args{random: randomness(sequentialID(), secret)}, wantErr: credential.ErrInvalidGrant},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, grant, err := credential.Generate(tt.args.random, tt.args.tenant, issuedAt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, generatedPair{Token: token, Grant: grant}, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGenerateKeepsTheReaderFailure(t *testing.T) {
	_, _, err := credential.Generate(bytes.NewReader(nil), testTenant, issuedAt)

	if !errors.Is(err, io.EOF) {
		t.Errorf("err = %v, want it to carry io.EOF", err)
	}
}

type flakyReader struct {
	failed bool
	rest   io.Reader
}

func (r *flakyReader) Read(buffer []byte) (int, error) {
	if !r.failed {
		r.failed = true
		return 0, errTransient
	}
	return r.rest.Read(buffer)
}

func TestGenerateRefusesAnIDItCouldNotRead(t *testing.T) {
	reader := &flakyReader{rest: randomness(sequentialID(), markerSecret())}

	token, _, err := credential.Generate(reader, testTenant, issuedAt)

	if !errors.Is(err, credential.ErrNoRandomness) || !errors.Is(err, errTransient) {
		t.Errorf("err = %v, want ErrNoRandomness carrying %v", err, errTransient)
	}
	if token.ID != "" {
		t.Errorf("Generate returned id %q after a failed read", token.ID)
	}
}

func pairOf(t *testing.T, id credential.ID, secret []byte, tenant string) generatedPair {
	t.Helper()
	return generatedPair{
		Token: parsed(t, id, secret),
		Grant: credential.Grant{ID: id, Tenant: tenant, Digest: digestOf(t, secret), IssuedAt: issuedAt},
	}
}
