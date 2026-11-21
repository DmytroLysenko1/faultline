package credential_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	testTenant      = "shop"
	otherTenant     = "bank"
	zeroSecretText  = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	firstIDText     = "abcdefghijkl"
	validZeroToken  = credential.Prefix + firstIDText + "_" + zeroSecretText
	markerSecretRun = "S"
	idStart         = len(credential.Prefix)
	secretStart     = idStart + credential.IDLength + 1
	last            = len(validZeroToken) - 1
)

var issuedAt = time.Date(2026, time.November, 21, 12, 0, 0, 0, time.UTC)

func randomness(idSymbols, secret []byte) *bytes.Reader {
	return bytes.NewReader(append(append([]byte{}, idSymbols...), secret...))
}

func sequentialID() []byte {
	symbols := make([]byte, credential.IDLength)
	for index := range symbols {
		symbols[index] = byte(index)
	}
	return symbols
}

func markerSecret() []byte {
	return []byte(strings.Repeat(markerSecretRun, credential.SecretBytes))
}

func byText() cmp.Options {
	return cmp.Options{
		cmp.Comparer(func(left, right credential.Secret) bool {
			revealed := credential.Reveal(credential.IngestToken{Secret: left})
			return revealed == credential.Reveal(credential.IngestToken{Secret: right})
		}),
		cmp.Comparer(func(left, right credential.Digest) bool {
			return digestText(left) == digestText(right)
		}),
	}
}

func digestText(digest credential.Digest) string {
	text, err := digest.MarshalText()
	if err != nil {
		return err.Error()
	}
	return string(text)
}

func digestOf(t *testing.T, secret []byte) credential.Digest {
	t.Helper()
	sum := sha256.Sum256(secret)
	digest, err := credential.ParseDigest(hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatalf("ParseDigest: %v", err)
	}
	return digest
}

func parsed(t *testing.T, id credential.ID, secret []byte) credential.IngestToken {
	t.Helper()
	token, err := credential.Parse(credential.Prefix + string(id) + "_" + base64.RawURLEncoding.EncodeToString(secret))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return token
}

func generated(
	t *testing.T,
	idSymbols []byte,
	secret []byte,
	tenant string,
) (credential.IngestToken, credential.Grant) {
	t.Helper()
	token, grant, err := credential.Generate(randomness(idSymbols, secret), tenant, issuedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return token, grant
}

func validWith(index int, replacement string) string {
	return validZeroToken[:index] + replacement + validZeroToken[index+len(replacement):]
}

func TestParseAcceptsOnlyTheExactFormat(t *testing.T) {
	malformed := credential.ErrMalformed
	zeroToken := parsed(t, firstIDText, make([]byte, credential.SecretBytes))
	type args struct {
		raw string
	}
	tests := []struct {
		name    string
		args    args
		want    credential.IngestToken
		wantErr error
	}{
		{name: "canonical", args: args{raw: validZeroToken}, want: zeroToken},
		{name: "empty", args: args{raw: ""}, wantErr: malformed},
		{name: "one short", args: args{raw: validZeroToken[1:]}, wantErr: malformed},
		{name: "one long", args: args{raw: validZeroToken + "A"}, wantErr: malformed},
		{name: "foreign prefix", args: args{raw: validWith(0, "flu_")}, wantErr: malformed},
		{name: "upper prefix", args: args{raw: validWith(0, "FLT_")}, wantErr: malformed},
		{name: "dash separator", args: args{raw: validWith(secretStart-1, "-")}, wantErr: malformed},
		{name: "upper id", args: args{raw: validWith(idStart, "A")}, wantErr: malformed},
		{name: "digit 0 in id", args: args{raw: validWith(idStart, "0")}, wantErr: malformed},
		{name: "digit 8 in id", args: args{raw: validWith(idStart, "8")}, wantErr: malformed},
		{name: "underscore in id", args: args{raw: validWith(idStart, "_")}, wantErr: malformed},
		{name: "standard alphabet plus", args: args{raw: validWith(secretStart, "+")}, wantErr: malformed},
		{name: "standard alphabet slash", args: args{raw: validWith(secretStart, "/")}, wantErr: malformed},
		{name: "padding", args: args{raw: validWith(last, "=")}, wantErr: malformed},
		{name: "trailing bits", args: args{raw: validWith(last, "B")}, wantErr: malformed},
		{name: "newline in secret", args: args{raw: validWith(secretStart, "\n")}, wantErr: malformed},
		{name: "carriage return in secret", args: args{raw: validWith(secretStart, "\r")}, wantErr: malformed},
		{name: "space", args: args{raw: validWith(secretStart, " ")}, wantErr: malformed},
		{name: "non ascii id", args: args{raw: validWith(idStart, "é")}, wantErr: malformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := credential.Parse(tt.args.raw)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseReadsBackARevealedToken(t *testing.T) {
	token, _ := generated(t, sequentialID(), markerSecret(), testTenant)

	got, err := credential.Parse(credential.Reveal(token))
	if err != nil {
		t.Fatalf("Parse(Reveal()): %v", err)
	}

	if diff := cmp.Diff(token, got, byText()); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
	if len(credential.Reveal(token)) != credential.TokenLength {
		t.Errorf("revealed length %d, want %d", len(credential.Reveal(token)), credential.TokenLength)
	}
}

func TestParseIDAcceptsOnlyTwelveBase32Symbols(t *testing.T) {
	type args struct {
		raw string
	}
	tests := []struct {
		name    string
		args    args
		want    credential.ID
		wantErr error
	}{
		{name: "every letter and digit", args: args{raw: "az234567mnop"}, want: "az234567mnop"},
		{name: "short", args: args{raw: firstIDText[1:]}, wantErr: credential.ErrMalformed},
		{name: "long", args: args{raw: firstIDText + "a"}, wantErr: credential.ErrMalformed},
		{name: "digit 1", args: args{raw: "1bcdefghijkl"}, wantErr: credential.ErrMalformed},
		{name: "digit 9", args: args{raw: "abcdefghijk9"}, wantErr: credential.ErrMalformed},
		{name: "upper", args: args{raw: "abcdefGhijkl"}, wantErr: credential.ErrMalformed},
		{name: "whole token", args: args{raw: validZeroToken}, wantErr: credential.ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := credential.ParseID(tt.args.raw)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, byText()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func FuzzParseToken(f *testing.F) {
	f.Add(validZeroToken)
	f.Add(validWith(last, "B"))
	f.Add(validWith(idStart, "\n"))
	f.Add("")
	f.Fuzz(func(t *testing.T, raw string) {
		token, err := credential.Parse(raw)
		if err != nil {
			if !errors.Is(err, credential.ErrMalformed) {
				t.Fatalf("Parse(%q) = %v, want ErrMalformed", raw, err)
			}
			if token.ID != "" || credential.Reveal(token) != credential.Reveal(credential.IngestToken{}) {
				t.Fatalf("Parse(%q) failed but returned a token", raw)
			}
			return
		}
		if credential.Reveal(token) != raw {
			t.Fatalf("Parse(%q) accepted a non-canonical form, revealed %q", raw, credential.Reveal(token))
		}
	})
}

func BenchmarkParseAndDigest(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		token, err := credential.Parse(validZeroToken)
		if err != nil {
			b.Fatalf("Parse: %v", err)
		}
		token.Digest()
	}
}
