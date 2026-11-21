package tokens

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	testTenant   = "shop"
	tokenBytes   = credential.IDLength + credential.SecretBytes
	idSymbolBits = 5
	idSeedBytes  = 2
)

var issuedAt = time.Date(2026, time.November, 21, 12, 0, 0, 123, time.UTC)

func testGrant(t *testing.T, seed byte) credential.Grant {
	t.Helper()
	random := slices.Concat([]byte{seed, seed >> idSymbolBits}, bytes.Repeat([]byte{seed}, tokenBytes-idSeedBytes))
	_, grant, err := credential.Generate(bytes.NewReader(random), testTenant, issuedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return grant
}

func digestText(t *testing.T, grant *credential.Grant) string {
	t.Helper()
	text, err := grant.Digest.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	return string(text)
}

func TestAnEntryReadsBackAsItsGrant(t *testing.T) {
	grant := testGrant(t, 1)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	got, err := decodeEntry(grant.ID, value)
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if !got.Digest.Equal(grant.Digest) ||
		got.Tenant != grant.Tenant ||
		got.ID != grant.ID ||
		!got.IssuedAt.Equal(grant.IssuedAt) {
		t.Errorf("decoded %+v, want %+v", got, grant)
	}
	if len(value) > MaxValueBytes {
		t.Errorf("value of %d bytes exceeds %d", len(value), MaxValueBytes)
	}
}

func TestAnInvalidValueIsRefused(t *testing.T) {
	grant := testGrant(t, 1)
	digest := digestText(t, &grant)
	valid := `{"v":1,"tenant":"shop","digest":"` + digest + `","issued_at":"2026-11-21T12:00:00Z"}`
	tests := []struct {
		name    string
		args    string
		wantErr error
	}{
		{name: "the valid value", args: valid},
		{name: "an oversized value", args: valid + strings.Repeat(" ", MaxValueBytes), wantErr: errInvalidEntry},
		{name: "another version", args: strings.Replace(valid, `"v":1`, `"v":2`, 1), wantErr: errInvalidEntry},
		{name: "an unknown field", args: strings.Replace(valid, `{`, `{"admin":true,`, 1), wantErr: errInvalidEntry},
		{name: "a trailing document", args: valid + `{}`, wantErr: errInvalidEntry},
		{name: "no tenant", args: strings.Replace(valid, `"shop"`, `""`, 1), wantErr: errInvalidEntry},
		{name: "a short digest", args: strings.Replace(valid, digest, digest[1:], 1), wantErr: errInvalidEntry},
		{
			name:    "a zero digest",
			args:    strings.Replace(valid, digest, strings.Repeat("0", len(digest)), 1),
			wantErr: errInvalidEntry,
		},
		{name: "a bad time", args: strings.Replace(valid, "2026-11-21T12:00:00Z", "yesterday", 1), wantErr: errInvalidEntry},
		{name: "not json", args: "flt_", wantErr: errInvalidEntry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEntry(grant.ID, []byte(tt.args))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestTheSubjectNamesTheID(t *testing.T) {
	grant := testGrant(t, 1)
	tests := []struct {
		name    string
		args    string
		want    credential.ID
		wantErr error
	}{
		{name: "the key of a grant", args: keyOf(grant.ID), want: grant.ID},
		{name: "a nested subject", args: keyOf(grant.ID) + ".x", wantErr: errInvalidEntry},
		{name: "another bucket", args: "$KV.other." + string(grant.ID), wantErr: errInvalidEntry},
		{name: "an id outside the alphabet", args: subjectPrefix + "ABCDEFGHIJKL", wantErr: errInvalidEntry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := idOf(tt.args)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAKVDeleteOrPurgeMarkerIsNoGrant(t *testing.T) {
	for operation, want := range map[string]bool{operationDelete: true, operationPurge: true, "": false} {
		entry := rawEntry{Header: nats.Header{}}
		if operation != "" {
			entry.Header.Set(operationHeader, operation)
		}
		if got := removed(entry); got != want {
			t.Errorf("removed(%q) = %v, want %v", operation, got, want)
		}
	}
}

func TestStrictHeadersReadOnlyTheCanonicalForm(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    nats.Header
		wantErr error
	}{
		{name: "no fields", args: "NATS/1.0\r\n\r\n", want: nats.Header{}},
		{
			name: "two fields",
			args: "NATS/1.0\r\nNats-Msg-Id: abc\r\nKV-Operation: DEL\r\n\r\n",
			want: nats.Header{"Nats-Msg-Id": {"abc"}, "KV-Operation": {"DEL"}},
		},
		{name: "a repeated key keeps both", args: "NATS/1.0\r\nA: 1\r\nA: 2\r\n\r\n", want: nats.Header{"A": {"1", "2"}}},
		{name: "a bare line feed", args: "NATS/1.0\r\nA: 1\nB: 2\r\n\r\n", wantErr: errInvalidEntry},
		{name: "a bare carriage return", args: "NATS/1.0\r\nA: 1\rB: 2\r\n\r\n", wantErr: errInvalidEntry},
		{name: "a bare line feed after the status", args: "NATS/1.0\nA: 1\r\n\r\n", wantErr: errInvalidEntry},
		{name: "no terminating blank line", args: "NATS/1.0\r\nA: 1\r\n", wantErr: errInvalidEntry},
		{name: "no line ends at all", args: "NATS/1.0\r\nA: 1", wantErr: errInvalidEntry},
		{name: "a status code", args: "NATS/1.0 503\r\n\r\n", wantErr: errInvalidEntry},
		{name: "a blank line inside", args: "NATS/1.0\r\n\r\nA: 1\r\n\r\n", wantErr: errInvalidEntry},
		{name: "no colon", args: "NATS/1.0\r\nbogus\r\n\r\n", wantErr: errInvalidEntry},
		{name: "an empty key", args: "NATS/1.0\r\n: v\r\n\r\n", wantErr: errInvalidEntry},
		{name: "a space in the key", args: "NATS/1.0\r\nNats-Msg-Id : v\r\n\r\n", wantErr: errInvalidEntry},
		{name: "nothing", args: "", wantErr: errInvalidEntry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := strictHeaders([]byte(tt.args))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
