package bearer_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/bearer"
)

type args struct {
	values []string
}

var token = credential.Prefix + "abcdefghijkl_" + strings.Repeat("A", credential.SecretLength)

func header(values ...string) args {
	return args{values: values}
}

func TestCredentialTable(t *testing.T) {
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{name: "the exact header gives the token", args: header("Bearer " + token), want: token},
		{name: "the scheme ignores case", args: header("bEARER " + token), want: token},
		{name: "no value is missing", args: header(), wantErr: bearer.ErrMissing},
		{name: "an empty list is missing", args: args{values: []string{}}, wantErr: bearer.ErrMissing},
		{name: "two values are malformed", args: header("Bearer "+token, "Bearer "+token), wantErr: bearer.ErrHeader},
		{name: "an empty value is malformed", args: header(""), wantErr: bearer.ErrHeader},
		{name: "another scheme is malformed", args: header("Basic  " + token), wantErr: bearer.ErrHeader},
		{name: "a second space is left for Parse", args: header("Bearer  " + token[1:]), want: " " + token[1:]},
		{name: "a tab instead of a space is malformed", args: header("Bearer\t" + token), wantErr: bearer.ErrHeader},
		{name: "a trailing space is malformed", args: header("Bearer " + token + " "), wantErr: bearer.ErrHeader},
		{name: "a short token is malformed", args: header("Bearer " + token[1:]), wantErr: bearer.ErrHeader},
		{name: "a bare token is malformed", args: header(token), wantErr: bearer.ErrHeader},
		{name: "a huge value is malformed", args: header("Bearer " + strings.Repeat("x", 1<<20)), wantErr: bearer.ErrHeader},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bearer.Credential(tt.args.values)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func FuzzBearerCredential(f *testing.F) {
	f.Add("Bearer "+token, "")
	f.Add("bearer "+token, "x")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, first, second string) {
		for _, values := range [][]string{{first}, {first, second}} {
			got, err := bearer.Credential(values)
			if err != nil {
				if got != "" || !errors.Is(err, bearer.ErrHeader) && !errors.Is(err, bearer.ErrMissing) {
					t.Fatalf("Credential(%q) = %q, %v", values, got, err)
				}
				continue
			}
			if len(values) != 1 || len(got) != credential.TokenLength || !strings.HasSuffix(values[0], got) {
				t.Fatalf("Credential(%q) = %q", values, got)
			}
			if !strings.EqualFold(values[0][:len(bearer.Scheme)], bearer.Scheme) {
				t.Fatalf("Credential(%q) accepted another scheme", values)
			}
		}
	})
}
