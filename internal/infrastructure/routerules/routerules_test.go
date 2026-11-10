package routerules_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/routerules"
)

const valid = `version: 1
services:
  - namespace: shop
    name: catalog
    routes:
      - /products/{sku}/reviews
      - /v1/{tenant}/items/{id}
`

func TestParseBuildsRulesThatTheNormalizerApplies(t *testing.T) {
	rules, err := routerules.Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	catalog := resource.ServiceRef{Namespace: "shop", Name: "catalog"}

	got := operation.NewNormalizer(rules).Normalize(catalog, &operation.Facts{HTTPMethod: "GET",
		Path: "/products/ABC/reviews"})

	want := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /products/{sku}/reviews"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("operation mismatch (-want +got):\n%s", diff)
	}
}

func TestParseRejectsInvalidFiles(t *testing.T) {
	twoServices := "version: 1\nservices:\n  - name: a\n    routes: [/a]\n  - name: b\n    routes:\n      - /ok\n" +
		"      - /b//c\n"
	type want struct {
		Line       int
		RuleReason string
	}
	tests := []struct {
		name    string
		args    string
		want    want
		wantErr error
	}{
		{name: "route with an empty segment", args: valid + "      - /a//b\n",
			want: want{Line: 8, RuleReason: "has an empty segment"}, wantErr: operation.ErrInvalidRules},
		{name: "route in the second service", args: twoServices,
			want: want{Line: 8, RuleReason: "has an empty segment"}, wantErr: operation.ErrInvalidRules},
		{name: "service without a name", args: "version: 1\nservices:\n  - namespace: shop\n    routes: [/a]\n",
			want: want{Line: 3, RuleReason: "service name is missing"}, wantErr: operation.ErrInvalidRules},
		{name: "service repeated through an alias",
			args: "version: 1\nservices:\n  - &a {name: a, routes: [/x]}\n  - *a\n",
			want: want{Line: 4, RuleReason: "service is listed twice"}, wantErr: operation.ErrInvalidRules},
		{name: "unknown field", args: "version: 1\nservices:\n  - name: a\n    paths: [/x]\n",
			wantErr: operation.ErrInvalidRules},
		{name: "unsupported version", args: "services: []\nversion: 2\n", want: want{Line: 2},
			wantErr: operation.ErrInvalidRules},
		{name: "two documents", args: "version: 1\n---\nversion: 1\n", wantErr: operation.ErrInvalidRules},
		{name: "empty file", args: "", wantErr: operation.ErrInvalidRules},
		{name: "file over 1 MiB", args: "version: 1\n#" + strings.Repeat("x", routerules.MaxFileBytes),
			wantErr: operation.ErrInvalidRules},
		{name: "wrong type", args: "version: one\n", wantErr: operation.ErrInvalidRules},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := routerules.Parse([]byte(tt.args))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Parse() = %v, want %v", err, tt.wantErr)
			}
			var got want
			if lineErr, ok := errors.AsType[*routerules.LineError](err); ok {
				got.Line = lineErr.Line
			}
			if ruleErr, ok := errors.AsType[*operation.RuleError](err); ok {
				got.RuleReason = ruleErr.Reason
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("error details mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(valid))
	f.Add([]byte("version: 1\nservices:\n  - name: a\n    routes:\n      - /a/{b}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		rules, err := routerules.Parse(data)
		if err != nil && !errors.Is(err, operation.ErrInvalidRules) {
			t.Fatalf("Parse() = %v, want ErrInvalidRules", err)
		}
		if err == nil && rules == nil {
			t.Fatal("Parse() returned neither rules nor an error")
		}
	})
}

func TestLineErrorWithoutALineKeepsTheCause(t *testing.T) {
	cause := &operation.RuleError{Service: 0, Route: -1, Reason: "service name is missing"}

	got := (&routerules.LineError{Err: cause}).Error()

	if diff := cmp.Diff(cause.Error(), got); diff != "" {
		t.Errorf("message of an error with an unknown line (-want +got):\n%s", diff)
	}
}
