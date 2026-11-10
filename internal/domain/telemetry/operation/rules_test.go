package operation_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

func routes(count int) []string {
	list := make([]string, count)
	for i := range list {
		list[i] = fmt.Sprintf("/r%d", i)
	}
	return list
}

func TestNewRulesRejectsInvalidRules(t *testing.T) {
	cart := resource.ServiceRef{Name: "cart"}
	single := func(route string) []operation.ServiceRoutes {
		return []operation.ServiceRoutes{{Service: catalog, Routes: []string{"/ok", route}}}
	}
	fullServices := make([]operation.ServiceRoutes, 0, operation.MaxRoutes/operation.MaxRoutesPerService)
	for i := range cap(fullServices) {
		fullServices = append(fullServices, operation.ServiceRoutes{
			Service: resource.ServiceRef{Name: fmt.Sprintf("s%d", i)}, Routes: routes(operation.MaxRoutesPerService),
		})
	}
	oneMore := append(slices.Clone(fullServices), operation.ServiceRoutes{
		Service: resource.ServiceRef{Name: "extra"}, Routes: routes(1),
	})
	tests := []struct {
		name string
		args []operation.ServiceRoutes
		want *operation.RuleError
	}{
		{name: "valid rules with the root", args: single("/")},
		{name: "missing service name", args: []operation.ServiceRoutes{{Routes: []string{"/a"}}},
			want: &operation.RuleError{Service: 0, Route: -1, Reason: "service name is missing"}},
		{name: "service listed twice", args: []operation.ServiceRoutes{{Service: cart}, {Service: cart}},
			want: &operation.RuleError{Service: 1, Route: -1, Reason: "service is listed twice"}},
		{name: "too many routes for a service",
			args: []operation.ServiceRoutes{{Service: cart, Routes: routes(operation.MaxRoutesPerService + 1)}},
			want: &operation.RuleError{Service: 0, Route: -1, Reason: "more than 256 routes"}},
		{name: "exactly the total limit", args: fullServices},
		{name: "32 segments", args: single(strings.Repeat("/a", operation.MaxPathSegments))},
		{name: "query character", args: single("/a/b?c"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: `segment "b?c" holds a query or fragment character`}},
		{name: "one route over the total limit", args: oneMore,
			want: &operation.RuleError{Service: 16, Route: -1, Reason: "more than 4096 routes"}},
		{name: "no leading slash", args: single("a/b"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: "does not start with /"}},
		{name: "empty segment", args: single("/a//b"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: "has an empty segment"}},
		{name: "trailing slash", args: single("/a/"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: "has an empty segment"}},
		{name: "broken parameter", args: single("/a/{b-c}"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: `segment "{b-c}" is neither a literal nor {name}`}},
		{name: "query character", args: single("/a/b?c"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: `segment "b?c" holds a query or fragment character`}},
		{name: "control character", args: single("/a\x01"),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: "is not printable UTF-8"}},
		{name: "too long for the longest method prefix", args: single("/" + strings.Repeat("a", operation.MaxTemplateBytes)),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: "longer than 249 bytes"}},
		{name: "longest template", args: single("/" + strings.Repeat("a", operation.MaxTemplateBytes-1))},
		{name: "too many segments", args: single(strings.Repeat("/a", operation.MaxPathSegments+1)),
			want: &operation.RuleError{Service: 0, Route: 1, Reason: "more than 32 segments"}},
		{name: "same shape with other parameter names", args: []operation.ServiceRoutes{{Service: cart,
			Routes: []string{"/a/{x}", "/a/{y}"}}},
			want: &operation.RuleError{Service: 0, Route: 1, Reason: `duplicates "/a/{x}"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := operation.NewRules(tt.args)
			got, _ := errors.AsType[*operation.RuleError](err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("rule error mismatch (-want +got):\n%s", diff)
			}
			if tt.want != nil && !errors.Is(err, operation.ErrInvalidRules) {
				t.Fatalf("NewRules() = %v, want it to wrap ErrInvalidRules", err)
			}
		})
	}
}

func TestOperationValidate(t *testing.T) {
	tests := []struct {
		name    string
		args    operation.Operation
		wantErr error
	}{
		{name: "unknown operation", args: operation.Operation{}},
		{name: "HTTP operation", args: httpOp("GET /users/{id}")},
		{name: "longest name", args: httpOp(strings.Repeat("a", operation.MaxNameBytes))},
		{name: "unknown protocol", args: operation.Operation{Protocol: 9, Name: "x"},
			wantErr: operation.ErrInvalidOperation},
		{name: "name without a protocol", args: operation.Operation{Name: "x"}, wantErr: operation.ErrInvalidOperation},
		{name: "protocol without a name", args: httpOp(""), wantErr: operation.ErrInvalidOperation},
		{name: "too long", args: httpOp(strings.Repeat("a", operation.MaxNameBytes+1)),
			wantErr: operation.ErrInvalidOperation},
		{name: "invalid UTF-8", args: httpOp("\xff"), wantErr: operation.ErrInvalidOperation},
		{name: "reserved overflow name", args: httpOp(operation.OverflowName), wantErr: operation.ErrInvalidOperation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.args.Validate(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
