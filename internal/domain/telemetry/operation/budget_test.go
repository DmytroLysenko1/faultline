package operation_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func combinationTemplates(prefix int) []string {
	templates := make([]string, 0, 1<<prefix+1)
	for combination := range 1 << prefix {
		var template strings.Builder
		for bit := range prefix {
			template.WriteString(map[bool]string{true: "/{p}", false: "/x"}[combination&(1<<bit) != 0])
		}
		templates = append(templates, template.String()+"/q")
	}
	return templates
}

func combinationRules(t testing.TB, prefix int) (*operation.Rules, string) {
	t.Helper()
	target := strings.Repeat("/{p}", prefix) + strings.Repeat("/x", operation.MaxPathSegments-prefix-1) + "/y"
	rules, err := operation.NewRules([]operation.ServiceRoutes{{Service: catalog,
		Routes: append(combinationTemplates(prefix), target)}})
	if err != nil {
		t.Fatal(err)
	}
	return rules, target
}

func TestRulesWithATooLongWorstWalkAreRejected(t *testing.T) {
	path := strings.Repeat("/x", operation.MaxPathSegments-1) + "/y"
	rules, target := combinationRules(t, 6)

	got := operation.NewNormalizer(rules).Normalize(catalog, &operation.Facts{HTTPMethod: "GET", Path: path})

	if diff := cmp.Diff(httpOp("GET "+target), got); diff != "" {
		t.Errorf("a rule behind 2⁶ ambiguous branches (-want +got):\n%s", diff)
	}
	tests := []struct {
		name string
		args []string
		want *operation.RuleError
	}{
		{name: "2⁷ ambiguous branches", args: combinationTemplates(7),
			want: &operation.RuleError{Service: 0, Route: -1,
				Reason: "rules too ambiguous: a path can visit 383 nodes, at most 256"}},
		{name: "ambiguous branches under an id-shaped literal", args: append(
			prefixed("/1234", combinationTemplates(7)[1:]), "/{a}"+strings.Repeat("/x", operation.MaxPathSegments-1)),
			want: &operation.RuleError{Service: 0, Route: -1,
				Reason: "rules too ambiguous: a path can visit 414 nodes, at most 256"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := operation.NewRules([]operation.ServiceRoutes{{Service: catalog, Routes: tt.args}})
			got, _ := errors.AsType[*operation.RuleError](err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("rule error mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func prefixed(prefix string, templates []string) []string {
	result := make([]string, len(templates))
	for i, template := range templates {
		result[i] = prefix + template
	}
	return result
}
