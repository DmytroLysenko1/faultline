package operation_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

type attribute struct {
	key, value string
}

func factsOf(attributes ...attribute) operation.Facts {
	var facts operation.Facts
	for _, a := range attributes {
		facts.Set(a.key, a.value)
	}
	return facts
}

func TestFactsSetReadsBothSemanticConventionForms(t *testing.T) {
	tests := []struct {
		Name string
		Args []attribute
		Want operation.Facts
	}{
		{Name: "current HTTP keys", Args: []attribute{{"http.request.method", "GET"}, {"http.route", "/a/{b}"},
			{"url.path", "/a/1"}, {"http.target", "/a/1?x"}, {"url.full", "http://h/a/1"}},
			Want: operation.Facts{HTTPMethod: "GET", Route: "/a/{b}", Path: "/a/1", Target: "/a/1?x", URL: "http://h/a/1"}},
		{Name: "deprecated HTTP keys", Args: []attribute{{"http.method", "POST"}, {"http.url", "http://h/x"}},
			Want: operation.Facts{HTTPMethod: "POST", URL: "http://h/x"}},
		{Name: "current RPC keys", Args: []attribute{{"rpc.system.name", "grpc"}, {"rpc.service", "s"}, {"rpc.method", "M"}},
			Want: operation.Facts{RPCSystem: "grpc", RPCService: "s", RPCMethod: "M"}},
		{Name: "deprecated rpc.system", Args: []attribute{{"rpc.system", "grpc"}}, Want: operation.Facts{RPCSystem: "grpc"}},
		{Name: "current key wins when it comes first", Args: []attribute{{"http.request.method", "GET"},
			{"http.method", "POST"}}, Want: operation.Facts{HTTPMethod: "GET"}},
		{Name: "current key wins when it comes last", Args: []attribute{{"http.url", "http://old/x"},
			{"url.full", "http://new/x"}, {"rpc.system", "dubbo"}, {"rpc.system.name", "grpc"}},
			Want: operation.Facts{URL: "http://new/x", RPCSystem: "grpc"}},
		{Name: "empty current key after a deprecated one keeps it", Args: []attribute{{"http.method", "GET"},
			{"http.request.method", ""}}, Want: operation.Facts{HTTPMethod: "GET"}},
		{Name: "empty current key before a deprecated one keeps it", Args: []attribute{{"http.request.method", ""},
			{"http.method", "GET"}}, Want: operation.Facts{HTTPMethod: "GET"}},
		{Name: "unrelated keys are ignored", Args: []attribute{{"user_agent.original", "curl"}}, Want: operation.Facts{}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, factsOf(tt.Args...)); diff != "" {
				t.Errorf("facts mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFactsSetReportsWhetherTheKeyIsAFact(t *testing.T) {
	var facts operation.Facts
	got := []bool{facts.Set("http.route", "/a"), facts.Set("http.method", "GET"), facts.Set("db.system", "pg")}
	if diff := cmp.Diff([]bool{true, true, false}, got); diff != "" {
		t.Errorf("[http.route, http.method, db.system] accepted mismatch (-want +got):\n%s", diff)
	}
}
