package operation_test

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func FuzzNormalize(f *testing.F) {
	f.Add("GET", "", "/users/123", "", "", "", "", "")
	f.Add("POST", "/a/{b}", "", "", "", "grpc", "", "")
	f.Add("", "", "", "", "", "", "", operation.OverflowName)
	f.Add("", "", "", "", "", "", "", "/")
	f.Add("GET", "", strings.Repeat("/я", 200), "", "", "", "", "")
	f.Add("GET", "", "/\xff\x00/x", "", "", "", "svc", "s/\x7f")
	f.Add("GET", "", "", "http://h/a?b=/c", "http://api#/x", "", "", "")
	normalizer := operation.NewNormalizer(nil)
	f.Fuzz(func(t *testing.T, method, route, path, target, url, system, service, rpcMethod string) {
		got := normalizer.Normalize(catalog, &operation.Facts{HTTPMethod: method, Route: route, Path: path,
			Target: target, URL: url, RPCSystem: system, RPCService: service, RPCMethod: rpcMethod})
		if err := got.Validate(); err != nil {
			t.Fatalf("Normalize() = %+v: %v", got, err)
		}
		if strings.ContainsFunc(got.Name, unicode.IsControl) {
			t.Fatalf("name %q holds a control character", got.Name)
		}
		if again := normalizeAgain(normalizer, got); again != got {
			t.Fatalf("N(N(x)) = %+v, N(x) = %+v", again, got)
		}
	})
}

func normalizeAgain(normalizer *operation.Normalizer, once operation.Operation) operation.Operation {
	switch once.Protocol {
	case operation.ProtocolHTTP:
		method, path, _ := strings.Cut(once.Name, " ")
		return normalizer.Normalize(catalog, &operation.Facts{HTTPMethod: method, Route: path})
	case operation.ProtocolGRPC:
		return normalizer.Normalize(catalog, &operation.Facts{RPCSystem: "grpc", RPCMethod: once.Name})
	case operation.ProtocolRPC:
		return normalizer.Normalize(catalog, &operation.Facts{RPCMethod: once.Name})
	}
	return once
}

func FuzzRouteRules(f *testing.F) {
	f.Add("/products/{sku}/reviews")
	f.Add("/")
	f.Add("/a//b")
	f.Add("/{x}/{y}")
	f.Fuzz(func(t *testing.T, route string) {
		rules, err := operation.NewRules([]operation.ServiceRoutes{{Service: catalog, Routes: []string{route}}})
		if err != nil {
			return
		}
		got := operation.NewNormalizer(rules).Normalize(catalog, &operation.Facts{HTTPMethod: "GET", Path: route})
		if want := "GET " + route; got.Name != want || !utf8.ValidString(got.Name) {
			t.Fatalf("an accepted route %q normalizes to %q, want %q", route, got.Name, want)
		}
	})
}

func FuzzNormalizeWithRules(f *testing.F) {
	f.Add("/products/ABC/reviews")
	f.Add("/" + strings.Repeat("z", 300) + "/b")
	f.Add("/v1/acme/items/9")
	rules, err := operation.NewRules([]operation.ServiceRoutes{{Service: catalog, Routes: []string{
		"/{a}", "/products/{sku}/reviews", "/v1/{tenant}/items/{item}", "/products/top/{tab}",
	}}})
	if err != nil {
		f.Fatal(err)
	}
	normalizer := operation.NewNormalizer(rules)
	f.Fuzz(func(t *testing.T, path string) {
		got := normalizer.Normalize(catalog, &operation.Facts{HTTPMethod: "GET", Path: path})
		if err := got.Validate(); err != nil {
			t.Fatalf("Normalize() = %+v: %v", got, err)
		}
		if strings.HasSuffix(got.Name, "…") {
			return
		}
		if again := normalizeAgain(normalizer, got); again != got {
			t.Fatalf("N(N(x)) = %+v, N(x) = %+v", again, got)
		}
	})
}
