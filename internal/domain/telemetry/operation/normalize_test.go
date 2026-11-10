package operation_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

var catalog = resource.ServiceRef{Namespace: "shop", Name: "catalog"}

func httpOp(name string) operation.Operation {
	return operation.Operation{Protocol: operation.ProtocolHTTP, Name: name}
}

func cut(name string) string {
	const room = operation.MaxNameBytes - len("…")
	end := room
	for !utf8.RuneStart(name[end]) {
		end--
	}
	return name[:end] + "…"
}

func normalize(t *testing.T, rules *operation.Rules, facts *operation.Facts) operation.Operation {
	t.Helper()
	return operation.NewNormalizer(rules).Normalize(catalog, facts)
}

func TestNormalizePicksTheFirstMatchingRule(t *testing.T) {
	tests := []struct {
		name string
		args operation.Facts
		want operation.Operation
	}{
		{name: "gRPC method with a separate service",
			args: operation.Facts{RPCSystem: "grpc", RPCService: "oteldemo.ProductCatalogService", RPCMethod: "GetProduct"},
			want: operation.Operation{Protocol: operation.ProtocolGRPC, Name: "oteldemo.ProductCatalogService/GetProduct"}},
		{name: "gRPC method in the full form with a leading slash",
			args: operation.Facts{RPCSystem: "grpc", RPCService: "ignored",
				RPCMethod: "/oteldemo.ProductCatalogService/GetProduct"},
			want: operation.Operation{Protocol: operation.ProtocolGRPC, Name: "oteldemo.ProductCatalogService/GetProduct"}},
		{name: "connect RPC is gRPC", args: operation.Facts{RPCSystem: "connect_rpc", RPCService: "s", RPCMethod: "M"},
			want: operation.Operation{Protocol: operation.ProtocolGRPC, Name: "s/M"}},
		{name: "other RPC system", args: operation.Facts{RPCSystem: "apache_dubbo", RPCService: "s", RPCMethod: "M"},
			want: operation.Operation{Protocol: operation.ProtocolRPC, Name: "s/M"}},
		{name: "RPC beats an HTTP route", args: operation.Facts{RPCMethod: "a/M", HTTPMethod: "POST", Route: "/x"},
			want: operation.Operation{Protocol: operation.ProtocolRPC, Name: "a/M"}},
		{name: "route beats the path", args: operation.Facts{HTTPMethod: "GET", Route: "/users/{user}", Path: "/users/7"},
			want: httpOp("GET /users/{user}")},
		{name: "url.path", args: operation.Facts{HTTPMethod: "GET", Path: "/users/7"}, want: httpOp("GET /users/{id}")},
		{name: "http.target without its query", args: operation.Facts{HTTPMethod: "GET", Target: "/cart?item=7#top"},
			want: httpOp("GET /cart")},
		{name: "path of url.full", args: operation.Facts{HTTPMethod: "PUT", URL: "https://cart:8080/cart/42?x=1"},
			want: httpOp("PUT /cart/{id}")},
		{name: "url.full without a path is the root", args: operation.Facts{HTTPMethod: "GET", URL: "http://cart:8080"},
			want: httpOp("GET /")},
		{name: "url.path beats http.target and url.full", args: operation.Facts{HTTPMethod: "GET", Path: "/p/x",
			Target: "/t/x", URL: "http://h/u/x"}, want: httpOp("GET /p/x")},
		{name: "http.target beats url.full", args: operation.Facts{HTTPMethod: "GET", Target: "/t/x",
			URL: "http://h/u/x"}, want: httpOp("GET /t/x")},
		{name: "absolute-form http.target", args: operation.Facts{HTTPMethod: "GET", Target: "http://h/a/7?x=1"},
			want: httpOp("GET /a/{id}")},
		{name: "query and fragment are cut from url.path", args: operation.Facts{HTTPMethod: "GET", Path: "/cart#x"},
			want: httpOp("GET /cart")},
		{name: "url.full with a query but no path is the root",
			args: operation.Facts{HTTPMethod: "GET", URL: "http://api?next=/users/1"}, want: httpOp("GET /")},
		{name: "url.full with a fragment but no path is the root",
			args: operation.Facts{HTTPMethod: "GET", URL: "http://api#/a/b"}, want: httpOp("GET /")},
		{name: "scheme separator in a relative path without a slash",
			args: operation.Facts{HTTPMethod: "GET", URL: "rel?to=http://x/y/9"}, want: httpOp("GET /rel")},
		{name: "scheme must start with a letter", args: operation.Facts{HTTPMethod: "GET", URL: "1http://h/a"},
			want: httpOp("GET /1http:/h/a")},
		{name: "scheme separator inside a query is not a scheme",
			args: operation.Facts{HTTPMethod: "GET", URL: "/rel?to=http://x/y/9"}, want: httpOp("GET /rel")},
		{name: "RPC method of only a slash", args: operation.Facts{RPCMethod: "/"},
			want: operation.Operation{Protocol: operation.ProtocolRPC, Name: "/"}},
		{name: "RPC method with several leading slashes", args: operation.Facts{RPCMethod: "//svc/M"},
			want: operation.Operation{Protocol: operation.ProtocolRPC, Name: "svc/M"}},
		{name: "unknown operation", args: operation.Facts{HTTPMethod: "GET"}, want: operation.Operation{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, normalize(t, nil, &tt.args)); diff != "" {
				t.Errorf("operation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNormalizeTemplatesPathSegments(t *testing.T) {
	long := strings.Repeat("z", 300)
	tests := []struct {
		name string
		args string
		want string
	}{
		{name: "decimal id", args: "/users/123", want: "GET /users/{id}"},
		{name: "UUID in upper case", args: "/orders/3F2504E0-4F89-11D3-9A0C-0305E82C3301", want: "GET /orders/{id}"},
		{name: "hex of 16", args: "/blobs/0123456789abcdef", want: "GET /blobs/{id}"},
		{name: "short git SHA", args: "/commits/3f2a9c1", want: "GET /commits/{id}"},
		{name: "hex word without digits stays", args: "/blobs/deadbeef", want: "GET /blobs/deadbeef"},
		{name: "six hex characters stay", args: "/colors/ff00aa", want: "GET /colors/ff00aa"},
		{name: "prefixed order number", args: "/orders/ORD-000123", want: "GET /orders/{id}"},
		{name: "number after an underscore", args: "/users/user_42", want: "GET /users/{id}"},
		{name: "ISO date", args: "/reports/2024-01-02", want: "GET /reports/{id}"},
		{name: "RFC 3339 time", args: "/at/2024-01-02T03:04:05Z", want: "GET /at/{id}"},
		{name: "negative number", args: "/delta/-42", want: "GET /delta/{id}"},
		{name: "fraction", args: "/ts/1700000000.123", want: "GET /ts/{id}"},
		{name: "IPv4 address", args: "/hosts/10.0.0.1", want: "GET /hosts/{id}"},
		{name: "UUID in braces", args: "/o/{3F2504E0-4F89-11D3-9A0C-0305E82C3301}", want: "GET /o/{id}"},
		{name: "UUID URN", args: "/o/urn:uuid:3f2504e0-4f89-11d3-9a0c-0305e82c3301", want: "GET /o/{id}"},
		{name: "version stays", args: "/api/v12", want: "GET /api/v12"},
		{name: "semantic version stays", args: "/sdk/v1.2.3", want: "GET /sdk/v1.2.3"},
		{name: "three digits inside a word stay", args: "/certs/x509", want: "GET /certs/x509"},
		{name: "e-mail", args: "/users/jane@example.com", want: "GET /users/{id}"},
		{name: "JWT", args: "/s/eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOjF9.d2VsbDE2", want: "GET /s/{id}"},
		{name: "base64url token", args: "/t/aB3dE5fG7h-J9kL1mN_3pQ", want: "GET /t/{id}"},
		{name: "standard base64 token with + and =", args: "/t/aB3dE5fG7h+J9kL1mN3pQ=", want: "GET /t/{id}"},
		{name: "token of 19 stays", args: "/t/aB3dE5fG7h1J9kL1mN3", want: "GET /t/aB3dE5fG7h1J9kL1mN3"},
		{name: "token of 20 with 4 switches stays", args: "/t/abcdefgh1ijklmno2pqr",
			want: "GET /t/abcdefgh1ijklmno2pqr"},
		{name: "token of 20 with 5 switches", args: "/t/abcdefgh1ijklmno2pqR", want: "GET /t/{id}"},
		{name: "upper-case hex of 16", args: "/blobs/0123456789ABCDEF", want: "GET /blobs/{id}"},
		{name: "percent-encoded e-mail", args: "/users/jane%40example.com", want: "GET /users/{id}"},
		{name: "kebab word with a version stays", args: "/product-catalog-service-v2",
			want: "GET /product-catalog-service-v2"},
		{name: "versioned words stay", args: "/api-v1-users-v2-orders-v3-items",
			want: "GET /api-v1-users-v2-orders-v3-items"},
		{name: "abbreviations stay", args: "/getHTTPResponseHeaders", want: "GET /getHTTPResponseHeaders"},
		{name: "camel case stays", args: "/OAuthCallbackHandlerV", want: "GET /OAuthCallbackHandlerV"},
		{name: "root", args: "/", want: "GET /"},
		{name: "empty segments and a trailing slash", args: "//users///7/", want: "GET /users/{id}"},
		{name: "control characters and invalid UTF-8", args: "/a\x01b/\xffc", want: "GET /a�b/�c"},
		{name: "C1 control character", args: "/a\u0085b", want: "GET /a\uFFFDb"},
		{name: "33 segments keep 32 and one ellipsis", args: strings.Repeat("/s", 33),
			want: "GET " + strings.Repeat("/s", 32) + "/…"},
		{name: "segment longer than a name is an id without a scan", args: "/" + long, want: "GET /{id}"},
		{name: "long name is cut with an ellipsis", args: strings.Repeat("/"+long[:20], 13),
			want: cut("GET " + strings.Repeat("/"+long[:20], 13))},
		{name: "cut on a rune start", args: strings.Repeat("/"+strings.Repeat("я", 20), 10),
			want: cut("GET " + strings.Repeat("/"+strings.Repeat("я", 20), 10))},
		{name: "cut backs up to the start of a rune", args: "/a" + strings.Repeat("/"+strings.Repeat("я", 20), 10),
			want: cut("GET /a" + strings.Repeat("/"+strings.Repeat("я", 20), 10))},
		{name: "UUID without digits inside a segment", args: "/o/order-abcdefab-cdef-abcd-efab-cdefabcdefab",
			want: "GET /o/{id}"},
		{name: "UUID without digits at the start of a segment", args: "/o/abcdefab-cdef-abcd-efab-cdefabcdefab.json",
			want: "GET /o/{id}"},
		{name: "number must end in a digit", args: "/n/12.", want: "GET /n/12."},
		{name: "number must start with a digit", args: "/n/.5", want: "GET /n/.5"},
		{name: "numeric suffix needs a word before the separator", args: "/n/_5", want: "GET /n/_5"},
		{name: "braced UUID without a run of four digits", args: "/o/{abcdef12-a3b4-c5d6-e7f8-a9b0c1d2e3f4}",
			want: "GET /o/{id}"},
		{name: "letter-only hex of 16", args: "/blobs/deadbeefcafebabe", want: "GET /blobs/{id}"},
		{name: "letter-only hex of 15 stays", args: "/blobs/deadbeefcafebab", want: "GET /blobs/deadbeefcafebab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalize(t, nil, &operation.Facts{HTTPMethod: "GET", Path: tt.args})
			if diff := cmp.Diff(httpOp(tt.want), got); diff != "" {
				t.Errorf("operation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNormalizeCanonicalizesTheHTTPMethod(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{name: "GET", args: "GET", want: "GET /"},
		{name: "HEAD", args: "HEAD", want: "HEAD /"},
		{name: "POST", args: "POST", want: "POST /"},
		{name: "PUT", args: "PUT", want: "PUT /"},
		{name: "DELETE", args: "DELETE", want: "DELETE /"},
		{name: "CONNECT", args: "CONNECT", want: "CONNECT /"},
		{name: "OPTIONS", args: "OPTIONS", want: "OPTIONS /"},
		{name: "TRACE", args: "TRACE", want: "TRACE /"},
		{name: "PATCH", args: "PATCH", want: "PATCH /"},
		{name: "lower case is other", args: "get", want: "_OTHER /"},
		{name: "missing method is other", args: "", want: "_OTHER /"},
		{name: "custom method is other", args: "PURGE", want: "_OTHER /"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalize(t, nil, &operation.Facts{HTTPMethod: tt.args, Path: "/"})
			if diff := cmp.Diff(httpOp(tt.want), got); diff != "" {
				t.Errorf("operation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReservedOverflowNameIsUnreachable(t *testing.T) {
	got := normalize(t, nil, &operation.Facts{RPCMethod: operation.OverflowName})
	want := operation.Operation{Protocol: operation.ProtocolRPC, Name: "/" + operation.OverflowName}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("operation mismatch (-want +got):\n%s", diff)
	}
}

func TestServiceRulesOverrideTheTemplate(t *testing.T) {
	rules, err := operation.NewRules([]operation.ServiceRoutes{{Service: catalog, Routes: []string{
		"/products/{sku}/reviews", "/products/{sku}/{tab}/more", "/products/top/{tab}/more", "/v1/{tenant}/items/{item}",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args string
		want string
	}{
		{name: "parameter matches any segment", args: "/products/ABC-1/reviews", want: "GET /products/{sku}/reviews"},
		{name: "literal in an earlier segment wins", args: "/products/top/x/more", want: "GET /products/top/{tab}/more"},
		{name: "backtracks from a dead literal branch", args: "/products/top/reviews",
			want: "GET /products/{sku}/reviews"},
		{name: "whole path with parameters", args: "/v1/acme/items/9", want: "GET /v1/{tenant}/items/{item}"},
		{name: "no rule falls back to the built-in template", args: "/v1/acme/items", want: "GET /v1/acme/items"},
		{name: "rule needs the exact segment count", args: "/v1/acme/items/9/x", want: "GET /v1/acme/items/{id}/x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalize(t, rules, &operation.Facts{HTTPMethod: "GET", Path: tt.args})
			if diff := cmp.Diff(httpOp(tt.want), got); diff != "" {
				t.Errorf("operation mismatch (-want +got):\n%s", diff)
			}
		})
	}
	other := resource.ServiceRef{Namespace: "shop", Name: "cart"}
	got := operation.NewNormalizer(rules).Normalize(other, &operation.Facts{HTTPMethod: "GET", Path: "/products/x/reviews"})
	if diff := cmp.Diff(httpOp("GET /products/x/reviews"), got); diff != "" {
		t.Errorf("rules of catalog applied to cart (-want +got):\n%s", diff)
	}
}
