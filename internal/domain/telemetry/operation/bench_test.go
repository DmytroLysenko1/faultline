package operation_test

import (
	"strings"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func BenchmarkNormalize(b *testing.B) {
	inputs := map[string]operation.Facts{
		"route":     {HTTPMethod: "GET", Route: "/api/products/{id}"},
		"path":      {HTTPMethod: "GET", Path: "/users/123/orders/3F2504E0-4F89-11D3-9A0C-0305E82C3301"},
		"rpc":       {RPCSystem: "grpc", RPCService: "oteldemo.ProductCatalogService", RPCMethod: "GetProduct"},
		"url.full":  {HTTPMethod: "POST", URL: "http://cart:8080/cart/checkout?x=1"},
		"long path": {HTTPMethod: "GET", Path: strings.Repeat("/segment", 64)},
	}
	normalizer := operation.NewNormalizer(nil)
	for name := range inputs {
		facts := inputs[name]
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				normalizer.Normalize(catalog, &facts)
			}
		})
	}
}

func BenchmarkRouteRulesWorst(b *testing.B) {
	rules, _ := combinationRules(b, 6)
	normalizer := operation.NewNormalizer(rules)
	facts := operation.Facts{HTTPMethod: "GET", Path: strings.Repeat("/x", operation.MaxPathSegments-1) + "/y"}
	b.ReportAllocs()
	for b.Loop() {
		normalizer.Normalize(catalog, &facts)
	}
}
