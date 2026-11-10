package operation_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func TestAcceptanceFRSTR006TenThousandIDsFormOneSeries(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	normalizer := operation.NewNormalizer(nil)
	names := make(map[string]int)
	for range 10_000 {
		path := fmt.Sprintf("/users/%d/orders/%s", random.Uint64(), randomUUID(random))
		names[normalizer.Normalize(catalog, &operation.Facts{HTTPMethod: "GET", Path: path}).Name]++
	}
	if diff := cmp.Diff(map[string]int{"GET /users/{id}/orders/{id}": 10_000}, names); diff != "" {
		t.Errorf("series mismatch (-want +got):\n%s", diff)
	}
}

func randomUUID(random *rand.Rand) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", random.Uint32(), random.IntN(1<<16), random.IntN(1<<16),
		random.IntN(1<<16), random.Uint64()>>16)
}

const base64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

func TestRandomTokensRarelyStayLiteral(t *testing.T) {
	tests := []struct {
		name string
		args int
		want float64
	}{
		{name: "length 20", args: 20, want: 0.06},
		{name: "length 22", args: 22, want: 0.03},
		{name: "length 32", args: 32, want: 0.001},
	}
	normalizer := operation.NewNormalizer(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			random := rand.New(rand.NewPCG(3, uint64(tt.args)))
			const samples = 100_000
			literal := 0
			for range samples {
				name := normalizer.Normalize(catalog, &operation.Facts{HTTPMethod: "GET",
					Path: "/t/" + randomToken(random, tt.args)}).Name
				if name != "GET /t/{id}" {
					literal++
				}
			}
			if share := float64(literal) / samples; share > tt.want {
				t.Fatalf("%.4f of random tokens stayed literal, want at most %.4f", share, tt.want)
			}
		})
	}
}

func randomToken(random *rand.Rand, length int) string {
	var token strings.Builder
	for range length {
		token.WriteByte(base64URL[random.IntN(len(base64URL))])
	}
	return token.String()
}

func TestNormalizerIsSafeForParallelHandlers(t *testing.T) {
	rules, err := operation.NewRules([]operation.ServiceRoutes{{Service: catalog, Routes: []string{"/p/{sku}/r"}}})
	if err != nil {
		t.Fatal(err)
	}
	normalizer := operation.NewNormalizer(rules)
	inputs := []operation.Facts{
		{HTTPMethod: "GET", Path: "/p/1/r"}, {HTTPMethod: "POST", Path: "/users/7"},
		{RPCSystem: "grpc", RPCService: "s", RPCMethod: "M"}, {HTTPMethod: "GET", Route: "/a/{b}"},
	}
	want := make([]operation.Operation, len(inputs))
	for i := range inputs {
		want[i] = normalizer.Normalize(catalog, &inputs[i])
	}
	var wg sync.WaitGroup
	results := make([][]operation.Operation, 8)
	for worker := range results {
		wg.Go(func() {
			for range 1000 {
				round := make([]operation.Operation, len(inputs))
				for i := range inputs {
					round[i] = normalizer.Normalize(catalog, &inputs[i])
				}
				results[worker] = round
			}
		})
	}
	wg.Wait()
	for worker, got := range results {
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("worker %d mismatch (-want +got):\n%s", worker, diff)
		}
	}
}

func TestNormalizeAllocatesOnlyTheName(t *testing.T) {
	normalizer := operation.NewNormalizer(nil)
	facts := operation.Facts{HTTPMethod: "GET", Path: "/users/123/orders/3F2504E0-4F89-11D3-9A0C-0305E82C3301"}
	if allocs := testing.AllocsPerRun(1000, func() { normalizer.Normalize(catalog, &facts) }); allocs > 1 {
		t.Fatalf("Normalize allocated %v times, want at most 1", allocs)
	}
}
