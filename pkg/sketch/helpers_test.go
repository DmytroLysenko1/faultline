package sketch_test

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

const (
	defaultBudget   = 8 << 20
	concedeBudget   = 4 << 20
	smallBudget     = 4 << 20
	minimalBudget   = sketch.MaxEntriesPerSketch
	medianLatencyNs = 5_000_000
	fuzzBudget      = 1 << 20
	subjectCells    = 420_000
	allBins         = 1093
	halfNanosecond  = 0.5
)

var gamma = (1 + sketch.RelativeAccuracy) / (1 - sketch.RelativeAccuracy)

func newArena(t testing.TB, budget int) *sketch.Arena {
	t.Helper()
	arena, err := sketch.NewArena(budget)
	if err != nil {
		t.Fatalf("NewArena(%d): %v", budget, err)
	}
	return arena
}

func lognormal(random *rand.Rand, median, sigma float64) int64 {
	return int64(median * math.Exp(sigma*random.NormFloat64()))
}

func lognormalSample(seed uint64, count int, sigma float64) []int64 {
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	values := make([]int64, count)
	for i := range values {
		values[i] = lognormal(random, medianLatencyNs, sigma)
	}
	return values
}

func record(arena *sketch.Arena, values []int64) *sketch.Sketch {
	s := new(sketch.Sketch)
	for _, value := range values {
		arena.Add(s, value)
	}
	return s
}

func exactPercentiles(values []int64) sketch.Percentiles {
	sorted := slices.Sorted(slices.Values(values))
	at := func(quantile float64) int64 {
		return sorted[int(quantile*float64(len(sorted)-1))]
	}
	return sketch.Percentiles{P50: at(0.50), P95: at(0.95), P99: at(0.99)}
}

func withinAccuracy(got, exact int64) bool {
	return math.Abs(float64(got-exact)) <= sketch.RelativeAccuracy*float64(exact)+halfNanosecond
}

func assertPercentilesAccurate(t *testing.T, got, exact sketch.Percentiles) {
	t.Helper()
	pairs := []struct {
		name       string
		got, exact int64
	}{
		{name: "p50", got: got.P50, exact: exact.P50},
		{name: "p95", got: got.P95, exact: exact.P95},
		{name: "p99", got: got.P99, exact: exact.P99},
	}
	for _, pair := range pairs {
		if !withinAccuracy(pair.got, pair.exact) {
			t.Errorf("%s = %d, exact %d: relative error %.4f > %.2f", pair.name, pair.got, pair.exact,
				math.Abs(float64(pair.got-pair.exact))/float64(pair.exact), sketch.RelativeAccuracy)
		}
	}
}

func valueInBin(index int) int64 {
	return int64(math.Round(2 * math.Pow(gamma, float64(index)) / (gamma + 1)))
}

func distinctBinValues(first, count int) []int64 {
	values := make([]int64, count)
	for i := range values {
		values[i] = valueInBin(first + i)
	}
	return values
}

func reversed(values []int64) []int64 {
	backward := slices.Clone(values)
	slices.Reverse(backward)
	return backward
}

func approxNanos() cmp.Option {
	return cmp.Comparer(func(x, y int64) bool {
		return withinAccuracy(x, y) && withinAccuracy(y, x)
	})
}
