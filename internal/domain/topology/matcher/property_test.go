package matcher_test

import (
	"math/rand/v2"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
)

const (
	propertyScenarios = 10_000
	propertySeed      = 20261109
)

func TestPairingAccountsForEverySpan(t *testing.T) {
	rng := rand.New(rand.NewPCG(propertySeed, propertySeed))
	for scenario := range propertyScenarios {
		limits := topologytest.RandomLimits(rng)
		pairing, err := matcher.New(limits)
		if err != nil {
			t.Fatalf("scenario %d: matcher.New(%+v): %v", scenario, limits, err)
		}
		var fed, pairs uint64
		for _, step := range topologytest.RandomSteps(t, rng) {
			now := telemetrytest.Epoch().Add(step.At)
			pairing.Expire(now)
			for _, span := range step.Spans {
				if _, side := role.Of(span); side != role.Ignored {
					fed++
				}
				if _, matched := pairing.Match(span, now); matched {
					pairs++
				}
			}
			checkAccounting(t, scenario, pairing, fed, pairs)
		}
	}
}

func checkAccounting(t *testing.T, scenario int, pairing *matcher.Matcher, fed, pairs uint64) {
	t.Helper()
	stats := pairing.Stats
	buckets := 2*stats.Matched + stats.Duplicate + stats.Unpairable + stats.Evicted + stats.Expired + stats.Pending
	switch {
	case stats.Observed != fed:
		t.Fatalf("scenario %d: observed %d, fed %d", scenario, stats.Observed, fed)
	case buckets != stats.Observed:
		t.Fatalf("scenario %d: I1 broken: observed %d != buckets %d (%+v)", scenario, stats.Observed, buckets, stats)
	case !stats.Balanced():
		t.Fatalf("scenario %d: Balanced disagrees with the bucket formula: %+v", scenario, stats)
	case stats.Matched != pairs:
		t.Fatalf("scenario %d: matched %d, returned calls %d", scenario, stats.Matched, pairs)
	case stats.ExpiredCaller > stats.Expired || stats.EvictedCaller > stats.Evicted:
		t.Fatalf("scenario %d: caller sub-counters exceed totals: %+v", scenario, stats)
	case pairing.Pending.Len() > pairing.Limits.MaxPending || len(pairing.PendingByKey) != pairing.Pending.Len():
		t.Fatalf(
			"scenario %d: pending list %d, index %d, limit %d",
			scenario,
			pairing.Pending.Len(),
			len(pairing.PendingByKey),
			pairing.Limits.MaxPending,
		)
	case uint64(pairing.Pending.Len()) != stats.Pending:
		t.Fatalf("scenario %d: pending counter %d, list %d", scenario, stats.Pending, pairing.Pending.Len())
	case pairing.Tombstones.Size > pairing.Limits.MaxTombstones ||
		len(pairing.Tombstones.ScopeByKey) != pairing.Tombstones.Size:
		t.Fatalf("scenario %d: tombstones ring %d, index %d, limit %d",
			scenario, pairing.Tombstones.Size, len(pairing.Tombstones.ScopeByKey), pairing.Limits.MaxTombstones)
	}
}
