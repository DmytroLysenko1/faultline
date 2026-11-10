package matcher_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
)

func newMatcher(t *testing.T) *matcher.Matcher {
	t.Helper()
	pairing, err := matcher.New(testLimits())
	if err != nil {
		t.Fatalf("matcher.New: %v", err)
	}
	return pairing
}

func outcomesOf(t *testing.T, spans ...span.Span) []matcher.Outcome {
	t.Helper()
	pairing := newMatcher(t)
	outcomes := make([]matcher.Outcome, 0, len(spans))
	for i := range spans {
		_, outcome := pairing.Match(&spans[i], telemetrytest.Epoch())
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func TestMatchReportsHowEachSpanEnded(t *testing.T) {
	caller := telemetrytest.Client(t, 1, 1, "frontend", nil)
	callee := telemetrytest.Server(t, 1, 2, "cart", nil)
	tests := []struct {
		Name string
		Args []span.Span
		Want []matcher.Outcome
	}{
		{Name: "internal span", Args: []span.Span{telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
			Kind: kind.Internal, Trace: 2, SpanID: 1, Service: "cart"})},
			Want: []matcher.Outcome{matcher.OutcomeIgnored}},
		{Name: "client to a database", Args: []span.Span{telemetrytest.Client(t, 3, 1, "cart",
			map[string]string{"db.system.name": "postgresql"})},
			Want: []matcher.Outcome{matcher.OutcomeUnpairable}},
		{Name: "root server span", Args: []span.Span{telemetrytest.NewSpan(t, telemetrytest.SpanSpec{
			Kind: kind.Server, Trace: 4, SpanID: 1, Service: "frontend"})},
			Want: []matcher.Outcome{matcher.OutcomeUnpairable}},
		{Name: "pair, caller first", Args: []span.Span{caller, callee},
			Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeMatched}},
		{Name: "pair, callee first", Args: []span.Span{callee, caller},
			Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeMatched}},
		{Name: "retried callee while waiting", Args: []span.Span{callee, callee},
			Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeDuplicate}},
		{Name: "retried callee after the match", Args: []span.Span{caller, callee, callee},
			Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeMatched, matcher.OutcomeDuplicate}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, outcomesOf(t, tt.Args...)); diff != "" {
				t.Errorf("outcomes (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBestEffortRowsOfTheServedTable(t *testing.T) {
	limits := testLimits()
	tests := []struct {
		Name string
		Args func(pairing *matcher.Matcher) []matcher.Outcome
		Want []matcher.Outcome
	}{
		{Name: "callee after its caller expired is still a request", Args: func(pairing *matcher.Matcher) []matcher.Outcome {
			caller, callee := telemetrytest.Client(t, 1, 1, "frontend", nil), telemetrytest.Server(t, 1, 2, "cart", nil)
			_, first := pairing.Match(&caller, telemetrytest.Epoch())
			pairing.Expire(telemetrytest.Epoch().Add(limits.TTL))
			_, late := pairing.Match(&callee, telemetrytest.Epoch().Add(limits.TTL))
			return []matcher.Outcome{first, late}
		}, Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeWaiting}},
		{Name: "callee re-sent after its own expiry is counted again", Args: func(pairing *matcher.Matcher) []matcher.Outcome {
			callee := telemetrytest.Server(t, 1, 2, "cart", nil)
			_, first := pairing.Match(&callee, telemetrytest.Epoch())
			pairing.Expire(telemetrytest.Epoch().Add(limits.TTL))
			_, again := pairing.Match(&callee, telemetrytest.Epoch().Add(limits.TTL))
			return []matcher.Outcome{first, again}
		}, Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeWaiting}},
		{Name: "callee re-sent after eviction is counted again", Args: func(pairing *matcher.Matcher) []matcher.Outcome {
			callee := telemetrytest.Server(t, 1, 2, "cart", nil)
			_, first := pairing.Match(&callee, telemetrytest.Epoch())
			for trace := byte(2); trace < byte(2+limits.MaxPending); trace++ {
				other := telemetrytest.Server(t, trace, 2, "cart", nil)
				pairing.Match(&other, telemetrytest.Epoch())
			}
			_, again := pairing.Match(&callee, telemetrytest.Epoch())
			return []matcher.Outcome{first, again}
		}, Want: []matcher.Outcome{matcher.OutcomeWaiting, matcher.OutcomeWaiting}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, tt.Args(newMatcher(t))); diff != "" {
				t.Errorf("outcomes (-want +got):\n%s", diff)
			}
		})
	}
}

func TestServedFollowsTheRoleAndOutcomeTable(t *testing.T) {
	served := map[role.Role][]matcher.Outcome{
		role.Callee: {matcher.OutcomeWaiting, matcher.OutcomeMatched},
		role.Entry:  {matcher.OutcomeUnpairable},
	}
	roles := []role.Role{role.Ignored, role.Unpairable, role.Caller, role.Callee, role.Entry}
	outcomes := []matcher.Outcome{matcher.OutcomeIgnored, matcher.OutcomeUnpairable, matcher.OutcomeWaiting,
		matcher.OutcomeMatched, matcher.OutcomeDuplicate}
	for _, side := range roles {
		for _, outcome := range outcomes {
			t.Run(fmt.Sprintf("role %d outcome %d", side, outcome), func(t *testing.T) {
				want := slices.Contains(served[side], outcome)
				if got := outcome.Served(side); got != want {
					t.Errorf("Served = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestMatchedCallCarriesTheCallerTimeAndTheCalleeOperation(t *testing.T) {
	route := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart/{id}"}
	path := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart/42"}
	callerTime := eventtime.FromUnixNanos(1_000, 9_000)
	caller := telemetrytest.Client(t, 1, 1, "frontend", nil)
	caller.Interval, caller.Operation = callerTime, path
	callee := telemetrytest.Server(t, 1, 2, "cart", nil)
	callee.Interval, callee.Operation = eventtime.FromUnixNanos(2_000, 8_000), route
	pairing := newMatcher(t)

	pairing.Match(&callee, telemetrytest.Epoch())
	got, outcome := pairing.Match(&caller, telemetrytest.Epoch())

	want := call.Call{Caller: caller.Service, Callee: callee.Service, Interval: callerTime, Operation: route}
	if outcome != matcher.OutcomeMatched {
		t.Fatalf("outcome = %d, want matched", outcome)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("call (-want +got):\n%s", diff)
	}
}
