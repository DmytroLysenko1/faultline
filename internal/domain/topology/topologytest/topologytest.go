package topologytest

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
)

const (
	ServiceCount         = 4
	maxCallsPerScenario  = 20
	maxSpansPerStep      = 6
	maxSpansPerCall      = 3
	callerSpanID         = 1
	calleeSpanID         = 2
	rootServerSpanID     = 3
	databaseSpanID       = 4
	internalSpanID       = 5
	stepJitterMillis     = 15_000
	stepBackwardMillis   = 3_000
	retryWindowMillis    = 70_000
	retryOneIn           = 4
	duplicateOneIn       = 5
	maxTTLSeconds        = 10
	maxTombstoneTTLTimes = 6
	maxLimitEntries      = 8
)

type Step struct {
	At    time.Duration
	Spans []span.Span
}

type callShape func(tb testing.TB, trace byte, caller, callee string) []span.Span

func RandomService(rng *rand.Rand) string {
	return [ServiceCount]string{
		"frontend",
		"cart",
		"checkout",
		"payment",
	}[rng.IntN(ServiceCount)]
}

func RandomLimits(rng *rand.Rand) matcher.Limits {
	ttl := time.Duration(1+rng.IntN(maxTTLSeconds)) * time.Second
	return matcher.Limits{
		TTL:           ttl,
		MaxPending:    1 + rng.IntN(maxLimitEntries),
		TombstoneTTL:  ttl * time.Duration(1+rng.IntN(maxTombstoneTTLTimes)),
		MaxTombstones: 1 + rng.IntN(maxLimitEntries),
	}
}

func RandomSteps(tb testing.TB, rng *rand.Rand) []Step {
	tb.Helper()
	spans := randomSpans(tb, rng)
	rng.Shuffle(len(spans), func(i, j int) {
		spans[i], spans[j] = spans[j], spans[i]
	})
	var steps []Step
	var clock time.Duration
	for len(spans) > 0 {
		size := min(len(spans), 1+rng.IntN(maxSpansPerStep))
		clock += time.Duration(rng.IntN(stepJitterMillis)-stepBackwardMillis) * time.Millisecond
		steps = append(steps, Step{
			At:    clock,
			Spans: spans[:size],
		})
		spans = spans[size:]
		if rng.IntN(retryOneIn) == 0 {
			retried := steps[rng.IntN(len(steps))]
			retryAt := clock + time.Duration(rng.IntN(retryWindowMillis))*time.Millisecond
			steps = append(steps, Step{
				At:    retryAt,
				Spans: retried.Spans,
			})
		}
	}
	return steps
}

func randomSpans(tb testing.TB, rng *rand.Rand) []span.Span {
	tb.Helper()
	calls := 1 + rng.IntN(maxCallsPerScenario)
	shapes := callShapes()
	spans := make([]span.Span, 0, maxSpansPerCall*calls)
	for i := range calls {
		caller, callee := RandomService(rng), RandomService(rng)
		spans = append(spans, shapes[rng.IntN(len(shapes))](tb, byte(i+1), caller, callee)...)
		if rng.IntN(duplicateOneIn) == 0 {
			spans = append(spans, spans[len(spans)-1])
		}
	}
	return spans
}

func callShapes() []callShape {
	return []callShape{
		callerOnly,
		calleeOnly,
		rootServer,
		databaseClient,
		internalSpan,
		fullPair,
		fullPair,
		fullPair,
	}
}

func callerOnly(tb testing.TB, trace byte, caller, _ string) []span.Span {
	tb.Helper()
	return []span.Span{
		telemetrytest.Client(tb, trace, callerSpanID, caller, nil),
	}
}

func calleeOnly(tb testing.TB, trace byte, _, callee string) []span.Span {
	tb.Helper()
	return []span.Span{
		telemetrytest.Server(tb, trace, calleeSpanID, callee, nil),
	}
}

func rootServer(tb testing.TB, trace byte, _, callee string) []span.Span {
	tb.Helper()
	return []span.Span{
		telemetrytest.NewSpan(tb, telemetrytest.SpanSpec{
			Kind:    kind.Server,
			Trace:   trace,
			SpanID:  rootServerSpanID,
			Service: callee,
		}),
	}
}

func databaseClient(tb testing.TB, trace byte, caller, _ string) []span.Span {
	tb.Helper()
	database := map[string]string{
		"db.system.name": "postgresql",
	}
	return []span.Span{
		telemetrytest.Client(tb, trace, databaseSpanID, caller, database),
	}
}

func internalSpan(tb testing.TB, trace byte, caller, _ string) []span.Span {
	tb.Helper()
	return []span.Span{
		telemetrytest.NewSpan(tb, telemetrytest.SpanSpec{
			Kind:    kind.Internal,
			Trace:   trace,
			SpanID:  internalSpanID,
			Service: caller,
		}),
	}
}

func fullPair(tb testing.TB, trace byte, caller, callee string) []span.Span {
	tb.Helper()
	return []span.Span{
		telemetrytest.Client(tb, trace, callerSpanID, caller, nil),
		telemetrytest.Server(tb, trace, calleeSpanID, callee, nil),
	}
}
