package matcher_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	restoreScenarios = 3_000
	restoreSeed      = 20261113
	matcherSection   = checkpoint.Tag(1)
)

type pairingLog struct {
	Calls      []call.Call
	Outcomes   []matcher.Outcome
	Unanswered [][]call.Unanswered
	Stats      matcher.Stats
}

func feed(pairing *matcher.Matcher, steps []topologytest.Step) pairingLog {
	var log pairingLog
	for _, step := range steps {
		now := telemetrytest.Epoch().Add(step.At)
		log.Unanswered = append(log.Unanswered, pairing.Expire(now))
		for i := range step.Spans {
			matched, outcome := pairing.Match(&step.Spans[i], now)
			log.Calls = append(log.Calls, matched)
			log.Outcomes = append(log.Outcomes, outcome)
		}
	}
	log.Stats = pairing.Stats
	return log
}

func matcherCheckpoint(t *testing.T, pairing *matcher.Matcher) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(matcherSection)
	pairing.WriteCheckpoint(w)
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreMatcher(limits matcher.Limits, data []byte) (*matcher.Matcher, error) {
	r := checkpoint.NewReader(bytes.NewReader(data))
	r.Begin(matcherSection)
	restored, err := matcher.Restore(limits, r)
	if err != nil {
		return nil, err
	}
	r.End()
	return restored, r.Finish()
}

// TestARestoredMatcherPairsLikeTheOriginal: the waiting halves keep their order and deadlines and
// the tombstones their scope, so after a restore the same spans pair, duplicate, expire and get
// evicted exactly as in the matcher that never stopped — retries and clock jumps back included.
func TestARestoredMatcherPairsLikeTheOriginal(t *testing.T) {
	rng := rand.New(rand.NewPCG(restoreSeed, restoreSeed))
	for scenario := range restoreScenarios {
		limits := topologytest.RandomLimits(rng)
		steps := topologytest.RandomSteps(t, rng)
		split := rng.IntN(len(steps) + 1)
		original, err := matcher.New(limits)
		if err != nil {
			t.Fatalf("scenario %d: New: %v", scenario, err)
		}
		feed(original, steps[:split])
		data := matcherCheckpoint(t, original)
		restored, err := restoreMatcher(limits, data)
		if err != nil {
			t.Fatalf("scenario %d: restore: %v", scenario, err)
		}
		if !bytes.Equal(data, matcherCheckpoint(t, restored)) {
			t.Fatalf("scenario %d: a restored matcher writes other bytes", scenario)
		}

		want, got := feed(original, steps[split:]), feed(restored, steps[split:])

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("scenario %d, restored after step %d (-original +restored):\n%s", scenario, split, diff)
		}
	}
}

func TestAMatcherCheckpointNeedsRoomForItsHalves(t *testing.T) {
	limits := matcher.Limits{TTL: 10, MaxPending: 4, TombstoneTTL: 10, MaxTombstones: 4}
	original, err := matcher.New(limits)
	if err != nil {
		t.Fatal(err)
	}
	for trace := range byte(4) {
		span := telemetrytest.Client(t, trace+1, 1, "frontend", nil)
		original.Match(&span, telemetrytest.Epoch())
	}
	smaller := limits
	smaller.MaxPending--

	_, err = restoreMatcher(smaller, matcherCheckpoint(t, original))

	if !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Errorf("err = %v, want four halves refused by a queue of three", err)
	}
}
