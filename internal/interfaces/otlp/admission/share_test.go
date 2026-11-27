package admission_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const (
	holdLimit         = 15 * time.Second
	collectorHorizon  = 300 * time.Second
	collectorStep     = 50 * time.Millisecond
	collectorSeeds    = 1000
	collectorInitial  = 5 * time.Second
	collectorMaximum  = 30 * time.Second
	collectorMultiple = 1.5
	collectorJitter   = 0.5
	collectorFloor    = time.Second
	seedStream        = 7
)

var (
	tokenA = token("tokenA000000")
	tokenB = token("tokenB000000")
	tokenC = token("tokenC000000")
)

func TestALateTokenGetsASlotOnceAHolderReleases(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	held := hold(t, gate, tokenA, 10)
	clock.Advance(5 * time.Second)
	_, lateArrival := enter(gate, tokenB)
	clock.Advance(200 * time.Millisecond)
	held[0]()
	_, holderAgain := enter(gate, tokenA)
	clock.Advance(800 * time.Millisecond)
	_, lateRetry := enter(gate, tokenB)
	want := []string{refusedBySlots, refusedByShare, admitted}
	got := []string{lateArrival, holderAgain, lateRetry}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("B late, A releases one and asks again, B retries (-want +got):\n%s", diff)
	}
}

type stalledBody struct {
	release func()
	until   time.Time
}

type backoff struct {
	random   *rand.Rand
	interval time.Duration
}

func (b *backoff) next() time.Duration {
	spread := collectorJitter * float64(b.interval)
	wait := time.Duration(float64(b.interval) - spread + b.random.Float64()*2*spread)
	b.interval = min(collectorMaximum, time.Duration(float64(b.interval)*collectorMultiple))
	return max(collectorFloor, wait)
}

func TestALateTokenGetsInAgainstACollectorBackoff(t *testing.T) {
	var lost []uint64
	for seed := range uint64(collectorSeeds) {
		if !lateTokenGetsIn(t, seed) {
			lost = append(lost, seed)
		}
	}
	if len(lost) > 0 {
		t.Errorf("B with a Collector backoff against a stalling A was never admitted in %d of %d seeds: %v",
			len(lost), collectorSeeds, lost)
	}
}

func lateTokenGetsIn(t *testing.T, seed uint64) bool {
	t.Helper()
	random := rand.New(rand.NewPCG(seed, seedStream))
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	begin := clock.Now()
	retry := &backoff{
		random:   random,
		interval: collectorInitial,
	}
	nextB := begin.Add(holdLimit + time.Duration(random.Float64()*float64(holdLimit)))
	giveUp := nextB.Add(collectorHorizon)
	var stalled []stalledBody
	for clock.Set(begin); !clock.Now().After(giveUp); clock.Advance(collectorStep) {
		stalled = stallEveryAdmitted(gate, clock.Now(), expire(clock.Now(), stalled))
		if clock.Now().Before(nextB) {
			continue
		}
		if enterAndLeave(gate, tokenB) == admitted {
			return true
		}
		nextB = clock.Now().Add(retry.next())
	}
	return false
}

func expire(now time.Time, stalled []stalledBody) []stalledBody {
	kept := stalled[:0]
	for _, body := range stalled {
		if now.Before(body.until) {
			kept = append(kept, body)
			continue
		}
		body.release()
	}
	return kept
}

func stallEveryAdmitted(gate admission.Gate, now time.Time, stalled []stalledBody) []stalledBody {
	for range 10 {
		release, outcome := enter(gate, tokenA)
		if outcome != admitted {
			return stalled
		}
		stalled = append(stalled, stalledBody{
			release: release,
			until:   now.Add(holdLimit),
		})
	}
	return stalled
}

func TestAQuietHolderKeepsItsShare(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	enterAndLeave(gate, tokenB)
	hold(t, gate, tokenA, 5)
	clock.Advance(11 * time.Second)
	enterAndLeave(gate, tokenC)
	clock.Advance(2 * time.Second)
	if _, outcome := enter(gate, tokenA); outcome != refusedByShare {
		t.Errorf("A holding 5 slots quiet for 11 s, after C and B: %s, want %s", outcome, refusedByShare)
	}
}

type refusal struct {
	Admitted int
	Then     string
}

func TestTheShareFollowsTheActiveTokens(t *testing.T) {
	tests := []struct {
		name string
		args int
		want int
	}{
		{
			name: "one token",
			args: 1,
			want: 10,
		},
		{
			name: "two tokens",
			args: 2,
			want: 5,
		},
		{
			name: "three tokens round up",
			args: 3,
			want: 4,
		},
		{
			name: "four tokens round up",
			args: 4,
			want: 3,
		},
		{
			name: "six tokens",
			args: 6,
			want: 2,
		},
		{
			name: "eleven tokens keep the minimum share",
			args: 11,
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := newGate(t, defaultLimits(), newClock())
			for index := range tt.args - 1 {
				enterAndLeave(gate, numbered(index))
			}
			share, stop := admitUntilRefused(gate, tokenA)
			want := refusal{
				Admitted: tt.want,
				Then:     refusedByShare,
			}
			got := refusal{
				Admitted: share,
				Then:     stop,
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("A among %d active tokens (-want +got):\n%s", tt.args, diff)
			}
		})
	}
}

func TestAReleaseKeepsATokenActive(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	releaseB := hold(t, gate, tokenB, 1)[0]
	clock.Advance(15 * time.Second)
	releaseB()
	clock.Advance(5 * time.Second)
	if share, _ := admitUntilRefused(gate, tokenA); share != 5 {
		t.Errorf("B released 5 s ago after holding 15 s: A share %d, want 5", share)
	}
}

type quiet struct {
	setup func(t *testing.T, gate admission.Gate)
	quiet time.Duration
}

func TestTheActiveWindowForgetsAQuietToken(t *testing.T) {
	type want struct {
		Share        int
		ActiveTokens int
	}
	tests := []struct {
		name string
		args quiet
		want want
	}{
		{
			name: "admitted, quiet 9 s: still active",
			args: quiet{
				setup: admittedOnce,
				quiet: 9 * time.Second,
			},
			want: want{
				Share:        5,
				ActiveTokens: 2,
			},
		},
		{
			name: "admitted, quiet 11 s: forgotten",
			args: quiet{
				setup: admittedOnce,
				quiet: 11 * time.Second,
			},
			want: want{
				Share:        10,
				ActiveTokens: 1,
			},
		},
		{
			name: "refused by slots, quiet 59 s: still waiting",
			args: quiet{
				setup: refusedBySlotsOnce,
				quiet: 59 * time.Second,
			},
			want: want{
				Share:        5,
				ActiveTokens: 2,
			},
		},
		{
			name: "refused by slots, quiet 61 s: forgotten",
			args: quiet{
				setup: refusedBySlotsOnce,
				quiet: 61 * time.Second,
			},
			want: want{
				Share:        10,
				ActiveTokens: 1,
			},
		},
		{
			name: "refused by rate, quiet 11 s: forgotten",
			args: quiet{
				setup: refusedByRateOnce,
				quiet: 11 * time.Second,
			},
			want: want{
				Share:        10,
				ActiveTokens: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newClock()
			gate := newGate(t, defaultLimits(), clock)
			tt.args.setup(t, gate)
			clock.Advance(tt.args.quiet)
			share, _ := admitUntilRefused(gate, tokenA)
			got := want{
				Share:        share,
				ActiveTokens: gate.Admission.Stats().ActiveTokens,
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("A after B went quiet (-want +got):\n%s", diff)
			}
		})
	}
}

func admittedOnce(_ *testing.T, gate admission.Gate) {
	enterAndLeave(gate, tokenB)
}

func refusedBySlotsOnce(t *testing.T, gate admission.Gate) {
	t.Helper()
	held := hold(t, gate, tokenC, 10)
	if outcome := enterAndLeave(gate, tokenB); outcome != refusedBySlots {
		t.Fatalf("B on full slots: %s, want %s", outcome, refusedBySlots)
	}
	releaseAll(held)
}

func refusedByRateOnce(t *testing.T, gate admission.Gate) {
	t.Helper()
	for range 2000 {
		enterAndLeave(gate, tokenC)
	}
	if outcome := enterAndLeave(gate, tokenB); outcome != refusedByRate {
		t.Fatalf("B after the instance budget is spent: %s, want %s", outcome, refusedByRate)
	}
}

func TestAWaitingTokenStopsWaitingOnceAdmitted(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	held := hold(t, gate, tokenA, 10)
	clock.Advance(500 * time.Millisecond)
	if _, outcome := enter(gate, tokenB); outcome != refusedBySlots {
		t.Fatalf("B while A holds every slot: %s, want %s", outcome, refusedBySlots)
	}
	held[0]()
	clock.Advance(500 * time.Millisecond)
	if outcome := enterAndLeave(gate, tokenB); outcome != admitted {
		t.Fatalf("B once A released one: %s, want %s", outcome, admitted)
	}
	releaseAll(held[1:])
	clock.Advance(11 * time.Second)
	count, stop := admitUntilRefused(gate, tokenA)
	want := refusal{
		Admitted: 10,
		Then:     refusedByShare,
	}
	got := refusal{
		Admitted: count,
		Then:     stop,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("A alone 11 s after B was refused, then admitted and left (-want +got):\n%s", diff)
	}
}

func TestStatsForgetTokensQuietBeyondTheirWindows(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	held := hold(t, gate, tokenA, 10)
	if _, outcome := enter(gate, tokenB); outcome != refusedBySlots {
		t.Fatalf("B while A holds every slot: %s, want %s", outcome, refusedBySlots)
	}
	releaseAll(held)
	clock.Advance(59 * time.Second)
	waiting := gate.Admission.Stats()
	clock.Advance(2 * time.Second)
	idle := gate.Admission.Stats()
	type outcome struct {
		InUse        int
		ActiveTokens int
	}
	want := []outcome{
		{
			ActiveTokens: 1,
		},
		{},
	}
	got := []outcome{
		{
			InUse:        waiting.InUse,
			ActiveTokens: waiting.ActiveTokens,
		},
		{
			InUse:        idle.InUse,
			ActiveTokens: idle.ActiveTokens,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("A released 10 and B refused by slots, stats with no request after 59 s and 61 s (-want +got):\n%s", diff)
	}
}
