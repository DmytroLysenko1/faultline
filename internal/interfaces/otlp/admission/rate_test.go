package admission_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const (
	rateRun          = 60 * time.Second
	heavyAttempts    = 40_000
	lightAttempts    = 10
	lightMinPercent  = 90
	instancePerRun   = 2000 * 60
	instanceSlack    = 2000
	halfBudgetPerRun = 1000*60 + 1000
)

type arrivals struct {
	random *rand.Rand
	next   time.Time
	rate   float64
}

func (a *arrivals) advance() {
	a.next = a.next.Add(time.Duration(a.random.ExpFloat64() / a.rate * float64(time.Second)))
}

func TestOneTokenCannotSpendTheWholeRateBudget(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	begin := clock.Now()
	random := rand.New(rand.NewPCG(1, 2))
	heavy := &arrivals{
		random: random,
		next:   begin,
		rate:   heavyAttempts,
	}
	light := &arrivals{
		random: random,
		next:   begin,
		rate:   lightAttempts,
	}
	counts := map[credential.ID]int{}
	tried := 0
	for {
		arrival, id := heavy, tokenA
		if light.next.Before(heavy.next) {
			arrival, id = light, tokenB
		}
		if arrival.next.Sub(begin) > rateRun {
			break
		}
		clock.Set(arrival.next)
		if id == tokenB {
			tried++
		}
		if enterAndLeave(gate, id) == admitted {
			counts[id]++
		}
		arrival.advance()
	}
	type outcome struct {
		LightAdmittedEnough bool
		InstanceWithin      bool
	}
	want := outcome{
		LightAdmittedEnough: true,
		InstanceWithin:      true,
	}
	got := outcome{
		LightAdmittedEnough: counts[tokenB]*100 >= tried*lightMinPercent,
		InstanceWithin:      counts[tokenA]+counts[tokenB] <= instancePerRun+instanceSlack,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("A tries %d/s, B %d/s for %s: A admitted %d, B %d of %d (-want +got):\n%s",
			heavyAttempts, lightAttempts, rateRun, counts[tokenA], counts[tokenB], tried, diff)
	}
}

func TestATokenSpendsAtMostItsPartOfTheRate(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	begin := clock.Now()
	enterAndLeave(gate, tokenB)
	heavyAdmitted := 0
	for step := range rateRun / time.Millisecond {
		clock.Set(begin.Add(step * time.Millisecond))
		if step%100 == 0 {
			enterAndLeave(gate, tokenB)
		}
		for range 40 {
			if enterAndLeave(gate, tokenA) == admitted {
				heavyAdmitted++
			}
		}
	}
	clock.Set(begin.Add(rateRun + 2*time.Second))
	afterIdle, _ := admitUntilReleased(gate, tokenA)
	type outcome struct {
		WithinHalf bool
		AfterIdle  int
	}
	want := outcome{
		WithinHalf: true,
		AfterIdle:  1000,
	}
	got := outcome{
		WithinHalf: heavyAdmitted <= halfBudgetPerRun,
		AfterIdle:  afterIdle,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("A next to B every 100 ms admitted %d in %s, at most (R/2)×60 + R/2 = %d; then 2 s idle (-want +got):\n%s",
			heavyAdmitted, rateRun, halfBudgetPerRun, diff)
	}
}

func TestArrivingTokensCannotStackTheirBursts(t *testing.T) {
	gate := newGate(t, defaultLimits(), newClock())
	total := 0
	stops := map[string]int{}
	for index := range 10 {
		count, stop := admitUntilReleased(gate, numbered(index))
		total += count
		stops[stop]++
	}
	type outcome struct {
		Total int
		Stops map[string]int
	}
	want := outcome{
		Total: 2000,
		Stops: map[string]int{
			refusedByRate: 10,
		},
	}
	got := outcome{
		Total: total,
		Stops: stops,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ten new tokens at one instant (-want +got):\n%s", diff)
	}
}

func admitUntilReleased(gate admission.Gate, id credential.ID) (int, string) {
	for count := range attemptCap {
		outcome := enterAndLeave(gate, id)
		if outcome != admitted {
			return count, outcome
		}
	}
	return attemptCap, neverRefused
}

func TestWaitingTokensDoNotDiluteTheRate(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	begin := clock.Now()
	stalled := hold(t, gate, tokenC, 10)
	for index := range 20 {
		clock.Set(begin.Add(time.Duration(index) * 100 * time.Millisecond))
		if outcome := enterAndLeave(gate, numbered(index)); outcome != refusedBySlots {
			t.Fatalf("waiting token %d: %s, want %s", index, outcome, refusedBySlots)
		}
	}
	clock.Set(begin.Add(15 * time.Second))
	releaseAll(stalled)
	random := rand.New(rand.NewPCG(3, 4))
	heavyAdmitted := 0
	const from, to = 15 * time.Second, 75 * time.Second
	for at := from; at < to; at += time.Millisecond {
		clock.Set(begin.Add(at))
		for range 2 {
			if random.Float64() < 0.75 && enterAndLeave(gate, tokenA) == admitted {
				heavyAdmitted++
			}
		}
	}
	if perSecond := float64(heavyAdmitted) / (to - from).Seconds(); perSecond < 1400 {
		t.Errorf("A tries 1500/s after 20 tokens waited and left: admitted %.0f/s, want at least 1400/s", perSecond)
	}
}

func TestAShareRefusalSpendsNoRate(t *testing.T) {
	limits := defaultLimits()
	limits.Slots = 4
	limits.RequestsPerSecond = 6
	gate := newGate(t, limits, newClock())
	enterAndLeave(gate, tokenB)
	held := hold(t, gate, tokenA, 2)
	refusals := map[string]int{}
	for range 1000 {
		_, outcome := enter(gate, tokenA)
		refusals[outcome]++
	}
	held[0]()
	_, again := enter(gate, tokenA)
	type outcome struct {
		Refusals map[string]int
		Again    string
	}
	want := outcome{
		Refusals: map[string]int{
			refusedByShare: 1000,
		},
		Again: admitted,
	}
	got := outcome{
		Refusals: refusals,
		Again:    again,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("A over its share 1 000 times, then one slot back (-want +got):\n%s", diff)
	}
}

func TestTheRateBudgetRefusesBeyondItsBurstAndRefills(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	burst, burstStop := admitUntilReleased(gate, tokenA)
	clock.Advance(time.Millisecond)
	refill, refillStop := admitUntilReleased(gate, tokenA)
	want := []refusal{
		{
			Admitted: 2000,
			Then:     refusedByRate,
		},
		{
			Admitted: 2,
			Then:     refusedByRate,
		},
	}
	got := []refusal{
		{
			Admitted: burst,
			Then:     burstStop,
		},
		{
			Admitted: refill,
			Then:     refillStop,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("2 000/s: at one instant, then 1 ms later (-want +got):\n%s", diff)
	}
}

func TestThirtyDaysIdleRefillsToTheBurstOnly(t *testing.T) {
	clock := newClock()
	gate := newGate(t, defaultLimits(), clock)
	enterAndLeave(gate, tokenA)
	clock.Advance(30 * 24 * time.Hour)
	total := 0
	for index := range 10 {
		count, _ := admitUntilReleased(gate, numbered(index))
		total += count
	}
	if total != 2000 {
		t.Errorf("after 30 days idle, ten tokens at one instant admitted %d, want the burst 2000", total)
	}
}

func TestANewTokenNextToAnActiveOneGetsHalfTheBurst(t *testing.T) {
	gate := newGate(t, defaultLimits(), newClock())
	enterAndLeave(gate, tokenA)
	count, stop := admitUntilReleased(gate, tokenB)
	want := refusal{
		Admitted: 1000,
		Then:     refusedByRate,
	}
	got := refusal{
		Admitted: count,
		Then:     stop,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("B new at the instant A was admitted once, 2 000/s (-want +got):\n%s", diff)
	}
}

func TestATokenRefusedOnArrivalKeepsTheBurstOfItsArrival(t *testing.T) {
	clock := newClock()
	limits := defaultLimits()
	limits.Slots = 2
	gate := newGate(t, limits, clock)
	begin := clock.Now()
	for index := range 4 {
		enterAndLeave(gate, numbered(index))
	}
	clock.Set(begin.Add(9 * time.Second))
	held := hold(t, gate, tokenC, 2)
	clock.Set(begin.Add(9600 * time.Millisecond))
	if _, outcome := enter(gate, tokenB); outcome != refusedBySlots {
		t.Fatalf("B among six tokens while C holds every slot: %s, want %s", outcome, refusedBySlots)
	}
	releaseAll(held)
	clock.Set(begin.Add(10050 * time.Millisecond))
	count, stop := admitUntilReleased(gate, tokenB)
	want := refusal{
		Admitted: 783,
		Then:     refusedByRate,
	}
	got := refusal{
		Admitted: count,
		Then:     stop,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("B arrived sixth with R/6, refills at R/2 for 450 ms after four quiet tokens left (-want +got):\n%s", diff)
	}
}
