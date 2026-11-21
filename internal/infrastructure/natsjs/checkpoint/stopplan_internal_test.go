package checkpoint

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	mebibyte     = 1 << 20
	oneMiBPerSec = mebibyte
	planSeed     = 34
	planRuns     = 20000
	planChunks   = 8
)

func millis(count int64) time.Duration {
	return time.Duration(count) * time.Millisecond
}

func unitSpeed() rates {
	return rates{
		upload: oneMiBPerSec,
		verify: oneMiBPerSec,
	}
}

func uploading(remaining time.Duration, queued bool) stopState {
	return stopState{
		phase:     phaseUpload,
		left:      mebibyte,
		size:      mebibyte,
		queued:    queued,
		final:     finalOf(queued),
		remaining: remaining,
		speed:     unitSpeed(),
	}
}

func finalOf(queued bool) int64 {
	if queued {
		return mebibyte
	}
	return 0
}

func TestThePlanAtABoundaryPrefersBothThenTheFinalThenTheCurrent(t *testing.T) {
	tests := []struct {
		name string
		args stopState
		want stopAction
	}{
		{name: "idle with nothing queued has nothing to do", args: stopState{phase: phaseIdle, speed: unitSpeed(),
			remaining: time.Hour}, want: keepCurrent},
		{name: "idle with a final that fits starts it", args: stopState{phase: phaseIdle, queued: true,
			final: mebibyte, remaining: millis(3700), speed: unitSpeed()}, want: startFinal},
		{name: "idle with a final one call short skips it", args: stopState{phase: phaseIdle, queued: true,
			final: mebibyte, remaining: millis(3600), speed: unitSpeed()}, want: skipFinal},
		{name: "idle with objects waiting skips the final whatever the time", args: stopState{phase: phaseIdle,
			queued: true, pending: true, final: mebibyte, remaining: time.Hour, speed: unitSpeed()}, want: skipFinal},
		{name: "a pointer write in flight is never dropped", args: stopState{phase: phaseCAS, queued: true,
			final: mebibyte, speed: unitSpeed()}, want: keepCurrent},
		{name: "both fit: the current goes on", args: uploading(millis(7500), true), want: keepCurrent},
		{name: "both one call short: the newer final wins", args: uploading(millis(7400), true),
			want: dropCurrent},
		{name: "only the final fits: the current is dropped", args: uploading(millis(5000), true),
			want: dropCurrent},
		{name: "the final short by the take-back: the current goes on", args: uploading(millis(4600), true),
			want: keepCurrent},
		{name: "nothing fits with a final: the current is dropped", args: uploading(millis(3400), true),
			want: dropCurrent},
		{name: "no final and the current fits", args: uploading(millis(3500), false), want: keepCurrent},
		{name: "no final and the current ends right on the cut", args: uploading(millis(3400), false),
			want: dropCurrent},
		{name: "the reconcile is priced as one call before the read", args: stopState{phase: phaseReconcile,
			size: mebibyte, remaining: millis(2300), speed: unitSpeed()}, want: keepCurrent},
		{name: "the reconcile without its call", args: stopState{phase: phaseReconcile, size: mebibyte,
			remaining: millis(2200), speed: unitSpeed()}, want: dropCurrent},
		{name: "the verify prices only what is left to read", args: stopState{phase: phaseVerify, size: mebibyte,
			left: mebibyte / 2, remaining: millis(1600), speed: unitSpeed()}, want: keepCurrent},
		{name: "the verify with no time past the cut", args: stopState{phase: phaseVerify, size: mebibyte,
			left: mebibyte / 2, remaining: millis(1500), speed: unitSpeed()}, want: dropCurrent},
		{name: "the upload with nothing left still owes the meta, the count and the read",
			args: stopState{phase: phaseUpload, size: mebibyte, remaining: millis(2500), speed: unitSpeed()},
			want: keepCurrent},
		{name: "no time at all", args: uploading(0, false), want: dropCurrent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, planAt(tt.args)); diff != "" {
				t.Errorf("planAt (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheStopBudgetsHoldTheirOwnArithmetic(t *testing.T) {
	tests := []struct {
		name string
		args bool
	}{
		{name: "one call priced at least the floor a call is let out with", args: callCost >= minCallBudget},
		{name: "the window between the cuts holds the pointer write", args: stopReserve-stopMargin >=
			callTime(casCalls)},
		{name: "the handover is the pointer write and the take-back of the previous", args: handoverCalls ==
			casCalls+takeBackCalls},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.args {
				t.Error("does not hold")
			}
		})
	}
}

func randomState(random *rand.Rand) stopState {
	size := int64(1+random.IntN(64)) * mebibyte
	return stopState{
		phase:     uploadPhase(random.IntN(int(phaseCAS) + 1)),
		left:      random.Int64N(size + 1),
		size:      size,
		queued:    random.IntN(2) == 1,
		pending:   random.IntN(4) == 0,
		final:     int64(1+random.IntN(64)) * mebibyte,
		remaining: time.Duration(random.Int64N(int64(20 * time.Second))),
		speed: rates{
			upload: int64(1+random.IntN(64)) * mebibyte,
			verify: int64(1+random.IntN(64)) * mebibyte,
		},
	}
}

func TestThePlanNeverDropsWhatItShouldKeep(t *testing.T) {
	random := rand.New(rand.NewPCG(planSeed, planSeed))
	for range planRuns {
		state := randomState(random)
		decided := planAt(state)
		if state.phase == phaseCAS && decided != keepCurrent {
			t.Fatalf("a pointer write in flight dropped: %+v", state)
		}
		if state.phase != phaseIdle && state.phase != phaseCAS && fitsBoth(state) && decided != keepCurrent {
			t.Fatalf("both fit but the current was dropped: %+v", state)
		}
		if decided == dropCurrent && state.queued && !fitsFinal(state) && fitsCurrent(state) {
			t.Fatalf("the current dropped for a final that does not fit: %+v", state)
		}
		if decided == startFinal && (state.pending || !fitsFinal(state)) {
			t.Fatalf("a final started that cannot land: %+v", state)
		}
	}
}

func TestMoreTimeNeverMakesAnUploadFitLess(t *testing.T) {
	random := rand.New(rand.NewPCG(planSeed, planSeed+1))
	for range planRuns {
		state := randomState(random)
		later := state
		later.remaining += time.Duration(random.Int64N(int64(10 * time.Second)))
		for name, fits := range map[string]func(stopState) bool{"current": fitsCurrent, "final": fitsFinal,
			"both": fitsBoth} {
			if fits(state) && !fits(later) {
				t.Fatalf("%s fits at %s but not at %s: %+v", name, state.remaining, later.remaining, state)
			}
		}
	}
}

type modelUpload struct {
	size      int64
	chunk     int64
	final     int64
	remaining time.Duration
	speed     rates
}

func (m *modelUpload) boundaries() []stopState {
	var states []stopState
	at := m.remaining
	for sent := int64(0); sent < m.size; sent += m.chunk {
		states = append(states, m.at(phaseUpload, m.size-sent, at))
		at -= transfer(min(m.chunk, m.size-sent), m.speed.upload)
	}
	states = append(states, m.at(phaseUpload, 0, at))
	at -= callTime(metaCalls)
	states = append(states, m.at(phaseReconcile, 0, at))
	at -= callTime(reconcileCalls)
	for read := int64(0); read < m.size; read += m.chunk {
		states = append(states, m.at(phaseVerify, m.size-read, at))
		at -= transfer(min(m.chunk, m.size-read), m.speed.verify)
	}
	return states
}

func (m *modelUpload) at(phase uploadPhase, left int64, remaining time.Duration) stopState {
	return stopState{
		phase:     phase,
		left:      left,
		size:      m.size,
		queued:    m.final > 0,
		final:     m.final,
		remaining: remaining,
		speed:     m.speed,
	}
}

func (m *modelUpload) idleAfterCAS(last stopState) stopState {
	return stopState{
		phase:     phaseIdle,
		queued:    m.final > 0,
		final:     m.final,
		remaining: last.remaining - transfer(last.left, m.speed.verify) - callTime(casCalls) - callTime(takeBackCalls),
		speed:     m.speed,
	}
}

func randomModel(random *rand.Rand) *modelUpload {
	size := int64(1+random.IntN(planChunks)) * mebibyte
	return &modelUpload{
		size:      size,
		chunk:     mebibyte,
		final:     int64(random.IntN(planChunks)) * mebibyte,
		remaining: time.Duration(random.Int64N(int64(30 * time.Second))),
		speed: rates{
			upload: int64(1+random.IntN(planChunks)) * mebibyte,
			verify: int64(1+random.IntN(planChunks)) * mebibyte,
		},
	}
}

func TestAtASteadySpeedAKeptUploadIsKeptAtEveryLaterBoundary(t *testing.T) {
	random := rand.New(rand.NewPCG(planSeed, planSeed+2))
	for range planRuns {
		model := randomModel(random)
		states := model.boundaries()
		for i := 1; i < len(states); i++ {
			if planAt(states[i-1]) == keepCurrent && planAt(states[i]) != keepCurrent {
				t.Fatalf("kept at boundary %d but dropped at %d: %+v then %+v", i-1, i, states[i-1], states[i])
			}
		}
	}
}

func TestBothFittingAtTheLastBoundaryStartsTheFinalInItsTurn(t *testing.T) {
	random := rand.New(rand.NewPCG(planSeed, planSeed+3))
	for range planRuns {
		model := randomModel(random)
		if model.final == 0 {
			continue
		}
		states := model.boundaries()
		last := states[len(states)-1]
		if fitsBoth(last) && planAt(model.idleAfterCAS(last)) != startFinal {
			t.Fatalf("both fit at the last boundary but the final is not started after the CAS: %+v", last)
		}
	}
}
