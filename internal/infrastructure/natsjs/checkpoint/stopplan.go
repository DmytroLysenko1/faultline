package checkpoint

import "time"

type uploadPhase uint8

const (
	phaseIdle uploadPhase = iota
	phaseUpload
	phaseReconcile
	phaseVerify
	phaseCAS
)

type stopAction uint8

const (
	keepCurrent stopAction = iota
	dropCurrent
	startFinal
	skipFinal
)

const (
	callCost        = 200 * time.Millisecond
	metaCalls       = 1
	reconcileCalls  = 1
	casCalls        = 2
	takeBackCalls   = 5
	readyCalls      = 1
	uploadTailCalls = metaCalls + reconcileCalls
	handoverCalls   = casCalls + takeBackCalls
)

type rates struct {
	upload int64
	verify int64
}

type stopState struct {
	phase     uploadPhase
	left      int64
	size      int64
	queued    bool
	pending   bool
	final     int64
	remaining time.Duration
	speed     rates
}

func planAt(s stopState) stopAction {
	switch s.phase {
	case phaseIdle:
		return planIdle(s)
	case phaseCAS:
		return keepCurrent
	default:
		return planRunning(s)
	}
}

func planIdle(s stopState) stopAction {
	switch {
	case !s.queued:
		return keepCurrent
	case !s.pending && fitsFinal(s):
		return startFinal
	default:
		return skipFinal
	}
}

func planRunning(s stopState) stopAction {
	if s.queued && !fitsBoth(s) && fitsFinal(s) {
		return dropCurrent
	}
	return keepOr(fitsCurrent(s))
}

func keepOr(fits bool) stopAction {
	if fits {
		return keepCurrent
	}
	return dropCurrent
}

func preCAS(s stopState) time.Duration {
	switch s.phase {
	case phaseUpload:
		return transfer(s.left, s.speed.upload) + callTime(uploadTailCalls) + transfer(s.size, s.speed.verify)
	case phaseReconcile:
		return callTime(reconcileCalls) + transfer(s.size, s.speed.verify)
	case phaseVerify:
		return transfer(s.left, s.speed.verify)
	default:
		return 0
	}
}

func callTime(count int) time.Duration {
	return time.Duration(count) * callCost
}

func budget(s stopState) time.Duration {
	return s.remaining - stopReserve
}

func fitsCurrent(s stopState) bool {
	return preCAS(s) < budget(s)
}

func fitsFinal(s stopState) bool {
	return dropCost(s)+callTime(readyCalls)+preCAS(finalUpload(s)) < budget(s)
}

func finalUpload(s stopState) stopState {
	return stopState{
		phase: phaseUpload,
		left:  s.final,
		size:  s.final,
		speed: s.speed,
	}
}

func dropCost(s stopState) time.Duration {
	if s.phase == phaseIdle {
		return 0
	}
	return callTime(takeBackCalls)
}

func idleAfter(s stopState) stopState {
	return stopState{
		phase:     phaseIdle,
		queued:    s.queued,
		final:     s.final,
		speed:     s.speed,
		remaining: s.remaining - preCAS(s) - callTime(handoverCalls),
	}
}

func fitsBoth(s stopState) bool {
	return fitsCurrent(s) && fitsFinal(idleAfter(s))
}
