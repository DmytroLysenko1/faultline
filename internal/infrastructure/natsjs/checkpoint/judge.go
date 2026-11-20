package checkpoint

import "time"

type verdict uint8

const (
	verdictProceed verdict = iota
	verdictAccepted
	verdictLeaseLost
	verdictForeign
	verdictRegressed
	verdictRetry
	verdictFence
	verdictPending
	verdictPurge
	verdictReferenced
)

type casStep uint8

const (
	stepUpdate casStep = iota
	stepRetry
	stepFence
	stepExhausted
)

var nextAfter = [...]verdict{
	stepUpdate:    verdictRetry,
	stepRetry:     verdictFence,
	stepFence:     verdictPending,
	stepExhausted: verdictPending,
}

func judgeBefore(found *pointer, own uint64, next *objectRef, created time.Time) verdict {
	if decided, settled := judgeEpoch(found, own); settled {
		return decided
	}
	if regressed(found, next, created) {
		return verdictRegressed
	}
	return verdictProceed
}

func judgeAfter(found *pointer, own uint64, next *objectRef, step casStep) verdict {
	if decided, settled := judgeEpoch(found, own); settled {
		return decided
	}
	if sameObject(found.value.Current, next) {
		return verdictAccepted
	}
	return nextAfter[step]
}

func judgePending(found *pointer, own uint64, item *pendingObject) verdict {
	if decided, settled := judgeEpoch(found, own); settled {
		return decided
	}
	switch {
	case sameObject(found.value.Current, &item.ref):
		return verdictAccepted
	case sameObject(found.value.Previous, &item.ref):
		return verdictReferenced
	case item.unwritable(found.revision):
		return verdictPurge
	default:
		return verdictFence
	}
}

func judgeEpoch(found *pointer, own uint64) (verdict, bool) {
	switch {
	case foreign(found, own):
		return verdictForeign, true
	case found.value.Epoch > own:
		return verdictLeaseLost, true
	default:
		return verdictProceed, false
	}
}

func foreign(found *pointer, own uint64) bool {
	return found == nil ||
		found.corrupt ||
		found.value.Epoch < own
}

func regressed(found *pointer, next *objectRef, created time.Time) bool {
	current := found.value.Current
	return current != nil &&
		found.created.Equal(created) &&
		current.Position > next.Position
}
