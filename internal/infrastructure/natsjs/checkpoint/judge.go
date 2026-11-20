package checkpoint

import "time"

type verdict uint8

const (
	verdictProceed verdict = iota
	verdictAccepted
	verdictLeaseLost
	verdictForeign
	verdictRegressed
	verdictUnknown
)

func judgeBefore(found *pointer, own uint64, next *objectRef, created time.Time) verdict {
	switch {
	case foreign(found, own):
		return verdictForeign
	case found.value.Epoch > own:
		return verdictLeaseLost
	case regressed(found, next, created):
		return verdictRegressed
	default:
		return verdictProceed
	}
}

func judgeAfter(found *pointer, own uint64, next *objectRef) verdict {
	switch {
	case found == nil || found.corrupt:
		return verdictUnknown
	case found.value.Epoch > own:
		return verdictLeaseLost
	case sameObject(found.value.Current, next):
		return verdictAccepted
	default:
		return verdictUnknown
	}
}

func foreign(found *pointer, own uint64) bool {
	return found == nil || found.corrupt || found.value.Epoch < own
}

func regressed(found *pointer, next *objectRef, created time.Time) bool {
	current := found.value.Current
	return current != nil &&
		found.created.Equal(created) &&
		current.Position > next.Position
}
