package checkpoint

import (
	"errors"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

type cycleKind uint8

const (
	kindFailed cycleKind = iota
	kindSkipped
	kindStopped
	kindTerminal
)

type cycleError struct {
	kind  cycleKind
	skip  skipReason
	fail  failReason
	cause error
}

func (e *cycleError) Error() string {
	return e.cause.Error()
}

func (e *cycleError) Unwrap() error {
	return e.cause
}

func skipped(reason skipReason, cause error) error {
	return &cycleError{
		kind:  kindSkipped,
		skip:  reason,
		cause: cause,
	}
}

func failed(reason failReason, cause error) error {
	return &cycleError{
		kind:  kindFailed,
		fail:  reason,
		cause: cause,
	}
}

func stopped(cause error) error {
	return &cycleError{
		kind:  kindStopped,
		cause: cause,
	}
}

func terminal(cause error) error {
	return &cycleError{
		kind:  kindTerminal,
		cause: cause,
	}
}

func (u *uploader) record(err error) bool {
	if err == nil {
		return false
	}
	outcome, classified := errors.AsType[*cycleError](err)
	if !classified {
		outcome = &cycleError{
			kind:  kindFailed,
			fail:  failUpload,
			cause: err,
		}
	}
	return outcome.apply(u)
}

func (e *cycleError) apply(u *uploader) bool {
	switch e.kind {
	case kindSkipped:
		u.store.stats.skip(e.skip)
	case kindFailed:
		u.store.stats.fail(e.fail, e.cause)
		u.store.stats.failUpload(e.uploadClass())
	case kindTerminal:
		return u.ended(e.cause)
	case kindStopped:
	}
	return false
}

func (u *uploader) ended(cause error) bool {
	if u.stopRequested() && (errors.Is(cause, errCASUnknown) || errors.Is(cause, errPurgeLeft)) {
		u.store.stats.leftAtStop.Add(1)
		u.store.stats.lastFailure.Store(&cause)
		return false
	}
	u.store.stats.lastFailure.Store(&cause)
	u.terminate(cause)
	return true
}

func (e *cycleError) uploadClass() uploadClass {
	if e.fail != failUpload {
		return noUploadClass
	}
	if errors.Is(e.cause, errForeignAck) {
		return classForeignStream
	}
	if class, listed := reasonClasses[natsjs.Classify(e.cause).Reason]; listed {
		return class
	}
	return classOther
}
