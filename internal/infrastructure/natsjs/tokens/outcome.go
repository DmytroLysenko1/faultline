package tokens

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	writeAttempt  = 2 * time.Second
	settleRead    = time.Second
	deliveryPause = time.Second
	defaultBudget = 7 * time.Second
)

const (
	codeNotLeader jetstream.ErrorCode = 10009
	codeQueueFull jetstream.ErrorCode = 10167
)

type move uint8

const (
	moveDone move = iota
	moveRetry
	moveSettle
	moveFail
)

type keyState uint8

const (
	keyAbsent keyState = iota
	keyPresent
	keyOwn
	keyForeign
	keyInvalid
	keyUnreadable
)

type next struct {
	move     move
	err      error
	ifAbsent move
}

func (n next) final() bool {
	return n.move == moveDone || n.move == moveFail
}

func done() next {
	return next{move: moveDone}
}

func retryLater() next {
	return next{move: moveRetry}
}

func fail(err error) next {
	return next{move: moveFail, err: err}
}

func settle(ifAbsent move) next {
	return next{move: moveSettle, ifAbsent: ifAbsent}
}

var registerReasons = map[natsjs.Reason]next{
	natsjs.ReasonStreamFull:    fail(tokenadmin.ErrTokenLimit),
	natsjs.ReasonTooLarge:      fail(tokenadmin.ErrStoreUnavailable),
	natsjs.ReasonSealed:        fail(tokenadmin.ErrStoreUnavailable),
	natsjs.ReasonAccountLimits: fail(tokenadmin.ErrStoreUnavailable),
	natsjs.ReasonTimeout:       settle(moveRetry),
	natsjs.ReasonDisconnected:  settle(moveRetry),
	natsjs.ReasonInProcess:     settle(moveRetry),
	natsjs.ReasonNoResponders:  settle(moveRetry),
}

var revokeReasons = map[natsjs.Reason]next{
	natsjs.ReasonSealed:       fail(tokenadmin.ErrRevokeBlocked),
	natsjs.ReasonTimeout:      settle(moveRetry),
	natsjs.ReasonDisconnected: settle(moveRetry),
	natsjs.ReasonNoResponders: settle(moveRetry),
}

func registerRules(duplicate bool, err error) next {
	if err != nil {
		return registerError(err)
	}
	if duplicate {
		return settle(moveFail)
	}
	return done()
}

func registerError(err error) next {
	if natsjs.WrongLastSequence(err) {
		return settle(moveRetry)
	}
	if notStored(err) {
		return retryLater()
	}
	return byReason(registerReasons, err)
}

func attemptSent(err error) bool {
	return !notStored(err)
}

func notStored(err error) bool {
	return natsjs.Unsent(err) ||
		natsjs.HasCode(err, codeNotLeader) ||
		natsjs.HasCode(err, codeQueueFull)
}

func byReason(rules map[natsjs.Reason]next, err error) next {
	if rule, found := rules[natsjs.Classify(err).Reason]; found {
		return rule
	}
	return fail(tokenadmin.ErrOutcomeUnknown)
}

func registerSettled(state keyState, ifAbsent move) next {
	switch state {
	case keyOwn:
		return done()
	case keyForeign:
		return fail(tokenadmin.ErrIDTaken)
	case keyAbsent, keyPresent, keyInvalid, keyUnreadable:
	}
	if ifAbsent == moveFail {
		return fail(tokenadmin.ErrOutcomeUnknown)
	}
	return retryLater()
}

func provenAfterSending(step next, sentBefore bool) next {
	if step.move != moveFail || !sentBefore || !provesNoRecord(step.err) {
		return step
	}
	if errors.Is(step.err, ErrUnacceptedGrant) {
		return fail(errors.Join(tokenadmin.ErrOutcomeUnknown, ErrUnacceptedGrant))
	}
	return fail(tokenadmin.ErrOutcomeUnknown)
}

func provesNoRecord(err error) bool {
	return errors.Is(err, tokenadmin.ErrTokenLimit) || errors.Is(err, tokenadmin.ErrStoreUnavailable)
}

func revokeRules(purged uint64, sentBefore bool, err error) (tokenadmin.Revocation, next) {
	if err != nil {
		return 0, revokeError(err)
	}
	return revocationOf(purged, sentBefore), done()
}

func revocationOf(purged uint64, sentBefore bool) tokenadmin.Revocation {
	if purged > 0 {
		return tokenadmin.Revoked
	}
	if sentBefore {
		return tokenadmin.Absent
	}
	return tokenadmin.NothingToRevoke
}

func revokeError(err error) next {
	if natsjs.PurgeDenied(err) {
		return fail(tokenadmin.ErrRevokeBlocked)
	}
	if natsjs.HasCode(err, jetstream.JSErrCodeStreamNotFound) {
		return fail(tokenadmin.ErrStoreUnavailable)
	}
	if natsjs.Unsent(err) {
		return retryLater()
	}
	return byReason(revokeReasons, err)
}

func revokeSettled(state keyState) (tokenadmin.Revocation, next) {
	if state == keyAbsent {
		return tokenadmin.Absent, done()
	}
	return 0, retryLater()
}

func endOfBudget(sent bool) error {
	if sent {
		return tokenadmin.ErrOutcomeUnknown
	}
	return tokenadmin.ErrStoreUnavailable
}
