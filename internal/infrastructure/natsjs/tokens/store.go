package tokens

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	emptySubject     = 0
	noPublishRetries = 0
	scheduleSlack    = 100 * time.Millisecond
)

var (
	ErrIrreversible    = errors.New("token bucket denies purge or is sealed")
	ErrUnacceptedGrant = errors.New("token bucket keeps the grant in a form the directory refuses")
)

type Store struct {
	reader  bucketReader
	js      jetstream.JetStream
	verdict configVerdict
}

type tryFunc func(ctx context.Context, sentBefore bool) (step next, sent bool, cause error)

func OpenStore(ctx context.Context, conn *natsjs.Conn) (*Store, error) {
	nc := conn.Control.NATS
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream on the control connection: %w", err)
	}
	reader := bucketReader{
		nc:     nc,
		stream: streamName,
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	info, err := reader.info(ctx, "")
	if err != nil {
		return nil, err
	}
	return &Store{
		reader:  reader,
		js:      js,
		verdict: verdictOf(&info.Config),
	}, nil
}

func (s *Store) ConfigNotes() []string {
	return s.verdict.notes()
}

func (s *Store) Register(ctx context.Context, grant *credential.Grant) error {
	if err := s.writable(); err != nil {
		return fmt.Errorf("token bucket: %w", err)
	}
	value, err := encodeEntry(grant)
	if err != nil {
		return errors.Join(tokenadmin.ErrStoreUnavailable, err)
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	err = untilSettled(ctx, func(ctx context.Context, sentBefore bool) (next, bool, error) {
		return s.registerOnce(ctx, grant, value, sentBefore)
	})
	if err != nil {
		return fmt.Errorf("token bucket: %w", err)
	}
	return nil
}

func (s *Store) Revoke(ctx context.Context, id credential.ID) (tokenadmin.Revocation, error) {
	if s.verdict.irreversible() {
		return 0, fmt.Errorf("token bucket: %w", errors.Join(tokenadmin.ErrRevokeBlocked, ErrIrreversible))
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	var revocation tokenadmin.Revocation
	err := untilSettled(ctx, func(ctx context.Context, sentBefore bool) (next, bool, error) {
		var step next
		var sent bool
		var cause error
		revocation, step, sent, cause = s.revokeOnce(ctx, id, sentBefore)
		return step, sent, cause
	})
	if err != nil {
		return 0, fmt.Errorf("token bucket: %w", err)
	}
	return revocation, nil
}

func (s *Store) List(ctx context.Context) ([]credential.Grant, error) {
	ctx, cancel := bounded(ctx)
	defer cancel()
	entries, _, err := s.reader.readEntries(ctx)
	if err != nil {
		return nil, fmt.Errorf("token bucket: %w", errors.Join(tokenadmin.ErrStoreUnavailable, err))
	}
	grants := slices.Collect(maps.Values(entries.Grants))
	slices.SortFunc(grants, func(a, b credential.Grant) int {
		return cmp.Or(a.IssuedAt.Compare(b.IssuedAt), cmp.Compare(a.ID, b.ID))
	})
	return grants, nil
}

func (s *Store) writable() error {
	if s.verdict.irreversible() {
		return errors.Join(tokenadmin.ErrStoreUnavailable, ErrIrreversible)
	}
	if s.verdict.drifted() {
		return errors.Join(tokenadmin.ErrStoreUnavailable, ErrBucketDrift)
	}
	return nil
}

func (s *Store) registerOnce(
	ctx context.Context,
	grant *credential.Grant,
	value []byte,
	sentBefore bool,
) (next, bool, error) {
	if s.reader.nc.IsClosed() {
		return fail(endOfBudget(sentBefore)), false, nats.ErrConnectionClosed
	}
	duplicate, err := s.publish(ctx, grant.ID, value)
	attempt := publishAttempt{
		step:     registerRules(duplicate, err),
		fresh:    err == nil && !duplicate,
		inFlight: duplicate || (err != nil && !natsjs.WrongLastSequence(err)),
	}
	return provenAfterSending(s.afterPublish(ctx, grant, attempt), sentBefore), attemptSent(err), err
}

type publishAttempt struct {
	step     next
	fresh    bool
	inFlight bool
}

func (s *Store) afterPublish(ctx context.Context, grant *credential.Grant, attempt publishAttempt) next {
	if attempt.step.move != moveSettle && !attempt.fresh {
		return attempt.step
	}
	state := s.registered(ctx, grant)
	if state == keyInvalid {
		return fail(errors.Join(s.refuse(ctx, grant.ID, attempt.inFlight), ErrUnacceptedGrant))
	}
	if attempt.fresh {
		return freshSettled(state)
	}
	return registerSettled(state, attempt.step.ifAbsent)
}

func freshSettled(state keyState) next {
	if state == keyForeign {
		return fail(tokenadmin.ErrIDTaken)
	}
	return done()
}

func (s *Store) refuse(ctx context.Context, id credential.ID, inFlight bool) error {
	withdrawn := s.withdraw(ctx, id)
	if inFlight {
		return tokenadmin.ErrOutcomeUnknown
	}
	return withdrawn
}

func (s *Store) revokeOnce(
	ctx context.Context,
	id credential.ID,
	sentBefore bool,
) (tokenadmin.Revocation, next, bool, error) {
	if s.reader.nc.IsClosed() {
		return 0, fail(endOfBudget(sentBefore)), false, nats.ErrConnectionClosed
	}
	attemptCtx, cancel := context.WithTimeout(ctx, writeAttempt)
	purged, err := purge(attemptCtx, s.reader.nc, s.reader.stream, keyOf(id))
	cancel()
	revocation, step := revokeRules(purged, sentBefore, err)
	if step.move == moveSettle {
		_, state := s.stored(ctx, id)
		revocation, step = revokeSettled(state)
	}
	return revocation, step, !natsjs.Unsent(err), err
}

func (s *Store) publish(ctx context.Context, id credential.ID, value []byte) (bool, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, writeAttempt)
	defer cancel()
	ack, err := s.js.PublishMsg(attemptCtx, &nats.Msg{
		Subject: keyOf(id),
		Data:    value,
	},
		jetstream.WithMsgID(string(id)),
		jetstream.WithExpectLastSequencePerSubject(emptySubject),
		jetstream.WithExpectStream(s.reader.stream),
		jetstream.WithRetryAttempts(noPublishRetries),
	)
	if err != nil {
		return false, fmt.Errorf("publish %s: %w", id, err)
	}
	return ack.Duplicate, nil
}

func (s *Store) stored(ctx context.Context, id credential.ID) (rawEntry, keyState) {
	readCtx, cancel := context.WithTimeout(ctx, settleRead)
	defer cancel()
	entry, found, err := s.reader.lookup(readCtx, keyOf(id))
	if err != nil {
		return rawEntry{}, keyUnreadable
	}
	if !found {
		return rawEntry{}, keyAbsent
	}
	return entry, keyPresent
}

func (s *Store) registered(ctx context.Context, grant *credential.Grant) keyState {
	readCtx, cancel := context.WithTimeout(ctx, settleRead)
	defer cancel()
	stored, found, err := s.reader.readKey(readCtx, keyOf(grant.ID))
	switch {
	case errors.Is(err, errInvalidEntry):
		return keyInvalid
	case err != nil:
		return keyUnreadable
	case !found:
		return keyAbsent
	}
	return ownership(grant, &stored)
}

func (s *Store) withdraw(ctx context.Context, id credential.ID) error {
	purgeCtx, cancel := context.WithTimeout(ctx, writeAttempt)
	defer cancel()
	if _, err := purge(purgeCtx, s.reader.nc, s.reader.stream, keyOf(id)); err != nil {
		return errors.Join(tokenadmin.ErrOutcomeUnknown, err)
	}
	return tokenadmin.ErrStoreUnavailable
}

func ownership(grant, stored *credential.Grant) keyState {
	if stored.Tenant != grant.Tenant || !stored.Digest.Equal(grant.Digest) {
		return keyForeign
	}
	return keyOwn
}

type attempts struct {
	sent  bool
	cause error
}

func (a *attempts) record(sent bool, cause error) {
	a.sent = a.sent || sent
	if cause != nil {
		a.cause = cause
	}
}

func untilSettled(ctx context.Context, try tryFunc) error {
	var past attempts
	for first := true; roomFor(ctx, first); first = false {
		step, sent, cause := try(ctx, past.sent)
		past.record(sent, cause)
		if step.final() {
			return errors.Join(step.err, causeOf(step, past.cause))
		}
		if !waitForNext(ctx) {
			break
		}
	}
	return errors.Join(endOfBudget(past.sent), past.cause)
}

func causeOf(step next, cause error) error {
	if step.move == moveDone {
		return nil
	}
	return cause
}

func roomFor(ctx context.Context, first bool) bool {
	return ctx.Err() == nil && (first || roomAfter(ctx, 0))
}

func roomAfter(ctx context.Context, wait time.Duration) bool {
	deadline, _ := ctx.Deadline()
	return time.Until(deadline) >= wait+writeAttempt-scheduleSlack
}

func waitForNext(ctx context.Context) bool {
	if !roomAfter(ctx, deliveryPause) {
		return false
	}
	return retry.Sleep(ctx, deliveryPause) == nil
}

func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, limited := ctx.Deadline(); limited {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultBudget)
}
