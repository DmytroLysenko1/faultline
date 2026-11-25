package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	layerConn     = "conn"
	componentConn = "conn"
	stageConnDial = "conn/dial"

	eventNATSUnreachable = "nats.unreachable"
	eventStillWaiting    = "lifecycle.still_waiting"

	connCloseBudget    = 500 * time.Millisecond
	dialPause          = 2 * time.Second
	waitReportPeriod   = 30 * time.Second
	connectionsPerDial = 3
	connectionsHeap    = mib
)

var errConnectionClosed = errors.New("nats connection closed outside its stop")

type phaseLog interface {
	transition(next lifecycleState, reason string)
}

type connUnit struct {
	options  *natsjs.ConnOptions
	phases   phaseLog
	logger   *slog.Logger
	conn     *natsjs.Conn
	stopping chan struct{}
	claimed  atomic.Bool
}

func connLayer(cfg *natsConfig, phases phaseLog, logger *slog.Logger) (layer, *connUnit) {
	connector := &connUnit{
		options: cfg.connOptions(logger),
		phases:  phases,
		logger:  logger,
	}
	return layer{
		name:       layerConn,
		unit:       connector,
		components: []string{componentConn},
		stopBudget: connCloseBudget,
		contributions: []contribution{
			{
				name:  "nats connections",
				bytes: connectionsHeap,
			},
		},
	}, connector
}

func (u *connUnit) start(ctx context.Context, sup unitSupervisor) error {
	u.phases.transition(stateConnecting, reasonDialing)
	conn, err := u.dial(ctx)
	if err != nil {
		return &stageError{
			stage: stageConnDial,
			err:   err,
		}
	}
	u.conn = conn
	u.stopping = make(chan struct{})
	sup.spawn(componentConn, u.watch)
	return nil
}

func (u *connUnit) watch() error {
	closed := u.awaitClose()
	if closed == nil {
		return nil
	}
	select {
	case <-u.stopping:
		return nil
	default:
	}
	if !u.claimed.CompareAndSwap(false, true) {
		<-u.stopping
		return nil
	}
	return closedCause(closed)
}

func (u *connUnit) awaitClose() *natsjs.Link {
	select {
	case <-u.stopping:
		return nil
	case <-u.conn.Publish.Closed:
		return u.conn.Publish
	case <-u.conn.Feed.Closed:
		return u.conn.Feed
	case <-u.conn.Control.Closed:
		return u.conn.Control
	}
}

func (u *connUnit) closedBefore() *natsjs.Link {
	for _, link := range []*natsjs.Link{u.conn.Publish, u.conn.Feed, u.conn.Control} {
		if link.NATS.IsClosed() {
			return link
		}
	}
	return nil
}

func (u *connUnit) claimClosedBefore() *natsjs.Link {
	closed := u.closedBefore()
	if closed == nil || !u.claimed.CompareAndSwap(false, true) {
		return nil
	}
	return closed
}

func (u *connUnit) stop(ctx context.Context) error {
	if u.conn == nil {
		return nil
	}
	closed := u.claimClosedBefore()
	close(u.stopping)
	err := u.conn.Close(ctx)
	if closed == nil {
		return err
	}
	return errors.Join(closedCause(closed), err)
}

func (u *connUnit) conns() *natsjs.Conn {
	return u.conn
}

func (u *connUnit) sever(ctx context.Context) error {
	closed := u.claimClosedBefore()
	u.claimed.Store(true)
	err := u.conn.Close(ctx)
	if closed == nil {
		return err
	}
	return errors.Join(closedCause(closed), err)
}

func (u *connUnit) dial(ctx context.Context) (*natsjs.Conn, error) {
	wait := newDialWait(time.Now())
	for {
		conn, err := u.attempt(ctx)
		if err == nil {
			return conn, nil
		}
		if err := u.awaitRetry(ctx, wait, err); err != nil {
			return nil, err
		}
	}
}

func (u *connUnit) attempt(ctx context.Context) (*natsjs.Conn, error) {
	budget := connectionsPerDial * u.options.ConnectTimeout * time.Duration(len(u.options.URLs))
	attemptCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return natsjs.Dial(attemptCtx, u.options)
}

func (u *connUnit) awaitRetry(ctx context.Context, wait *dialWait, err error) error {
	reason, unreachable := natsjs.Unreachable(err)
	if !unreachable {
		return err
	}
	u.warnOnce(ctx, wait, reason, err)
	retryNow, passErr := u.passIfDue(ctx, wait)
	if passErr != nil {
		return passErr
	}
	wait.remind(ctx, u.logger)
	if retryNow {
		return nil
	}
	return pause(ctx, dialPause)
}

func (u *connUnit) passIfDue(ctx context.Context, wait *dialWait) (bool, error) {
	if time.Now().Before(wait.nextPass) {
		return false, nil
	}
	failures, err := natsjs.DialEach(ctx, u.options)
	if err != nil {
		return false, err
	}
	wait.passed(time.Now(), failures)
	if refused, found := lo.Find(failures, notWaitable); found {
		return false, fmt.Errorf("nats %s: %w", refused.Host, refused.Err)
	}
	return len(failures) < len(u.options.URLs), nil
}

func (u *connUnit) warnOnce(
	ctx context.Context,
	wait *dialWait,
	reason natsjs.DialReason,
	err error,
) {
	wait.current = reason
	wait.lastError = err
	if wait.warned[reason] {
		return
	}
	wait.warned[reason] = true
	u.logger.WarnContext(ctx, eventNATSUnreachable, "reason", string(reason), "last_error", err)
}

func closedCause(link *natsjs.Link) error {
	cause := link.NATS.LastError()
	if cause == nil {
		return fmt.Errorf("%w: %s", errConnectionClosed, link.NATS.Opts.Name)
	}
	return fmt.Errorf("%w: %s: %w", errConnectionClosed, link.NATS.Opts.Name, cause)
}

func notWaitable(failure natsjs.URLFailure) bool {
	return !failure.Waitable()
}

type dialWait struct {
	reminder
	warned    map[natsjs.DialReason]bool
	current   natsjs.DialReason
	lastError error
	nextPass  time.Time
	perURL    []natsjs.URLFailure
}

func newDialWait(now time.Time) *dialWait {
	wait := &dialWait{
		warned:   map[natsjs.DialReason]bool{},
		nextPass: now,
	}
	wait.reminder = reminder{
		state:  stateConnecting,
		level:  slog.LevelWarn,
		began:  now,
		next:   now.Add(waitReportPeriod),
		detail: wait.details,
	}
	return wait
}

func (w *dialWait) passed(now time.Time, failures []natsjs.URLFailure) {
	w.nextPass = now.Add(waitReportPeriod)
	w.perURL = failures
}

func (w *dialWait) details() []any {
	return []any{
		"reason", string(w.current),
		"last_error", w.lastError,
		"seen_reasons", slices.Sorted(maps.Keys(w.warned)),
		"per_url", lo.Map(w.perURL, urlEntry),
	}
}

type reminder struct {
	state  lifecycleState
	level  slog.Level
	began  time.Time
	next   time.Time
	detail func() []any
}

func (r *reminder) remind(ctx context.Context, logger *slog.Logger) {
	now := time.Now()
	if now.Before(r.next) {
		return
	}
	r.next = now.Add(waitReportPeriod)
	attrs := append([]any{
		"state", string(r.state),
		slog.Float64("elapsed_seconds", now.Sub(r.began).Seconds()),
	}, r.detail()...)
	logger.Log(ctx, r.level, eventStillWaiting, attrs...)
}

func pause(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func urlEntry(failure natsjs.URLFailure, _ int) map[string]string {
	return map[string]string{
		"host":   failure.Host,
		"reason": string(failure.Reason),
	}
}
