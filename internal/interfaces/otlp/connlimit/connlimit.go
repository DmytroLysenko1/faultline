package connlimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/throttle"
)

const (
	eventRefused     = "otlp.connections.refused"
	eventCloseFailed = "otlp.connections.close_failed"
)

var ErrInvalidLimits = errors.New("invalid connection limits")

type clock interface {
	Now() time.Time
}

type admittance struct {
	tracked *trackedConn
	evicted *trackedConn
	open    int
}

type Limits struct {
	MaxConnections     int
	MarkLifetime       time.Duration
	RefusalLogInterval time.Duration
}

func (l Limits) validate() error {
	if l.MaxConnections < 1 || l.MarkLifetime <= 0 || l.RefusalLogInterval <= 0 {
		return fmt.Errorf("max connections %d, mark lifetime %s, refusal log interval %s: %w",
			l.MaxConnections, l.MarkLifetime, l.RefusalLogInterval, ErrInvalidLimits)
	}
	return nil
}

type Stats struct {
	Open                   int
	Accepted               uint64
	EvictedUnauthenticated uint64
	EvictedLapsed          uint64
	EvictedOverShare       uint64
	Refused                uint64
}

type Listener struct {
	Inner    net.Listener
	Limits   Limits
	Clock    clock
	Logger   *slog.Logger
	refusals *throttle.Throttle
	lock     sync.Mutex
	conns    map[*trackedConn]struct{}
	stats    Stats
}

func NewListener(
	inner net.Listener,
	limits Limits,
	clock clock,
	logger *slog.Logger,
) (*Listener, error) {
	if inner == nil || clock == nil || logger == nil {
		return nil, fmt.Errorf("listener, clock or logger missing: %w", ErrInvalidLimits)
	}
	if err := limits.validate(); err != nil {
		return nil, err
	}
	return &Listener{
		Inner:    inner,
		Limits:   limits,
		Clock:    clock,
		Logger:   logger,
		refusals: throttle.New(limits.RefusalLogInterval, clock),
		conns:    map[*trackedConn]struct{}{},
	}, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Inner.Accept()
		if err != nil {
			return nil, err
		}
		if tracked, admitted := l.admit(conn); admitted {
			return tracked, nil
		}
	}
}

func (l *Listener) Close() error {
	return l.Inner.Close()
}

func (l *Listener) Addr() net.Addr {
	return l.Inner.Addr()
}

func (l *Listener) Authenticated(remote net.Addr, id credential.ID) {
	addr, tracked := remote.(trackedAddr)
	if !tracked || addr.conn.owner != l {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	addr.conn.mark(id, l.Clock.Now().Add(l.Limits.MarkLifetime))
}

func (l *Listener) Stats() Stats {
	l.lock.Lock()
	defer l.lock.Unlock()
	stats := l.stats
	stats.Open = len(l.conns)
	return stats
}

func (l *Listener) admit(conn net.Conn) (*trackedConn, bool) {
	admitted := l.register(conn)
	if admitted.tracked == nil {
		peer := conn.RemoteAddr().String()
		l.closeLogged(conn)
		l.warnRefused(peer, admitted.open)
		return nil, false
	}
	if admitted.evicted != nil {
		l.closeLogged(admitted.evicted.Conn)
	}
	return admitted.tracked, true
}

func (l *Listener) register(conn net.Conn) admittance {
	l.lock.Lock()
	defer l.lock.Unlock()
	now := l.Clock.Now()
	evicted, room := l.makeRoom(now)
	if !room {
		l.stats.Refused++
		return admittance{
			open: len(l.conns),
		}
	}
	return admittance{
		tracked: l.track(conn),
		evicted: evicted,
		open:    len(l.conns),
	}
}

func (l *Listener) makeRoom(now time.Time) (*trackedConn, bool) {
	if len(l.conns) < l.Limits.MaxConnections {
		return nil, true
	}
	evicted := l.victim(now)
	if evicted == nil {
		return nil, false
	}
	delete(l.conns, evicted)
	return evicted, true
}

func (l *Listener) track(conn net.Conn) *trackedConn {
	l.stats.Accepted++
	tracked := &trackedConn{
		Conn:  conn,
		owner: l,
		order: l.stats.Accepted,
	}
	tracked.addr = trackedAddr{
		Addr: conn.RemoteAddr(),
		conn: tracked,
	}
	l.conns[tracked] = struct{}{}
	return tracked
}

func (l *Listener) forget(conn *trackedConn) {
	l.lock.Lock()
	defer l.lock.Unlock()
	delete(l.conns, conn)
}

func (l *Listener) warnRefused(peer string, open int) {
	suppressed, admitted := l.refusals.Admit(0)
	if !admitted {
		return
	}
	l.Logger.LogAttrs(context.Background(), slog.LevelWarn, eventRefused,
		slog.String("peer", peer),
		slog.Int("open", open),
		slog.Int("max_connections", l.Limits.MaxConnections),
		slog.Int64("suppressed_connections", suppressed.Events),
	)
}

func (l *Listener) closeLogged(conn net.Conn) {
	if err := conn.Close(); err != nil {
		l.Logger.LogAttrs(context.Background(), slog.LevelDebug, eventCloseFailed,
			slog.String("peer", conn.RemoteAddr().String()),
			slog.Any("error", err),
		)
	}
}
