package connlimit

import (
	"net"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

type trackedAddr struct {
	net.Addr
	conn *trackedConn
}

type trackedConn struct {
	net.Conn
	owner             *Listener
	addr              net.Addr
	order             uint64
	markedUntil       time.Time
	token             credential.ID
	everAuthenticated bool
}

func (c *trackedConn) RemoteAddr() net.Addr {
	return c.addr
}

func (c *trackedConn) Close() error {
	c.owner.forget(c)
	return c.Conn.Close()
}

func (c *trackedConn) mark(id credential.ID, until time.Time) {
	c.token = id
	c.markedUntil = until
	c.everAuthenticated = true
}

func (c *trackedConn) fresh(now time.Time) bool {
	return now.Before(c.markedUntil)
}

func (c *trackedConn) lapsed(now time.Time) bool {
	return c.everAuthenticated && !c.fresh(now)
}

func acceptedEarlier(conn, than *trackedConn) bool {
	return than == nil || conn.order < than.order
}

func markedEarlier(conn, than *trackedConn) bool {
	if than == nil {
		return true
	}
	if !conn.markedUntil.Equal(than.markedUntil) {
		return conn.markedUntil.Before(than.markedUntil)
	}
	return conn.order < than.order
}
