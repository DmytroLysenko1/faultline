package connlimit

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

type tokenLoad struct {
	fresh  int
	first  uint64
	oldest *trackedConn
}

func (l *Listener) victim(now time.Time) *trackedConn {
	if victim := l.unauthenticatedVictim(); victim != nil {
		l.stats.EvictedUnauthenticated++
		return victim
	}
	if victim := l.lapsedVictim(now); victim != nil {
		l.stats.EvictedLapsed++
		return victim
	}
	victim := l.overShareVictim(now)
	if victim != nil {
		l.stats.EvictedOverShare++
	}
	return victim
}

func (l *Listener) unauthenticatedVictim() *trackedConn {
	var oldest *trackedConn
	for conn := range l.conns {
		if !conn.everAuthenticated && acceptedEarlier(conn, oldest) {
			oldest = conn
		}
	}
	return oldest
}

func (l *Listener) lapsedVictim(now time.Time) *trackedConn {
	var oldest *trackedConn
	for conn := range l.conns {
		if conn.lapsed(now) && markedEarlier(conn, oldest) {
			oldest = conn
		}
	}
	return oldest
}

func (l *Listener) overShareVictim(now time.Time) *trackedConn {
	loads := l.freshLoads(now)
	heaviest := heaviestToken(loads)
	if heaviest == nil || heaviest.fresh <= connShare(l.Limits.MaxConnections, len(loads)) {
		return nil
	}
	return heaviest.oldest
}

func (l *Listener) freshLoads(now time.Time) map[credential.ID]*tokenLoad {
	loads := map[credential.ID]*tokenLoad{}
	for conn := range l.conns {
		if !conn.fresh(now) {
			continue
		}
		load, seen := loads[conn.token]
		if !seen {
			load = &tokenLoad{
				first: conn.order,
			}
			loads[conn.token] = load
		}
		load.add(conn)
	}
	return loads
}

func (t *tokenLoad) add(conn *trackedConn) {
	t.fresh++
	t.first = min(t.first, conn.order)
	if markedEarlier(conn, t.oldest) {
		t.oldest = conn
	}
}

func (t *tokenLoad) yieldsBefore(other *tokenLoad) bool {
	if t.fresh != other.fresh {
		return t.fresh > other.fresh
	}
	return t.first > other.first
}

func heaviestToken(loads map[credential.ID]*tokenLoad) *tokenLoad {
	var heaviest *tokenLoad
	for _, load := range loads {
		if heaviest == nil || load.yieldsBefore(heaviest) {
			heaviest = load
		}
	}
	return heaviest
}

func connShare(maxConnections, tokens int) int {
	return max(1, maxConnections/(tokens+1))
}
