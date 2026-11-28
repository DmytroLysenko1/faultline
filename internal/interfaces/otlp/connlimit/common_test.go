package connlimit_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/connlimit"
)

const (
	markLifetime  = 60 * time.Second
	lockProbeWait = time.Second
	eventRefused  = "otlp.connections.refused"
	newcomerName  = "newcomer"
	refusedClass  = "refused"
	noEviction    = "none"
)

var errDrained = errors.New("no connection is queued")

type fakeClock struct {
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{
		now: time.Unix(1_000_000, 0),
	}
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(step time.Duration) {
	c.now = c.now.Add(step)
}

type fakeAddr string

func (a fakeAddr) Network() string {
	return "tcp"
}

func (a fakeAddr) String() string {
	return string(a)
}

type fakeConn struct {
	net.Conn
	name     string
	remote   fakeAddr
	closeErr error
	onClose  func()
	closes   int
}

func (c *fakeConn) RemoteAddr() net.Addr {
	return c.remote
}

func (c *fakeConn) Close() error {
	c.closes++
	if c.onClose != nil {
		c.onClose()
	}
	return c.closeErr
}

type queuedListener struct {
	queue  []net.Conn
	closes int
}

func (q *queuedListener) Accept() (net.Conn, error) {
	if len(q.queue) == 0 {
		return nil, errDrained
	}
	next := q.queue[0]
	q.queue = q.queue[1:]
	return next, nil
}

func (q *queuedListener) Close() error {
	q.closes++
	return nil
}

func (q *queuedListener) Addr() net.Addr {
	return fakeAddr("127.0.0.1:4317")
}

type lockedBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
	probe  func() bool
	locked int
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	if b.probe != nil && !b.probe() {
		b.locked++
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) events(t *testing.T, name string) []map[string]any {
	t.Helper()
	b.lock.Lock()
	defer b.lock.Unlock()
	var matched []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(b.buffer.Bytes()))
	for scanner.Scan() {
		record := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, scanner.Text())
		}
		if record["msg"] == name {
			matched = append(matched, record)
		}
	}
	return matched
}

type fixture struct {
	t        *testing.T
	clock    *fakeClock
	base     time.Time
	inner    *queuedListener
	logs     *lockedBuffer
	listener *connlimit.Listener
	offered  int
	conns    map[string]*fakeConn
	accepted map[string]net.Conn
	locked   int
}

func newFixture(t *testing.T, maxConnections int) *fixture {
	t.Helper()
	f := &fixture{
		t:        t,
		clock:    newClock(),
		inner:    &queuedListener{},
		logs:     &lockedBuffer{},
		conns:    map[string]*fakeConn{},
		accepted: map[string]net.Conn{},
	}
	logger := slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	listener, err := connlimit.NewListener(f.inner, connlimit.Limits{
		MaxConnections:     maxConnections,
		MarkLifetime:       markLifetime,
		RefusalLogInterval: warnInterval,
	}, f.clock, logger)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	f.base = f.clock.now
	f.listener = listener
	f.logs.probe = f.lockFree
	return f
}

func (f *fixture) lockFree() bool {
	if f.locked > 0 || f.logs.locked > 0 {
		return false
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.listener.Stats()
	}()
	select {
	case <-done:
		return true
	case <-time.After(lockProbeWait):
		return false
	}
}

func (f *fixture) offer(name string) net.Conn {
	f.t.Helper()
	f.offered++
	return f.offerFrom(name, fakeAddr(fmt.Sprintf("10.0.%d.%d:%d", f.offered/250, f.offered%250, 40000+f.offered)))
}

func (f *fixture) offerFrom(name string, remote fakeAddr) net.Conn {
	f.t.Helper()
	raw := &fakeConn{
		name:   name,
		remote: remote,
	}
	raw.onClose = func() {
		if !f.lockFree() {
			f.locked++
		}
	}
	f.conns[name] = raw
	f.inner.queue = append(f.inner.queue, raw)
	accepted, err := f.listener.Accept()
	if errors.Is(err, errDrained) {
		return nil
	}
	if err != nil {
		f.t.Fatalf("Accept %s: %v", name, err)
	}
	f.accepted[name] = accepted
	return accepted
}

func (f *fixture) mark(name string, id credential.ID) {
	f.t.Helper()
	accepted, found := f.accepted[name]
	if !found {
		f.t.Fatalf("%s was never accepted", name)
	}
	f.listener.Authenticated(accepted.RemoteAddr(), id)
}

func (f *fixture) closedByTheListener() []string {
	var closed []string
	for name, raw := range f.conns {
		if raw.closes > 0 {
			closed = append(closed, name)
		}
	}
	return closed
}

func (f *fixture) open(names []string) int {
	count := 0
	for _, name := range names {
		if f.conns[name].closes == 0 {
			count++
		}
	}
	return count
}

func classOf(before, after connlimit.Stats) string {
	switch {
	case after.EvictedUnauthenticated > before.EvictedUnauthenticated:
		return "unauthenticated"
	case after.EvictedLapsed > before.EvictedLapsed:
		return "lapsed"
	case after.EvictedOverShare > before.EvictedOverShare:
		return "over share"
	case after.Refused > before.Refused:
		return refusedClass
	}
	return noEviction
}

func sorted(names []string) []string {
	slices.Sort(names)
	return names
}
