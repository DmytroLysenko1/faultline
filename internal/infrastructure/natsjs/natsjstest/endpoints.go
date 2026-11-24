package natsjstest

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	natsScheme   = "nats://"
	resetLinger  = 0
	tcpNetwork   = "tcp"
	anyLocalPort = "127.0.0.1:0"
)

type answer func(conn net.Conn) bool

type Endpoint struct {
	URL      string
	accepted atomic.Int64
}

func (e *Endpoint) Accepted() int64 {
	return e.accepted.Load()
}

func SilentEndpoint(tb testing.TB) *Endpoint {
	tb.Helper()
	return serveEndpoint(tb, holdOpen)
}

func ClosingEndpoint(tb testing.TB) *Endpoint {
	tb.Helper()
	return serveEndpoint(tb, closeAtOnce)
}

func ResettingEndpoint(tb testing.TB) *Endpoint {
	tb.Helper()
	return serveEndpoint(tb, resetAtOnce)
}

func FlappingEndpoint(tb testing.TB) *Endpoint {
	tb.Helper()
	var answered atomic.Int64
	return serveEndpoint(tb, func(conn net.Conn) bool {
		if answered.Add(1)%2 == 1 {
			return closeAtOnce(conn)
		}
		return resetAtOnce(conn)
	})
}

func RefusedURL(tb testing.TB) string {
	tb.Helper()
	listener := listenLocal(tb)
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		tb.Fatalf("close the refused port: %v", err)
	}
	return natsScheme + address
}

func serveEndpoint(tb testing.TB, reply answer) *Endpoint {
	tb.Helper()
	listener := listenLocal(tb)
	endpoint := &Endpoint{
		URL: natsScheme + listener.Addr().String(),
	}
	var served sync.WaitGroup
	served.Go(func() {
		endpoint.accept(tb, listener, reply)
	})
	tb.Cleanup(func() {
		if err := listener.Close(); err != nil {
			tb.Errorf("close the endpoint: %v", err)
		}
		served.Wait()
	})
	return endpoint
}

func (e *Endpoint) accept(tb testing.TB, listener net.Listener, reply answer) {
	var held []net.Conn
	defer closeHeld(tb, &held)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		e.accepted.Add(1)
		if reply(conn) {
			held = append(held, conn)
		}
	}
}

func closeHeld(tb testing.TB, held *[]net.Conn) {
	for _, conn := range *held {
		closeQuietly(tb, conn)
	}
}

func listenLocal(tb testing.TB) net.Listener {
	tb.Helper()
	listener, err := new(net.ListenConfig).Listen(tb.Context(), tcpNetwork, anyLocalPort)
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	return listener
}

func holdOpen(net.Conn) bool {
	return true
}

func closeAtOnce(conn net.Conn) bool {
	return conn.Close() != nil
}

func resetAtOnce(conn net.Conn) bool {
	tcp, isTCP := conn.(*net.TCPConn)
	if isTCP {
		if err := tcp.SetLinger(resetLinger); err != nil {
			return true
		}
	}
	return conn.Close() != nil
}

func closeQuietly(tb testing.TB, conn net.Conn) {
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		tb.Errorf("close a held connection: %v", err)
	}
}

func StartWithoutJetStream(tb testing.TB) string {
	tb.Helper()
	running, err := server.NewServer(&server.Options{
		Host:       localHost,
		Port:       anyPort,
		MaxPayload: MaxPayload,
		NoSigs:     true,
		NoLog:      true,
	})
	if err != nil {
		tb.Fatalf("nats server without JetStream: %v", err)
	}
	go running.Start()
	tb.Cleanup(func() {
		running.Shutdown()
		running.WaitForShutdown()
	})
	if !running.ReadyForConnections(readyTimeout) {
		tb.Fatalf("nats server without JetStream not ready after %s", readyTimeout)
	}
	return running.ClientURL()
}
