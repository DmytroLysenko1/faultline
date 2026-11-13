package natsjs_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats-server/v2/server"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

func TestDialOpensThreeNamedConnectionsWithoutAReconnectBuffer(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)

	conn := dial(t, cluster)

	if !conn.Connected() {
		t.Fatal("a fresh connection reports itself disconnected")
	}
	var names []string
	for _, link := range []*natsjs.Link{conn.Publish, conn.Feed, conn.Control} {
		options := link.NATS.Opts
		if options.ReconnectBufSize != -1 {
			t.Errorf("%s keeps a reconnect buffer of %d bytes: a publish could reach the server after the retry",
				options.Name, options.ReconnectBufSize)
		}
		names = append(names, options.Name)
	}
	want := []string{connName + "-publish", connName + "-feed", connName + "-control"}
	if diff := cmp.Diff(want, names); diff != "" {
		t.Errorf("connection names (-want +got):\n%s", diff)
	}
}

// TestDialRefusesAServerThatCannotTakeTheLargestRecordWithItsHeaders: one byte short of the guard
// plus the header budget and a record at the guard would fail on every retry, so the ingester must
// not start.
func TestDialRefusesAServerThatCannotTakeTheLargestRecordWithItsHeaders(t *testing.T) {
	tests := []struct {
		name    string
		args    int32
		wantErr error
	}{
		{name: "the default 1 MiB", args: 1 << 20, wantErr: natsjs.ErrPayloadLimit},
		{name: "one byte short", args: record.MaxRecordBytes + natsjs.HeaderBudget - 1, wantErr: natsjs.ErrPayloadLimit},
		{name: "exactly enough", args: record.MaxRecordBytes + natsjs.HeaderBudget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNode, func(options *server.Options) { options.MaxPayload = tt.args })

			conn, err := natsjs.Dial(t.Context(), optionsFor(cluster.URLs()))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Dial = %v, want %v", err, tt.wantErr)
			}
			if conn != nil {
				closeConn(t, conn)
			}
		})
	}
}

func TestDialRefusesOptionsItCannotKeep(t *testing.T) {
	tests := []struct {
		name string
		args func(*natsjs.ConnOptions)
	}{
		{name: "no urls", args: func(o *natsjs.ConnOptions) { o.URLs = nil }},
		{name: "no name", args: func(o *natsjs.ConnOptions) { o.Name = "" }},
		{name: "no connect timeout", args: func(o *natsjs.ConnOptions) { o.ConnectTimeout = 0 }},
		{name: "no drain timeout — in-flight answers would be cut at once", args: func(o *natsjs.ConnOptions) {
			o.DrainTimeout = 0
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := optionsFor([]string{"nats://127.0.0.1:1"})
			tt.args(options)

			_, err := natsjs.Dial(t.Context(), options)

			if !errors.Is(err, natsjs.ErrInvalidConnOptions) {
				t.Errorf("Dial = %v, want ErrInvalidConnOptions", err)
			}
		})
	}
	if _, err := natsjs.Dial(t.Context(), nil); !errors.Is(err, natsjs.ErrInvalidConnOptions) {
		t.Errorf("Dial(nil) = %v, want ErrInvalidConnOptions", err)
	}
}

func TestDialToNobodyFails(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	urls := cluster.URLs()
	cluster.Stop(0)

	_, err := natsjs.Dial(t.Context(), optionsFor(urls))

	if err == nil {
		t.Error("Dial to a stopped server succeeded")
	}
}

// TestDialStaysWithinItsDeadline: every server of a dead list gets only its share of what is left.
func TestDialStaysWithinItsDeadline(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	urls := cluster.URLs()
	cluster.Stop(0)
	dead := []string{urls[0], urls[0], urls[0]}
	ctx, cancel := context.WithTimeout(t.Context(), drainTimeout)
	defer cancel()
	started := time.Now()

	_, err := natsjs.Dial(ctx, optionsFor(dead))

	if err == nil {
		t.Fatal("Dial to a dead list succeeded")
	}
	if took := time.Since(started); took > drainTimeout+connectTimeout/2 {
		t.Errorf("Dial took %s with a %s deadline", took, drainTimeout)
	}
	expired, cancelExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if _, err := natsjs.Dial(expired, optionsFor(urls)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Dial with an expired deadline = %v, want DeadlineExceeded", err)
	}
}

// TestCloseEndsEveryConnectionWithoutCountingAnOutage: a deploy closes every connection; that is not
// a disconnect, or every deploy would page.
func TestCloseEndsEveryConnectionWithoutCountingAnOutage(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	conn, err := natsjs.Dial(t.Context(), optionsFor(cluster.URLs()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	closeConn(t, conn)

	for _, link := range []*natsjs.Link{conn.Publish, conn.Feed, conn.Control} {
		if !link.NATS.IsClosed() || link.Disconnects.Load() != 0 {
			t.Errorf("%s: closed %t, disconnects %d; want closed and none", link.NATS.Opts.Name, link.NATS.IsClosed(),
				link.Disconnects.Load())
		}
	}
	if conn.Connected() {
		t.Error("a closed connection reports itself connected")
	}
}

func TestALostServerIsCountedAsADisconnectAndItsReturnAsAReconnect(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	conn := dial(t, cluster)

	cluster.Stop(0)
	eventually(t, "the publish connection counts the lost server", func() bool {
		return conn.Publish.Disconnects.Load() > 0
	})
	cluster.Restart(0)

	eventually(t, "the publish connection counts its return", func() bool {
		return conn.Publish.Reconnects.Load() > 0 && conn.Connected()
	})
}

// silentServer accepts connections and never says a word, the way a hung or firewalled server
// looks to a client waiting for its INFO.
func silentServer(t *testing.T) string {
	t.Helper()
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var held sync.WaitGroup
	held.Go(func() { holdConnections(t, listener) })
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
		held.Wait()
	})
	return "nats://" + listener.Addr().String()
}

func holdConnections(t *testing.T, listener net.Listener) {
	var accepted []net.Conn
	defer func() {
		for _, conn := range accepted {
			if err := conn.Close(); err != nil {
				t.Errorf("close a held connection: %v", err)
			}
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted = append(accepted, conn)
	}
}

// TestDialSharesItsDeadlineAmongSilentServers: three servers that never answer must not cost three
// full connect timeouts each; Dial ends with its deadline.
func TestDialSharesItsDeadlineAmongSilentServers(t *testing.T) {
	silent := silentServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), drainTimeout)
	defer cancel()
	started := time.Now()

	_, err := natsjs.Dial(ctx, optionsFor([]string{silent, silent, silent}))

	if err == nil {
		t.Fatal("Dial to silent servers succeeded")
	}
	if took := time.Since(started); took > 2*drainTimeout {
		t.Errorf("Dial took %s with a %s deadline", took, drainTimeout)
	}
}

// TestDialGivesUpWhenItsCallerCancels: a cancel without a deadline must end Dial too, though
// nats.Connect itself cannot be interrupted.
func TestDialGivesUpWhenItsCallerCancels(t *testing.T) {
	silent := silentServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	stop := time.AfterFunc(drainTimeout/2, cancel)
	defer stop.Stop()
	started := time.Now()

	_, err := natsjs.Dial(ctx, optionsFor([]string{silent}))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial = %v, want the cancel", err)
	}
	if took := time.Since(started); took > drainTimeout {
		t.Errorf("Dial returned %s after a cancel at %s", took, drainTimeout/2)
	}
}
