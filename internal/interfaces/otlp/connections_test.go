package otlp_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/connlimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	tokenOfA          = "aaaaaaaaaaaa"
	tokenOfB          = "bbbbbbbbbbbb"
	refusedDials      = 10
	closedWithin      = 2 * time.Second
	enteredWithin     = 5 * time.Second
	legitimateWithin  = time.Second
	silentConnections = 64
	openPoll          = time.Millisecond
	openWithin        = 10 * time.Second
	limitOfAllRole    = 4
)

type wallClock struct{}

func (wallClock) Now() time.Time {
	return time.Now()
}

type connClock interface {
	Now() time.Time
}

type limitedSetup struct {
	maxConnections int
	acceptor       acceptorFunc
	keepalive      *otlp.Keepalive
	clock          connClock
}

type limitedServer struct {
	address  string
	listener *connlimit.Listener
	gate     *admission.Gate
}

func asItsID(_ context.Context, token string) (ingestauth.Principal, error) {
	parsed, err := credential.Parse(token)
	if err != nil {
		return ingestauth.Principal{}, ingestauth.RejectionError{
			Reason: ingestauth.ReasonMalformed,
		}
	}
	return ingestauth.Principal{
		TokenID: parsed.ID,
	}, nil
}

func startLimited(t *testing.T, chosen limitedSetup) limitedServer {
	t.Helper()
	inner, err := new(net.ListenConfig).Listen(t.Context(), "tcp", loopbackAddress)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	settings := options(&logBuffer{}, &fakeClock{})
	if chosen.keepalive != nil {
		settings.Keepalive = *chosen.keepalive
	}
	listener, err := connlimit.NewListener(inner, otlp.ConnectionLimits(chosen.maxConnections), chosen.clock,
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	gate := gateWith(t, otlp.AdmissionLimits(otlp.MaxRequestsPerSecond), listener)
	serveGuarded(t, listener, chosen.acceptor, otlp.Guards{
		Tap:  otlp.NewTap(otlptest.AuthenticatorFunc(asItsID), settings),
		Gate: gate,
	}, settings)
	return limitedServer{
		address:  inner.Addr().String(),
		listener: listener,
		gate:     gate,
	}
}

func clientOf(t *testing.T, address, id string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		otlptest.WithToken(tokenWith(id)),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return conn
}

func exportWithinOn(ctx context.Context, conn *grpc.ClientConn, within time.Duration, callOptions ...grpc.CallOption) error {
	bounded, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	_, err := ptraceotlp.NewGRPCClient(conn).Export(bounded, requestOf(1), callOptions...)
	return err
}

func dialTCP(t *testing.T, address string) net.Conn {
	t.Helper()
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close raw conn: %v", err)
		}
	})
	return conn
}

func dialSilent(t *testing.T, address string) net.Conn {
	t.Helper()
	conn := dialTCP(t, address)
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatalf("write preface: %v", err)
	}
	if err := http2.NewFramer(conn, conn).WriteSettings(); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	return conn
}

func closedByTheServer(t *testing.T, conn net.Conn) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(closedWithin)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	_, err := io.Copy(io.Discard, conn)
	return err == nil
}

func waitForOpen(t *testing.T, listener *connlimit.Listener, want int) {
	t.Helper()
	deadline := time.Now().Add(openWithin)
	for listener.Stats().Open != want {
		if time.Now().After(deadline) {
			t.Fatalf("%d connections open, want %d", listener.Stats().Open, want)
		}
		time.Sleep(openPoll)
	}
}

func TestAcceptNeverReturnsAnErrorForARefusal(t *testing.T) {
	type want struct {
		Closed     int
		FreshAfter codes.Code
		Stats      connlimit.Stats
	}
	server := startLimited(t, limitedSetup{
		maxConnections: 1,
		acceptor:       returning(ingest.AcceptResult{}, nil),
		clock:          &fakeClock{},
	})
	fresh := clientOf(t, server.address, tokenOfA)
	if err := exportWithinOn(t.Context(), fresh, connectDeadline); err != nil {
		t.Fatalf("the first client: %v", err)
	}
	closed := 0
	for range refusedDials {
		if closedByTheServer(t, dialTCP(t, server.address)) {
			closed++
		}
	}
	got := want{
		Closed:     closed,
		FreshAfter: status.Code(exportWithinOn(t.Context(), fresh, connectDeadline)),
		Stats:      server.listener.Stats(),
	}
	wanted := want{
		Closed:     refusedDials,
		FreshAfter: codes.OK,
		Stats: connlimit.Stats{
			Open:     1,
			Accepted: 1,
			Refused:  refusedDials,
		},
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

type parkedAcceptor struct {
	entered chan struct{}
	once    sync.Once
}

func (p *parkedAcceptor) accept(ctx context.Context, _ ingest.AcceptCommand) (ingest.AcceptResult, error) {
	p.once.Do(func() {
		close(p.entered)
	})
	<-ctx.Done()
	return ingest.AcceptResult{}, ctx.Err()
}

func TestAnEvictedConnectionReleasesItsSlot(t *testing.T) {
	type want struct {
		InUseInside   int
		Code          codes.Code
		InUseAfter    int
		EvictedLapsed uint64
	}
	clock := &fakeClock{}
	parked := &parkedAcceptor{
		entered: make(chan struct{}),
	}
	server := startLimited(t, limitedSetup{
		maxConnections: 1,
		acceptor:       parked.accept,
		clock:          clock,
	})
	evicted := clientOf(t, server.address, tokenOfA)
	codeOf := make(chan codes.Code, 1)
	go func() {
		codeOf <- status.Code(exportWithinOn(t.Context(), evicted, connectDeadline))
	}()
	select {
	case <-parked.entered:
	case <-time.After(enteredWithin):
		t.Fatal("the request never reached AcceptBatch")
	}
	inside := server.gate.Admission.Stats().InUse
	clock.Advance(otlp.MarkLifetime)
	dialTCP(t, server.address)
	got := want{
		InUseInside:   inside,
		Code:          <-codeOf,
		InUseAfter:    settledStats(server.gate).InUse,
		EvictedLapsed: server.listener.Stats().EvictedLapsed,
	}
	wanted := want{
		InUseInside:   1,
		Code:          codes.Unavailable,
		EvictedLapsed: 1,
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTheTransportClosingAnEvictedConnectionDoesNotDeadlockTheListener(t *testing.T) {
	type want struct {
		Newcomer codes.Code
		Stats    connlimit.Stats
	}
	clock := &fakeClock{}
	server := startLimited(t, limitedSetup{
		maxConnections: 1,
		acceptor:       returning(ingest.AcceptResult{}, nil),
		clock:          clock,
	})
	if err := exportWithinOn(t.Context(), clientOf(t, server.address, tokenOfA), connectDeadline); err != nil {
		t.Fatalf("the first client: %v", err)
	}
	clock.Advance(otlp.MarkLifetime)
	newcomer := clientOf(t, server.address, tokenOfB)
	code := status.Code(exportWithinOn(t.Context(), newcomer, legitimateWithin))
	if code == codes.OK {
		code = status.Code(exportWithinOn(t.Context(), newcomer, legitimateWithin))
	}
	got := want{
		Newcomer: code,
		Stats:    server.listener.Stats(),
	}
	wanted := want{
		Newcomer: codes.OK,
		Stats: connlimit.Stats{
			Open:          1,
			Accepted:      2,
			EvictedLapsed: 1,
		},
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestSixtyFourSilentConnectionsDoNotLockOutALegitimateClient(t *testing.T) {
	server := startLimited(t, limitedSetup{
		maxConnections: otlp.DefaultMaxConnections,
		acceptor:       returning(ingest.AcceptResult{}, nil),
		clock:          wallClock{},
	})
	for range silentConnections {
		dialSilent(t, server.address)
	}
	waitForOpen(t, server.listener, silentConnections)
	began := time.Now()
	err := exportWithinOn(t.Context(), clientOf(t, server.address, tokenOfA), legitimateWithin)
	took := time.Since(began)
	if err != nil {
		t.Fatalf("the legitimate client after %s: %v", took, err)
	}
	if evicted := server.listener.Stats().EvictedUnauthenticated; evicted < 1 {
		t.Errorf("evicted %d silent connections, want at least one", evicted)
	}
	t.Logf("the legitimate client was served in %s", took)
}

func TestOneTokenCannotHoldAllConnections(t *testing.T) {
	tests := []struct {
		name string
		args int
	}{
		{
			name: "the limit of all",
			args: limitOfAllRole,
		},
		{
			name: "the limit of the ingester",
			args: otlp.DefaultMaxConnections,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := startLimited(t, limitedSetup{
				maxConnections: tt.args,
				acceptor:       returning(ingest.AcceptResult{}, nil),
				clock:          wallClock{},
			})
			for index := range tt.args {
				if err := exportWithinOn(t.Context(), clientOf(t, server.address, tokenOfA), connectDeadline); err != nil {
					t.Fatalf("connection %d of A: %v", index+1, err)
				}
			}
			waitForOpen(t, server.listener, tt.args)
			began := time.Now()
			err := exportWithinOn(t.Context(), clientOf(t, server.address, tokenOfB), legitimateWithin)
			took := time.Since(began)
			if err != nil {
				t.Fatalf("B after %s: %v", took, err)
			}
			if evicted := server.listener.Stats().EvictedOverShare; evicted < 1 {
				t.Errorf("evicted %d connections of A over its share, want at least one", evicted)
			}
			t.Logf("B was served in %s", took)
		})
	}
}

func TestTheConnectionLimitsMarkForAMinuteAndLogRefusalsEveryTenSeconds(t *testing.T) {
	want := connlimit.Limits{
		MaxConnections:     4,
		MarkLifetime:       time.Minute,
		RefusalLogInterval: 10 * time.Second,
	}
	if diff := cmp.Diff(want, otlp.ConnectionLimits(4)); diff != "" {
		t.Errorf("connection limits for 4 connections (-want +got):\n%s", diff)
	}
}
