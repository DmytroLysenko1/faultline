package natsjs_test

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	authUser     = "faultline"
	authPassword = "secret"
	shortConnect = 500 * time.Millisecond
)

type classified struct {
	Reason      natsjs.DialReason
	Unreachable bool
}

func reachableAs(reason natsjs.DialReason) classified {
	return classified{
		Reason:      reason,
		Unreachable: true,
	}
}

func opError(op string, err error) error {
	return fmt.Errorf("connect publish: %w", &net.OpError{
		Op:  op,
		Net: "tcp",
		Err: err,
	})
}

func TestUnreachableClassifies(t *testing.T) {
	tests := []struct {
		name string
		args error
		want classified
	}{
		{
			name: "a name that does not resolve",
			args: opError("dial", &net.DNSError{
				Err:        "no such host",
				Name:       "nats.invalid",
				IsNotFound: true,
			}),
			want: reachableAs(natsjs.DialDNS),
		},
		{
			name: "a refused port on every url",
			args: fmt.Errorf("connect publish: %w", nats.ErrNoServers),
			want: reachableAs(natsjs.DialNoServers),
		},
		{
			name: "a reset after accept, a sidecar without its upstream",
			args: opError("read", os.NewSyscallError("read", syscall.ECONNRESET)),
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "a broken pipe",
			args: opError("write", os.NewSyscallError("write", syscall.EPIPE)),
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "a dial that times out",
			args: opError("dial", os.ErrDeadlineExceeded),
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "an unroutable host",
			args: opError("dial", os.NewSyscallError("connect", syscall.EHOSTUNREACH)),
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "the client timeout",
			args: fmt.Errorf("connect publish: %w", nats.ErrTimeout),
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "the attempt deadline",
			args: fmt.Errorf("connect feed: %w", context.DeadlineExceeded),
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "a silent port",
			args: opError("read", os.ErrDeadlineExceeded),
			want: reachableAs(natsjs.DialNoNATSGreeting),
		},
		{
			name: "a port that closes without a greeting",
			args: fmt.Errorf("connect publish: %w", io.EOF),
			want: reachableAs(natsjs.DialNoNATSGreeting),
		},
		{
			name: "a bare EOF of handshake first",
			args: io.EOF,
			want: reachableAs(natsjs.DialNoNATSGreeting),
		},
		{
			name: "an authorization violation",
			args: fmt.Errorf("connect publish: %w", nats.ErrAuthorization),
		},
		{
			name: "an unknown certificate authority",
			args: fmt.Errorf("connect publish: %w", x509.UnknownAuthorityError{}),
		},
		{
			name: "a server that wants TLS",
			args: fmt.Errorf("connect publish: %w", nats.ErrSecureConnRequired),
		},
		{
			name: "a signal",
			args: fmt.Errorf("connect publish: %w", context.Canceled),
		},
		{
			name: "a cancel joined with a closed greeting",
			args: errors.Join(io.EOF, context.Canceled),
		},
		{
			name: "no error",
		},
		{
			name: "a payload limit below the record",
			args: natsjs.ErrPayloadLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, unreachable := natsjs.Unreachable(tt.args)

			got := classified{
				Reason:      reason,
				Unreachable: unreachable,
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unreachable(%v) (-want +got):\n%s", tt.args, diff)
			}
		})
	}
}

func authServer(t *testing.T) string {
	t.Helper()
	cluster := natsjstest.Start(t, singleNode, func(options *server.Options) {
		options.Username = authUser
		options.Password = authPassword
	})
	return cluster.URLs()[0]
}

func quickOptions(urls ...string) *natsjs.ConnOptions {
	options := optionsFor(urls)
	options.ConnectTimeout = shortConnect
	return options
}

func TestUnreachableClassifiesWhatDialMeets(t *testing.T) {
	tests := []struct {
		name string
		args func(t *testing.T) string
		want classified
	}{
		{
			name: "a refused port",
			args: func(t *testing.T) string { return natsjstest.RefusedURL(t) },
			want: reachableAs(natsjs.DialNoServers),
		},
		{
			name: "a silent port",
			args: func(t *testing.T) string { return natsjstest.SilentEndpoint(t).URL },
			want: reachableAs(natsjs.DialNoNATSGreeting),
		},
		{
			name: "a port that closes at once",
			args: func(t *testing.T) string { return natsjstest.ClosingEndpoint(t).URL },
			want: reachableAs(natsjs.DialNoNATSGreeting),
		},
		{
			name: "a port that resets at once",
			args: func(t *testing.T) string { return natsjstest.ResettingEndpoint(t).URL },
			want: reachableAs(natsjs.DialNetwork),
		},
		{
			name: "a server that refuses our credentials",
			args: authServer,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := natsjs.Dial(t.Context(), quickOptions(tt.args(t)))
			if err == nil {
				t.Fatal("Dial succeeded")
			}

			reason, unreachable := natsjs.Unreachable(err)

			got := classified{
				Reason:      reason,
				Unreachable: unreachable,
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unreachable(%v) (-want +got):\n%s", err, diff)
			}
		})
	}
}

func TestUnreachableIsNotACanceledDial(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := natsjs.Dial(ctx, quickOptions(natsjstest.SilentEndpoint(t).URL))

	if _, unreachable := natsjs.Unreachable(err); unreachable || !errors.Is(err, context.Canceled) {
		t.Errorf("Dial on a canceled context = %v, unreachable %t; want the cancel, not a class", err, unreachable)
	}
}
