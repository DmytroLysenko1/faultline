package otlp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	firstStream       = 1
	defaultFrameBytes = 16 << 10
	paddingHeader     = "x-padding-"
	paddingSymbol     = "a"
	emptyGRPCMessage  = "\x00\x00\x00\x00\x00"
	outcomeAnswered   = "answered"
	outcomeClosed     = "connection closed"
	outcomeTimedOut   = "timed out"
	exchangeDeadline  = 10 * time.Second
	smallHeaders      = 10
	manyHeaders       = 20
	headerValueBytes  = 1000
)

var outcomeFrameSizeReset = "reset " + http2.ErrCodeFrameSize.String()

type transportOutcome struct {
	Advertised         uint32
	Outcome            string
	AuthenticatorCalls int64
	AcceptorCalls      int64
}

type rawClient struct {
	conn   net.Conn
	framer *http2.Framer
}

func dialRaw(t *testing.T, address string) rawClient {
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
	if err := conn.SetDeadline(time.Now().Add(exchangeDeadline)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatalf("write preface: %v", err)
	}
	client := rawClient{
		conn:   conn,
		framer: http2.NewFramer(conn, conn),
	}
	if err := client.framer.WriteSettings(); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	return client
}

func headerBlock(t *testing.T, padding padding) []byte {
	t.Helper()
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{
			Name:  ":method",
			Value: "POST",
		},
		{
			Name:  ":scheme",
			Value: "http",
		},
		{
			Name:  ":path",
			Value: exportMethod,
		},
		{
			Name:  ":authority",
			Value: "localhost",
		},
		{
			Name:  "content-type",
			Value: "application/grpc",
		},
		{
			Name:  "te",
			Value: "trailers",
		},
		{
			Name:  "authorization",
			Value: "Bearer " + otlptest.Token,
		},
	} {
		if err := encoder.WriteField(field); err != nil {
			t.Fatalf("encode %s: %v", field.Name, err)
		}
	}
	value := strings.Repeat(paddingSymbol, padding.valueBytes)
	for index := range padding.headers {
		field := hpack.HeaderField{
			Name:  paddingHeader + strconv.Itoa(index),
			Value: value,
		}
		if err := encoder.WriteField(field); err != nil {
			t.Fatalf("encode %s: %v", field.Name, err)
		}
	}
	return block.Bytes()
}

func (c rawClient) sendRequest(block []byte) error {
	first := block[:min(len(block), defaultFrameBytes)]
	rest := block[len(first):]
	err := c.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      firstStream,
		BlockFragment: first,
		EndHeaders:    len(rest) == 0,
	})
	for len(rest) > 0 && err == nil {
		fragment := rest[:min(len(rest), defaultFrameBytes)]
		rest = rest[len(fragment):]
		err = c.framer.WriteContinuation(firstStream, len(rest) == 0, fragment)
	}
	if err != nil {
		return fmt.Errorf("send headers: %w", err)
	}
	if err := c.framer.WriteData(firstStream, true, []byte(emptyGRPCMessage)); err != nil {
		return fmt.Errorf("send data: %w", err)
	}
	return nil
}

func (c rawClient) awaitOutcome(t *testing.T) (uint32, string) {
	t.Helper()
	var advertised uint32
	for {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			return advertised, readFailure(err)
		}
		switch typed := frame.(type) {
		case *http2.SettingsFrame:
			advertised = c.ackSettings(t, typed, advertised)
		case *http2.RSTStreamFrame:
			return advertised, "reset " + typed.ErrCode.String()
		case *http2.GoAwayFrame:
			return advertised, "goaway " + typed.ErrCode.String()
		case *http2.HeadersFrame:
			if typed.StreamEnded() {
				return advertised, outcomeAnswered
			}
		}
	}
}

func readFailure(err error) string {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return outcomeTimedOut
	}
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed) {
		return outcomeClosed
	}
	return "read failed: " + err.Error()
}

func (c rawClient) ackSettings(t *testing.T, frame *http2.SettingsFrame, advertised uint32) uint32 {
	t.Helper()
	if frame.IsAck() {
		return advertised
	}
	if err := c.framer.WriteSettingsAck(); err != nil {
		t.Logf("the server closed the connection before the settings ack: %v", err)
	}
	if value, ok := frame.Value(http2.SettingMaxHeaderListSize); ok {
		return value
	}
	return advertised
}

type padding struct {
	headers    int
	valueBytes int
}

func TestHugeMetadataIsRefusedByTheTransport(t *testing.T) {
	limit := otlp.MaxHeaderListBytes
	type args struct {
		padding padding
	}
	tests := []struct {
		name string
		args args
		want transportOutcome
	}{
		{
			name: "one value under the limit",
			args: args{
				padding: padding{
					headers:    1,
					valueBytes: limit / 2,
				},
			},
			want: transportOutcome{
				Advertised:         otlp.MaxHeaderListBytes,
				Outcome:            outcomeAnswered,
				AuthenticatorCalls: 1,
				AcceptorCalls:      1,
			},
		},
		{
			name: "many headers under the limit",
			args: args{
				padding: padding{
					headers:    smallHeaders,
					valueBytes: headerValueBytes,
				},
			},
			want: transportOutcome{
				Advertised:         otlp.MaxHeaderListBytes,
				Outcome:            outcomeAnswered,
				AuthenticatorCalls: 1,
				AcceptorCalls:      1,
			},
		},
		{
			name: "many headers over the limit",
			args: args{
				padding: padding{
					headers:    manyHeaders,
					valueBytes: headerValueBytes,
				},
			},
			want: transportOutcome{
				Advertised: otlp.MaxHeaderListBytes,
				Outcome:    outcomeFrameSizeReset,
			},
		},
		{
			name: "one value over the limit",
			args: args{
				padding: padding{
					headers:    1,
					valueBytes: limit + 1,
				},
			},
			want: transportOutcome{
				Advertised: otlp.MaxHeaderListBytes,
				Outcome:    outcomeClosed,
			},
		},
		{
			name: "sixteen mebibytes of metadata",
			args: args{
				padding: padding{
					headers:    1,
					valueBytes: sendLimitBytes,
				},
			},
			want: transportOutcome{
				Advertised: otlp.MaxHeaderListBytes,
				Outcome:    outcomeClosed,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls, authentications atomic.Int64
			listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", loopbackAddress)
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			settings := options(&logBuffer{}, &fakeClock{})
			counted := func(ctx context.Context, token string) (ingestauth.Principal, error) {
				authentications.Add(1)
				return otlptest.AdmitToken(ctx, token)
			}
			gate := otlp.NewTap(otlptest.AuthenticatorFunc(counted), settings)
			serveTapped(t, listener, func(context.Context, ingest.AcceptCommand) (ingest.AcceptResult, error) {
				calls.Add(1)
				return ingest.AcceptResult{}, nil
			}, gate, settings)
			client := dialRaw(t, listener.Addr().String())

			sent := client.sendRequest(headerBlock(t, tt.args.padding))
			advertised, outcome := client.awaitOutcome(t)
			if sent != nil {
				t.Logf("the server stopped reading the request: %v", sent)
			}

			got := transportOutcome{
				Advertised:         advertised,
				Outcome:            outcome,
				AuthenticatorCalls: authentications.Load(),
				AcceptorCalls:      calls.Load(),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
