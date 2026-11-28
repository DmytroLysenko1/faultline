package otlp_test

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	connFloodEnv        = "TEST_CONN_FLOOD"
	floodTargetEnv      = "CONN_FLOOD_TARGET"
	floodRateEnv        = "CONN_FLOOD_RATE"
	floodClientTest     = "-test.run=^TestAConnectionFloodClient$"
	floodDuration       = 5 * time.Second
	floodWarmUp         = 500 * time.Millisecond
	floodRate           = 500
	connectedExportStep = 250 * time.Millisecond
	reconnectWithin     = 2 * time.Second
	reconnectingClients = 3
	floodDialTimeout    = time.Second
)

func floodGated(t *testing.T) {
	t.Helper()
	if os.Getenv(floodTargetEnv) != "" {
		t.Skip("the flood process runs TestAConnectionFloodClient only")
	}
	if os.Getenv(connFloodEnv) == "" {
		t.Skip("set " + connFloodEnv + "=1 to run the connection flood gates (make test-stress)")
	}
}

func startFlood(t *testing.T, address string, rate int) {
	t.Helper()
	flood := exec.CommandContext(t.Context(), os.Args[0], floodClientTest)
	flood.Env = append(os.Environ(), floodTargetEnv+"="+address, floodRateEnv+"="+strconv.Itoa(rate))
	if err := flood.Start(); err != nil {
		t.Fatalf("start the flood process: %v", err)
	}
	t.Cleanup(func() {
		if err := flood.Process.Kill(); err != nil {
			t.Errorf("kill the flood process: %v", err)
		}
		if err := flood.Wait(); err == nil {
			t.Errorf("the flood process exited by itself")
		}
	})
	time.Sleep(floodWarmUp)
}

func TestAConnectionFloodClient(t *testing.T) {
	address := os.Getenv(floodTargetEnv)
	if address == "" {
		t.Skip("runs only as the flood process of the connection flood gates")
	}
	rate, err := strconv.Atoi(os.Getenv(floodRateEnv))
	if err != nil || rate < 1 {
		t.Fatalf("%s %q: %v", floodRateEnv, os.Getenv(floodRateEnv), err)
	}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()
	var dials sync.WaitGroup
	defer dials.Wait()
	for {
		select {
		case <-t.Context().Done():
			return
		case <-ticker.C:
			dials.Go(func() {
				floodOnce(t.Context(), address)
			})
		}
	}
}

func floodOnce(ctx context.Context, address string) {
	dialer := net.Dialer{
		Timeout: floodDialTimeout,
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			return
		}
	}()
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		return
	}
	if err := http2.NewFramer(conn, conn).WriteSettings(); err != nil {
		return
	}
	if _, err := io.Copy(io.Discard, conn); err != nil {
		return
	}
}

func delayedDialer(delay time.Duration) grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		time.Sleep(delay)
		return conn, nil
	})
}

func TestAConnectionFloodDoesNotDisturbAConnectedClient(t *testing.T) {
	floodGated(t)
	server := startLimited(t, limitedSetup{
		maxConnections: otlp.DefaultMaxConnections,
		acceptor:       returning(ingest.AcceptResult{}, nil),
		clock:          wallClock{},
	})
	connected := clientOf(t, server.address, tokenOfA)
	if err := exportWithinOn(t.Context(), connected, connectDeadline); err != nil {
		t.Fatalf("the connected client before the flood: %v", err)
	}
	startFlood(t, server.address, floodRate)
	refused, sent := 0, 0
	for deadline := time.Now().Add(floodDuration); time.Now().Before(deadline); time.Sleep(connectedExportStep) {
		sent++
		if err := exportWithinOn(t.Context(), connected, connectDeadline); err != nil {
			refused++
			t.Logf("export %d: %v", sent, err)
		}
	}
	if refused != 0 {
		t.Errorf("the connected client was refused %d of %d exports during the flood", refused, sent)
	}
	t.Logf("%d exports, connections %+v", sent, server.listener.Stats())
}

func TestALegitimateClientReconnectsDuringAConnectionFlood(t *testing.T) {
	floodGated(t)
	type args struct {
		maxConnections int
		rate           int
		firstStream    time.Duration
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "ingester, 500 conn/s, no delay",
			args: args{otlp.DefaultMaxConnections, floodRate, 0},
		},
		{
			name: "ingester, 500 conn/s, 5 ms to the first stream",
			args: args{otlp.DefaultMaxConnections, floodRate, 5 * time.Millisecond},
		},
		{
			name: "ingester, 500 conn/s, 20 ms to the first stream",
			args: args{otlp.DefaultMaxConnections, floodRate, 20 * time.Millisecond},
		},
		{
			name: "all, 500 conn/s, no delay",
			args: args{limitOfAllRole, floodRate, 0},
		},
		{
			name: "all, 100 conn/s, 20 ms to the first stream",
			args: args{limitOfAllRole, 100, 20 * time.Millisecond},
		},
		{
			name: "all, 50 conn/s, 50 ms to the first stream",
			args: args{limitOfAllRole, 50, 50 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := startLimited(t, limitedSetup{
				maxConnections: tt.args.maxConnections,
				acceptor:       returning(ingest.AcceptResult{}, nil),
				clock:          wallClock{},
			})
			startFlood(t, server.address, tt.args.rate)
			for index := range reconnectingClients {
				took, err := reconnect(t, server.address, tt.args.firstStream)
				if err != nil {
					t.Errorf("client %d not served within %s: %v", index+1, reconnectWithin, err)
					continue
				}
				t.Logf("client %d served in %s", index+1, took)
			}
		})
	}
}

func reconnect(t *testing.T, address string, firstStream time.Duration) (time.Duration, error) {
	t.Helper()
	client, err := grpc.NewClient("passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		otlptest.WithToken(tokenWith(tokenOfA)),
		delayedDialer(firstStream),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close the reconnecting client: %v", err)
		}
	}()
	began := time.Now()
	err = exportWithinOn(t.Context(), client, reconnectWithin, grpc.WaitForReady(true))
	return time.Since(began), err
}
