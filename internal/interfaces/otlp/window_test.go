package otlp_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	heldConnections       = 64
	heldStreams           = heldConnections * otlp.MaxConcurrentStreams
	heldBodyBytes         = 4 << 20
	heldStreamHeapBytes   = 80 << 10
	heldArrivalDeadline   = time.Minute
	heldSettle            = time.Second
	heldClientAddressEnv  = "HELD_STREAMS_SERVER"
	heldClientTest        = "-test.run=^TestAStreamBeforeItsSlotClient$"
	heldClientCallTimeout = time.Hour
	loopbackListen        = "127.0.0.1:0"
)

type blockingMarks struct {
	entered atomic.Int64
	release chan struct{}
}

func (m *blockingMarks) Authenticated(net.Addr, credential.ID) {
	m.entered.Add(1)
	<-m.release
}

func TestAStreamBeforeItsSlotCostsAtMostTheWindow(t *testing.T) {
	if os.Getenv(heldClientAddressEnv) != "" {
		t.Skip("the client process runs TestAStreamBeforeItsSlotClient only")
	}
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", loopbackListen)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	marks := &blockingMarks{
		release: make(chan struct{}),
	}
	gate := gateWith(t, otlp.AdmissionLimits(otlp.MaxRequestsPerSecond), marks)
	settings := options(&logBuffer{}, &fakeClock{})
	serveGuarded(t, listener, returning(ingest.AcceptResult{}, nil), otlp.Guards{
		Tap:  otlp.NewTap(otlptest.Admitting(), settings),
		Gate: gate,
	}, settings)
	before := liveHeap()
	client := startHeldClient(t, listener.Addr().String())
	waitForHeldStreams(t, marks)
	time.Sleep(heldSettle)
	perStream := (liveHeap() - before) / heldStreams
	stopHeldClient(t, client)
	close(marks.release)
	if perStream > heldStreamHeapBytes {
		t.Errorf("%d streams held before their slot cost %d B of live heap each, want at most %d B",
			heldStreams, perStream, heldStreamHeapBytes)
	}
}

func startHeldClient(t *testing.T, address string) *exec.Cmd {
	t.Helper()
	client := exec.CommandContext(t.Context(), os.Args[0], heldClientTest)
	client.Env = append(os.Environ(), heldClientAddressEnv+"="+address)
	if err := client.Start(); err != nil {
		t.Fatalf("start the client process: %v", err)
	}
	return client
}

func stopHeldClient(t *testing.T, client *exec.Cmd) {
	t.Helper()
	if err := client.Process.Kill(); err != nil {
		t.Errorf("kill the client process: %v", err)
	}
	if err := client.Wait(); err == nil {
		t.Errorf("the client process exited by itself")
	}
}

func waitForHeldStreams(t *testing.T, marks *blockingMarks) {
	t.Helper()
	deadline := time.Now().Add(heldArrivalDeadline)
	for marks.entered.Load() < heldStreams {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d streams reached the gate within %s", marks.entered.Load(), heldStreams, heldArrivalDeadline)
		}
		time.Sleep(releasePoll)
	}
}

func TestAStreamBeforeItsSlotClient(t *testing.T) {
	address := os.Getenv(heldClientAddressEnv)
	if address == "" {
		t.Skip("runs only as the client process of TestAStreamBeforeItsSlotCostsAtMostTheWindow")
	}
	body := &rawMessage{
		Wire: make([]byte, heldBodyBytes),
	}
	var calls sync.WaitGroup
	for range heldConnections {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), otlptest.WithTestToken())
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		for range otlp.MaxConcurrentStreams {
			calls.Go(func() {
				ctx, cancel := context.WithTimeout(t.Context(), heldClientCallTimeout)
				defer cancel()
				if err := conn.Invoke(ctx, exportMethod, body, &rawMessage{}, grpc.ForceCodecV2(rawCodec{})); err != nil {
					t.Logf("held call: %v", err)
				}
			})
		}
	}
	calls.Wait()
	t.Errorf("every held call to %s returned, want them held until the process is killed", address)
}
