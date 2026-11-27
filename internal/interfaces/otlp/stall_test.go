package otlp_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/test/bufconn"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	stallHoldLimit    = 200 * time.Millisecond
	stallAnswerBound  = time.Second
	nearHoldLimit     = 600 * time.Millisecond
	nearBodyDelay     = 300 * time.Millisecond
	nearAcceptorWork  = 500 * time.Millisecond
	statusWait        = 5 * time.Second
	stalledBodyBytes  = 10
	firstStreamID     = 1
	secondStreamID    = 3
	statusOK          = "0"
	statusCancelled   = "1"
	grpcStatusHeader  = "grpc-status"
	hpackTableBytes   = 4096
	bufconnPeerString = "bufconn"
)

type answer struct {
	status string
	at     time.Time
}

type stallClient struct {
	conn    net.Conn
	framer  *http2.Framer
	writing sync.Mutex
	lock    sync.Mutex
	answers map[uint32]answer
}

func dialStalling(t *testing.T, listener *bufconn.Listener) *stallClient {
	t.Helper()
	conn, err := listener.DialContext(t.Context())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := &stallClient{
		conn:    conn,
		framer:  http2.NewFramer(conn, conn),
		answers: map[uint32]answer{},
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatalf("preface: %v", err)
	}
	if err := client.framer.WriteSettings(); err != nil {
		t.Fatalf("settings: %v", err)
	}
	var reading sync.WaitGroup
	reading.Go(client.read)
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close: %v", err)
		}
		reading.Wait()
	})
	return client
}

func (c *stallClient) read() {
	decoder := hpack.NewDecoder(hpackTableBytes, nil)
	for {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			return
		}
		c.handle(decoder, frame)
	}
}

func (c *stallClient) handle(decoder *hpack.Decoder, frame http2.Frame) {
	switch typed := frame.(type) {
	case *http2.SettingsFrame:
		if !typed.IsAck() {
			c.write(c.framer.WriteSettingsAck)
		}
	case *http2.PingFrame:
		if !typed.IsAck() {
			c.write(func() error {
				return c.framer.WritePing(true, typed.Data)
			})
		}
	case *http2.HeadersFrame:
		fields, err := decoder.DecodeFull(typed.HeaderBlockFragment())
		if err == nil {
			c.record(typed.StreamID, fields)
		}
	}
}

func (c *stallClient) record(stream uint32, fields []hpack.HeaderField) {
	for _, field := range fields {
		if field.Name != grpcStatusHeader {
			continue
		}
		c.lock.Lock()
		c.answers[stream] = answer{
			status: field.Value,
			at:     time.Now(),
		}
		c.lock.Unlock()
	}
}

func (c *stallClient) write(frame func() error) {
	c.writing.Lock()
	defer c.writing.Unlock()
	if err := frame(); err != nil {
		return
	}
}

func requestHeaders(t *testing.T) []byte {
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
			Value: bufconnPeerString,
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
			Name:  authorization,
			Value: bearerPrefix + otlptest.Token,
		},
	} {
		if err := encoder.WriteField(field); err != nil {
			t.Fatalf("encode %s: %v", field.Name, err)
		}
	}
	return block.Bytes()
}

func (c *stallClient) open(t *testing.T, stream, declared uint32, first []byte) {
	t.Helper()
	prefix := binary.BigEndian.AppendUint32([]byte{0}, declared)
	c.write(func() error {
		return c.framer.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      stream,
			BlockFragment: requestHeaders(t),
			EndHeaders:    true,
		})
	})
	c.write(func() error {
		return c.framer.WriteData(stream, false, append(prefix, first...))
	})
}

func (c *stallClient) finish(stream uint32, rest []byte) {
	c.write(func() error {
		return c.framer.WriteData(stream, true, rest)
	})
}

func (c *stallClient) await(stream uint32) (answer, bool) {
	deadline := time.Now().Add(statusWait)
	for time.Now().Before(deadline) {
		c.lock.Lock()
		found, answered := c.answers[stream]
		c.lock.Unlock()
		if answered {
			return found, true
		}
		time.Sleep(releasePoll)
	}
	return answer{}, false
}

type stallServer struct {
	listener *bufconn.Listener
	gate     *admission.Gate
	logs     *logBuffer
}

func serveStalling(t *testing.T, acceptor acceptorFunc, holdLimit time.Duration) stallServer {
	t.Helper()
	logs := &logBuffer{}
	settings := options(logs, &fakeClock{
		now: time.Unix(0, 0),
	})
	settings.SlotHoldLimit = holdLimit
	served := stallServer{
		listener: bufconn.Listen(bufferBytes),
		gate:     frozenGate(t, otlp.MaxRequestsPerSecond),
		logs:     logs,
	}
	serveGuarded(t, served.listener, acceptor, otlp.Guards{
		Tap:  otlp.NewTap(otlptest.Admitting(), settings),
		Gate: served.gate,
	}, settings)
	return served
}

func TestAStalledBodyLosesItsSlotWithinTheHoldLimit(t *testing.T) {
	served := serveStalling(t, returning(ingest.AcceptResult{}, nil), stallHoldLimit)
	client := dialStalling(t, served.listener)
	opened := time.Now()
	client.open(t, firstStreamID, stalledBodyBytes, []byte{0})
	stalled, answered := client.await(firstStreamID)
	if !answered {
		t.Fatalf("a stalled body got no status within %s", statusWait)
	}
	afterStall := settledStats(served.gate)
	client.open(t, secondStreamID, 0, nil)
	client.finish(secondStreamID, nil)
	next, _ := client.await(secondStreamID)
	type outcome struct {
		Stalled         string
		WithinBound     bool
		InUseAfterStall int
		Next            string
		Stats           admission.Stats
		TooSlowEvents   []map[string]any
	}
	want := outcome{
		Stalled:     statusCancelled,
		WithinBound: true,
		Next:        statusOK,
		Stats: admission.Stats{
			ActiveTokens: 1,
			Admitted:     2,
			TooSlow:      1,
		},
		TooSlowEvents: []map[string]any{
			{
				"level":               "WARN",
				"msg":                 eventTooSlow,
				"token_id":            otlptest.TokenID,
				"peer":                bufconnPeerString,
				"suppressed_requests": float64(0),
			},
		},
	}
	got := outcome{
		Stalled:         stalled.status,
		WithinBound:     stalled.at.Sub(opened) >= stallHoldLimit && stalled.at.Sub(opened) <= stallHoldLimit+stallAnswerBound,
		InUseAfterStall: afterStall.InUse,
		Next:            next.status,
		Stats:           settledStats(served.gate),
		TooSlowEvents:   withoutTime(served.logs.Events(t, eventTooSlow)),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("a stalled body, then a prompt one on the same connection, stall answered after %s (-want +got):\n%s",
			stalled.at.Sub(opened), diff)
	}
}

type slowAcceptor struct {
	lock     sync.Mutex
	accepted int
	doomed   int
}

func (a *slowAcceptor) accept(ctx context.Context, _ ingest.AcceptCommand) (ingest.AcceptResult, error) {
	timer := time.NewTimer(nearAcceptorWork)
	defer timer.Stop()
	select {
	case <-timer.C:
		a.count(&a.accepted)
		return ingest.AcceptResult{}, nil
	case <-ctx.Done():
		a.count(&a.doomed)
		return ingest.AcceptResult{}, ctx.Err()
	}
}

func (a *slowAcceptor) count(counter *int) {
	a.lock.Lock()
	defer a.lock.Unlock()
	*counter++
}

func (a *slowAcceptor) counts() (int, int) {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.accepted, a.doomed
}

func TestABodyJustBeforeTheHoldLimitIsAccepted(t *testing.T) {
	acceptor := &slowAcceptor{}
	served := serveStalling(t, acceptor.accept, nearHoldLimit)
	client := dialStalling(t, served.listener)
	wire, err := requestOf(1).MarshalProto()
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}
	client.open(t, firstStreamID, uint32(len(wire)), wire[:1])
	time.Sleep(nearBodyDelay)
	client.finish(firstStreamID, wire[1:])
	near, _ := client.await(firstStreamID)
	accepted, doomed := acceptor.counts()
	type outcome struct {
		Status   string
		Accepted int
		Doomed   int
		Stats    admission.Stats
	}
	want := outcome{
		Status:   statusOK,
		Accepted: 1,
		Stats: admission.Stats{
			ActiveTokens: 1,
			Admitted:     1,
		},
	}
	got := outcome{
		Status:   near.status,
		Accepted: accepted,
		Doomed:   doomed,
		Stats:    settledStats(served.gate),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("a body done %s before the hold limit, the acceptor busy %s (-want +got):\n%s",
			nearHoldLimit-nearBodyDelay, nearAcceptorWork, diff)
	}
}
