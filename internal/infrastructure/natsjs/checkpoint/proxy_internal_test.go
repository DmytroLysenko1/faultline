package checkpoint

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type verdict uint8

const (
	forward verdict = iota
	drop
	loseReply
	foreignReply
)

const subjectField = `"subject":"`

type frame struct {
	op      string
	subject string
	reply   string
	payload []byte
}

// natsProxy sits between the store's client and the embedded server and speaks enough of the NATS protocol to
// drop a request, lose or alter its answer, or run a write of a second client before a request goes on.
// Callbacks run on the proxy's goroutines: they report with t.Errorf, never t.Fatalf.
type natsProxy struct {
	t        *testing.T
	listener net.Listener
	target   string
	mu       sync.Mutex
	inspect  func(f *frame) verdict
	answers  map[string]verdict
	conns    []net.Conn
	closed   bool
	altered  atomic.Int32
	wg       sync.WaitGroup
}

func startProxy(t *testing.T, target string) *natsProxy {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &natsProxy{t: t, listener: listener, target: target, answers: map[string]verdict{}}
	ctx := t.Context()
	p.wg.Go(func() { p.accept(ctx) })
	t.Cleanup(p.close)
	return p
}

func (p *natsProxy) url() string {
	return "nats://" + p.listener.Addr().String()
}

func (p *natsProxy) intercept(inspect func(f *frame) verdict) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inspect = inspect
}

func (p *natsProxy) accept(ctx context.Context) {
	var dialer net.Dialer
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := dialer.DialContext(ctx, "tcp", p.target)
		if err != nil {
			closeQuietly(client)
			continue
		}
		if !p.track(client, server) {
			closeQuietly(client)
			closeQuietly(server)
			return
		}
		p.wg.Go(func() { p.pump(client, server, p.toServer) })
		p.wg.Go(func() { p.pump(server, client, p.toClient) })
	}
}

func (p *natsProxy) track(conns ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns = append(p.conns, conns...)
	return true
}

func (p *natsProxy) close() {
	closeQuietly(p.listener)
	p.mu.Lock()
	p.closed = true
	for _, conn := range p.conns {
		closeQuietly(conn)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *natsProxy) answersAltered() int32 {
	return p.altered.Load()
}

func closeQuietly(closer io.Closer) {
	if err := closer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return
	}
}

func (p *natsProxy) pump(from, to net.Conn, judge func(f *frame) verdict) {
	defer closeQuietly(to)
	reader := bufio.NewReader(from)
	for {
		line, payload, f, err := readFrame(reader)
		if err != nil {
			return
		}
		if f != nil && judge(f) == drop {
			continue
		}
		if _, err := to.Write(append([]byte(line), payload...)); err != nil {
			return
		}
	}
}

func readFrame(reader *bufio.Reader) (string, []byte, *frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, nil, err
	}
	fields := strings.Fields(line)
	if len(fields) == 0 || !carriesPayload(fields[0]) {
		return line, nil, nil, nil
	}
	size, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil {
		return "", nil, nil, err
	}
	payload := make([]byte, size+len("\r\n"))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return "", nil, nil, err
	}
	return line, payload, frameOf(fields, payload[:size]), nil
}

func carriesPayload(op string) bool {
	switch strings.ToUpper(op) {
	case "PUB", "HPUB", "MSG", "HMSG":
		return true
	}
	return false
}

func frameOf(fields []string, payload []byte) *frame {
	op := strings.ToUpper(fields[0])
	f := &frame{op: op, subject: fields[1], payload: payload}
	withReply := map[string]int{"PUB": 4, "HPUB": 5, "MSG": 5, "HMSG": 6}
	replyAt := map[string]int{"PUB": 2, "HPUB": 2, "MSG": 3, "HMSG": 3}
	if len(fields) == withReply[op] {
		f.reply = fields[replyAt[op]]
	}
	return f
}

func (p *natsProxy) toServer(f *frame) verdict {
	p.mu.Lock()
	inspect := p.inspect
	p.mu.Unlock()
	if inspect == nil {
		return forward
	}
	judged := inspect(f)
	if judged == drop {
		return drop
	}
	if judged == forward {
		return forward
	}
	if f.reply == "" {
		p.t.Errorf("proxy: %s on %s, which has no reply to alter", f.op, f.subject)
		return forward
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answers[f.reply] = judged
	return forward
}

func (p *natsProxy) toClient(f *frame) verdict {
	p.mu.Lock()
	judged, pending := p.answers[f.subject]
	delete(p.answers, f.subject)
	p.mu.Unlock()
	if !pending {
		return forward
	}
	p.altered.Add(1)
	if judged == loseReply {
		return drop
	}
	foreignSubject(f.payload)
	return forward
}

// foreignSubject changes the last byte of the message subject in place, so the frame keeps its length.
func foreignSubject(payload []byte) {
	start := bytes.Index(payload, []byte(subjectField))
	if start < 0 {
		return
	}
	end := start + len(subjectField) + bytes.IndexByte(payload[start+len(subjectField):], '"')
	payload[end-1] ^= 1
}
