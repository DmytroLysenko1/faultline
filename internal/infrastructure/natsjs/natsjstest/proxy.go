package natsjstest

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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type verdictKind uint8

const (
	forward verdictKind = iota
	drop
	loseReply
	alterReply
)

const (
	opPub  = "PUB"
	opHPub = "HPUB"
	opMsg  = "MSG"
	opHMsg = "HMSG"
	crlf   = "\r\n"
	scheme = "nats://"

	publishReplyAt = 2
	messageReplyAt = 3
	plainSizes     = 1
	headerSizes    = 2
)

type Verdict struct {
	kind  verdictKind
	field string
}

func Forward() Verdict {
	return Verdict{
		kind: forward,
	}
}

func Drop() Verdict {
	return Verdict{
		kind: drop,
	}
}

func LoseReply() Verdict {
	return Verdict{
		kind: loseReply,
	}
}

func AlterReply(field string) Verdict {
	return Verdict{
		kind:  alterReply,
		field: field,
	}
}

type Frame struct {
	Op      string
	Subject string
	Reply   string
	Payload []byte
}

type Proxy struct {
	tb       testing.TB
	listener net.Listener
	target   string
	mu       sync.Mutex
	inspect  func(f *Frame) Verdict
	answers  map[string]Verdict
	conns    []net.Conn
	closed   bool
	altered  atomic.Int32
	wg       sync.WaitGroup
}

func StartProxy(tb testing.TB, url string) *Proxy {
	tb.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(tb.Context(), "tcp", localHost+":0")
	if err != nil {
		tb.Fatalf("proxy listen: %v", err)
	}
	p := &Proxy{
		tb:       tb,
		listener: listener,
		target:   strings.TrimPrefix(url, scheme),
		answers:  map[string]Verdict{},
	}
	ctx := tb.Context()
	p.wg.Go(func() {
		p.accept(ctx)
	})
	tb.Cleanup(p.close)
	return p
}

func (p *Proxy) URL() string {
	return scheme + p.listener.Addr().String()
}

func (p *Proxy) Intercept(inspect func(f *Frame) Verdict) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inspect = inspect
}

func (p *Proxy) AnswersAltered() int {
	return int(p.altered.Load())
}

func (p *Proxy) accept(ctx context.Context) {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		if !p.relay(ctx, client) {
			return
		}
	}
}

func (p *Proxy) relay(ctx context.Context, client net.Conn) bool {
	var dialer net.Dialer
	server, err := dialer.DialContext(ctx, "tcp", p.target)
	if err != nil {
		p.closeQuietly(client)
		return true
	}
	if !p.track(client, server) {
		p.closeQuietly(client)
		p.closeQuietly(server)
		return false
	}
	p.wg.Go(func() {
		p.pump(client, server, p.toServer)
	})
	p.wg.Go(func() {
		p.pump(server, client, p.toClient)
	})
	return true
}

func (p *Proxy) track(conns ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns = append(p.conns, conns...)
	return true
}

func (p *Proxy) close() {
	p.closeQuietly(p.listener)
	p.mu.Lock()
	p.closed = true
	for _, conn := range p.conns {
		p.closeQuietly(conn)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *Proxy) closeQuietly(closer io.Closer) {
	if err := closer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		p.tb.Logf("proxy: close: %v", err)
	}
}

func (p *Proxy) pump(from, to net.Conn, judge func(f *Frame) bool) {
	defer p.closeQuietly(to)
	reader := bufio.NewReader(from)
	for {
		if err := relayFrame(reader, to, judge); err != nil {
			return
		}
	}
}

func relayFrame(reader *bufio.Reader, to io.Writer, judge func(f *Frame) bool) error {
	line, payload, f, err := readFrame(reader)
	if err != nil {
		return err
	}
	if f != nil && !judge(f) {
		return nil
	}
	_, err = to.Write(append([]byte(line), payload...))
	return err
}

func readFrame(reader *bufio.Reader) (string, []byte, *Frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, nil, err
	}
	fields := strings.Fields(line)
	if len(fields) == 0 || !carriesPayload(fields[0]) {
		return line, nil, nil, nil
	}
	payload, err := readPayload(reader, fields[len(fields)-1])
	if err != nil {
		return "", nil, nil, err
	}
	return line, payload, frameOf(fields, payload[:len(payload)-len(crlf)]), nil
}

func readPayload(reader *bufio.Reader, sizeField string) ([]byte, error) {
	size, err := strconv.Atoi(sizeField)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, size+len(crlf))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func carriesPayload(op string) bool {
	switch strings.ToUpper(op) {
	case opPub, opHPub, opMsg, opHMsg:
		return true
	default:
		return false
	}
}

func frameOf(fields []string, payload []byte) *Frame {
	op := strings.ToUpper(fields[0])
	f := &Frame{
		Op:      op,
		Subject: fields[1],
		Payload: payload,
	}
	replyAt, sizes := shapeOf(op)
	if len(fields) == replyAt+1+sizes {
		f.Reply = fields[replyAt]
	}
	return f
}

func shapeOf(op string) (replyAt, sizes int) {
	switch op {
	case opPub:
		return publishReplyAt, plainSizes
	case opHPub:
		return publishReplyAt, headerSizes
	case opMsg:
		return messageReplyAt, plainSizes
	default:
		return messageReplyAt, headerSizes
	}
}

func (p *Proxy) toServer(f *Frame) bool {
	p.mu.Lock()
	inspect := p.inspect
	p.mu.Unlock()
	if inspect == nil {
		return true
	}
	judged := inspect(f)
	if judged.kind == forward {
		return true
	}
	if judged.kind == drop {
		return false
	}
	if f.Reply == "" {
		p.tb.Errorf("proxy: %s on %s has no reply to lose or alter", f.Op, f.Subject)
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answers[f.Reply] = judged
	return true
}

func (p *Proxy) toClient(f *Frame) bool {
	p.mu.Lock()
	judged, pending := p.answers[f.Subject]
	delete(p.answers, f.Subject)
	p.mu.Unlock()
	if !pending {
		return true
	}
	if judged.kind == loseReply {
		p.altered.Add(1)
		return false
	}
	if !alterField(f.Payload, judged.field) {
		p.tb.Errorf("proxy: the answer on %s has no string field %q: %s", f.Subject, judged.field, f.Payload)
		return true
	}
	p.altered.Add(1)
	return true
}

func alterField(payload []byte, field string) bool {
	key := []byte(`"` + field + `":"`)
	start := bytes.Index(payload, key)
	if start < 0 {
		return false
	}
	value := start + len(key)
	length := bytes.IndexByte(payload[value:], '"')
	if length <= 0 {
		return false
	}
	payload[value+length-1] ^= 1
	return true
}

func JetStream(tb testing.TB, url string) jetstream.JetStream {
	tb.Helper()
	conn, err := nats.Connect(url)
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}
	tb.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}
	return js
}
