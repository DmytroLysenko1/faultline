package natsjstest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"slices"
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
	answerReply
	holdFrame
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
	sidAt          = 2
	space          = " "
)

type Verdict struct {
	kind    verdictKind
	field   string
	payload []byte
	held    *HeldFrame
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

func Answer(payload []byte) Verdict {
	return Verdict{
		kind:    answerReply,
		payload: slices.Clone(payload),
	}
}

func Hold() (Verdict, *HeldFrame) {
	held := &HeldFrame{
		release:  make(chan struct{}),
		answered: make(chan struct{}),
	}
	return Verdict{
		kind: holdFrame,
		held: held,
	}, held
}

type HeldFrame struct {
	release   chan struct{}
	released  sync.Once
	answered  chan struct{}
	answering sync.Once
	answer    []byte
}

func (h *HeldFrame) Release() {
	h.released.Do(func() {
		close(h.release)
	})
}

func (h *HeldFrame) Answered() <-chan struct{} {
	return h.answered
}

func (h *HeldFrame) Answer() []byte {
	return h.answer
}

type Frame struct {
	Op      string
	Subject string
	Reply   string
	Payload []byte
}

type relay struct {
	pass        bool
	replaced    bool
	replacement []byte
	held        *HeldFrame
}

func passed() relay {
	return relay{pass: true}
}

func (r relay) wire(line string, payload []byte, f *Frame) []byte {
	if !r.replaced {
		return append([]byte(line), payload...)
	}
	fields := []string{opMsg, f.Subject, strings.Fields(line)[sidAt]}
	if f.Reply != "" {
		fields = append(fields, f.Reply)
	}
	fields = append(fields, strconv.Itoa(len(r.replacement)))
	return slices.Concat([]byte(strings.Join(fields, space)+crlf), r.replacement, []byte(crlf))
}

type Proxy struct {
	tb       testing.TB
	listener net.Listener
	target   string
	done     chan struct{}
	mu       sync.Mutex
	inspect  func(f *Frame) Verdict
	withhold func(f *Frame) bool
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
		done:     make(chan struct{}),
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

func (p *Proxy) Withhold(withhold func(f *Frame) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.withhold = withhold
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
	toServer := &lockedConn{
		Conn: server,
	}
	p.wg.Go(func() {
		p.pump(client, toServer, p.passToServer)
	})
	p.wg.Go(func() {
		p.pump(server, &lockedConn{
			Conn: client,
		}, p.passToClient)
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
	if !p.closed {
		close(p.done)
	}
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

func (p *Proxy) passToServer(f *Frame) relay {
	p.mu.Lock()
	inspect := p.inspect
	p.mu.Unlock()
	if inspect == nil {
		return passed()
	}
	judged := inspect(f)
	if judged.kind == holdFrame {
		p.awaitAnswer(f, judged)
		return relay{
			pass: true,
			held: judged.held,
		}
	}
	return relay{
		pass: p.toServer(f, judged),
	}
}

func (p *Proxy) awaitAnswer(f *Frame, judged Verdict) {
	if f.Reply == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answers[f.Reply] = judged
}

func (p *Proxy) toServer(f *Frame, judged Verdict) bool {
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

func (p *Proxy) pendingAnswer(f *Frame) (Verdict, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	judged, pending := p.answers[f.Subject]
	delete(p.answers, f.Subject)
	return judged, pending
}

func (p *Proxy) withheld(f *Frame) bool {
	p.mu.Lock()
	withhold := p.withhold
	p.mu.Unlock()
	return withhold != nil && withhold(f)
}

func (p *Proxy) passToClient(f *Frame) relay {
	if p.withheld(f) {
		return relay{}
	}
	return p.toClient(f)
}

func (p *Proxy) toClient(f *Frame) relay {
	judged, pending := p.pendingAnswer(f)
	if !pending {
		return passed()
	}
	switch judged.kind {
	case holdFrame:
		judged.held.answering.Do(func() {
			judged.held.answer = slices.Clone(f.Payload)
			close(judged.held.answered)
		})
		return passed()
	case loseReply:
		p.altered.Add(1)
		return relay{}
	case answerReply:
		p.altered.Add(1)
		return relay{
			pass:        true,
			replaced:    true,
			replacement: judged.payload,
		}
	default:
		return p.alteredAnswer(f, judged.field)
	}
}

func (p *Proxy) alteredAnswer(f *Frame, field string) relay {
	if !alterField(f.Payload, field) {
		p.tb.Errorf("proxy: the answer on %s has no string field %q: %s", f.Subject, field, f.Payload)
		return passed()
	}
	p.altered.Add(1)
	return passed()
}

func (p *Proxy) pump(from net.Conn, to *lockedConn, judge func(f *Frame) relay) {
	defer p.closeQuietly(to.Conn)
	reader := bufio.NewReader(from)
	for {
		if err := p.relayFrame(reader, to, judge); err != nil {
			return
		}
	}
}

func (p *Proxy) relayFrame(reader *bufio.Reader, to io.Writer, judge func(f *Frame) relay) error {
	line, payload, f, err := readFrame(reader)
	if err != nil {
		return err
	}
	decided := passed()
	if f != nil {
		decided = judge(f)
	}
	if decided.held != nil {
		p.park(decided.held, to, decided.wire(line, payload, f))
		return nil
	}
	if !decided.pass {
		return nil
	}
	_, err = to.Write(decided.wire(line, payload, f))
	return err
}

func (p *Proxy) park(held *HeldFrame, to io.Writer, frame []byte) {
	p.wg.Go(func() {
		select {
		case <-held.release:
		case <-p.done:
			return
		}
		if _, err := to.Write(frame); err != nil && !errors.Is(err, net.ErrClosed) {
			p.tb.Logf("proxy: write a released frame: %v", err)
		}
	})
}

type lockedConn struct {
	net.Conn
	mu sync.Mutex
}

func (c *lockedConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.Write(b)
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
