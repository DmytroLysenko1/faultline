package feed_test

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	pullSubjectPrefix = "$JS.API.CONSUMER.MSG.NEXT."
	subjectSeparator  = "."
	// a heartbeat of 5 s, in the nanoseconds of the pull request's JSON
	fiveSecondHeartbeat = `"idle_heartbeat":5000000000`
)

// pulls is what the proxy saw of the feed's pull requests: the consumer each asked and its body.
type pulls struct {
	mu        sync.Mutex
	consumers []string
	bodies    [][]byte
}

func (p *pulls) seen(f *natsjstest.Frame) {
	if !strings.HasPrefix(f.Subject, pullSubjectPrefix) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.consumers = append(p.consumers, f.Subject[strings.LastIndex(f.Subject, subjectSeparator)+1:])
	p.bodies = append(p.bodies, bytes.Clone(f.Payload))
}

func (p *pulls) last() (string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.consumers) == 0 {
		return "", 0
	}
	return p.consumers[len(p.consumers)-1], len(p.consumers)
}

func (p *pulls) withoutHeartbeat() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	var missing [][]byte
	for _, body := range p.bodies {
		if !bytes.Contains(body, []byte(fiveSecondHeartbeat)) {
			missing = append(missing, body)
		}
	}
	return missing
}

// A pull that asks for no heartbeat leaves a consumer lost with its node unnoticed until the pull expires, so
// every pull the feed sends asks for one, a recreated consumer's too.
func TestEveryPullAsksForAHeartbeatEvenAfterTheConsumerIsRecreated(t *testing.T) {
	l := startLive(t, singleNode, oneReplica)
	proxy := natsjstest.StartProxy(t, l.cluster.URLs()[0])
	seen := &pulls{}
	proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		seen.seen(f)
		return natsjstest.Forward()
	})
	behind := dialAt(t, proxy.URL())
	opened, err := feed.Open(t.Context(), behind, newHeldLease(), feed.Start{}, optionsOf(l.topology))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { shut(t, opened) })

	settled(t, "a first pull", func() bool { _, count := seen.last(); return count > 0 })
	first, _ := seen.last()
	if err := l.raw.DeleteConsumer(t.Context(), l.stream(), first); err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}
	settled(t, "a pull of the recreated consumer", func() bool { name, _ := seen.last(); return name != first })

	if missing := seen.withoutHeartbeat(); len(missing) != 0 {
		t.Errorf("pulls without %s: %q", fiveSecondHeartbeat, missing)
	}
}

func dialAt(t *testing.T, url string) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), &natsjs.ConnOptions{
		URLs: []string{url}, Name: t.Name(), ConnectTimeout: connectTimeout, DrainTimeout: drainTimeout,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { closeConn(t, conn) })
	return conn
}
