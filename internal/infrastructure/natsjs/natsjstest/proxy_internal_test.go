package natsjstest

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
)

const (
	smallBuffer    = 16
	headers        = "NATS/1.0\r\n\r\n"
	body           = "hi"
	serviceSubject = "svc"
	streamAnswer   = `{"stream":"ABC"}`
	alteredAnswer  = `{"stream":"ABB"}`
	replacedAnswer = `{"error":{"code":503,"err_code":10077,"description":"maximum bytes exceeded"}}`
	requestWait    = 300 * time.Millisecond
	oneNode        = 1
)

type relayed struct {
	Frame   *Frame
	Written string
}

// A frame is read whole however TCP splits it, and goes on byte for byte.
func TestAFrameArrivingByteByByteIsParsedAndRelayedUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    *Frame
		wantErr error
	}{
		{name: "a line without a payload", args: "PING\r\n"},
		{
			name: "a publish without a reply",
			args: "PUB a 2\r\n" + body + "\r\n",
			want: &Frame{Op: opPub, Subject: "a", Payload: []byte(body)},
		},
		{
			name: "a publish with a reply",
			args: "PUB a r 2\r\n" + body + "\r\n",
			want: &Frame{Op: opPub, Subject: "a", Reply: "r", Payload: []byte(body)},
		},
		{
			name: "a publish with headers and a reply",
			args: "HPUB a r 12 14\r\n" + headers + body + "\r\n",
			want: &Frame{Op: opHPub, Subject: "a", Reply: "r", Payload: []byte(headers + body)},
		},
		{
			name: "a message with a reply",
			args: "MSG a 1 r 2\r\n" + body + "\r\n",
			want: &Frame{Op: opMsg, Subject: "a", Reply: "r", Payload: []byte(body)},
		},
		{
			name: "a message with headers and no reply",
			args: "HMSG a 1 12 14\r\n" + headers + body + "\r\n",
			want: &Frame{Op: opHMsg, Subject: "a", Payload: []byte(headers + body)},
		},
		{
			name: "a message with headers and a reply",
			args: "HMSG a 1 r 12 14\r\n" + headers + body + "\r\n",
			want: &Frame{Op: opHMsg, Subject: "a", Reply: "r", Payload: []byte(headers + body)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReaderSize(iotest.OneByteReader(strings.NewReader(tt.args)), smallBuffer)
			var written bytes.Buffer
			var seen *Frame

			err := relayFrame(reader, &written, func(f *Frame) relay {
				seen = f
				return passed()
			})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			want := relayed{Frame: tt.want, Written: tt.args}
			if diff := cmp.Diff(want, relayed{Frame: seen, Written: written.String()}); diff != "" {
				t.Errorf("relayed mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type altered struct {
	Payload string
	Altered bool
}

func TestAlterFieldFlipsOnlyTheLastByteOfAStringValue(t *testing.T) {
	tests := []struct {
		name string
		args string
		want altered
	}{
		{name: "the field is there", args: streamAnswer, want: altered{Payload: alteredAnswer, Altered: true}},
		{name: "no such field", args: `{"subject":"ABC"}`, want: altered{Payload: `{"subject":"ABC"}`}},
		{name: "an empty value", args: `{"stream":""}`, want: altered{Payload: `{"stream":""}`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := []byte(tt.args)

			ok := alterField(payload, "stream")

			if diff := cmp.Diff(tt.want, altered{Payload: string(payload), Altered: ok}); diff != "" {
				t.Errorf("alterField mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type outcome struct {
	Answer  string
	Served  int32
	Altered int
}

// Each verdict on a request through the proxy: what reached the server, what came back, what was counted.
func TestEachVerdictDoesWhatItSays(t *testing.T) {
	tests := []struct {
		name    string
		args    Verdict
		want    outcome
		wantErr error
	}{
		{name: "forward", args: Forward(), want: outcome{Answer: streamAnswer, Served: 1}},
		{name: "drop", args: Drop(), want: outcome{}, wantErr: nats.ErrTimeout},
		{name: "lose the reply", args: LoseReply(), want: outcome{Served: 1, Altered: 1}, wantErr: nats.ErrTimeout},
		{
			name: "alter the reply",
			args: AlterReply("stream"),
			want: outcome{Answer: alteredAnswer, Served: 1, Altered: 1},
		},
		{
			name: "answer instead of the server",
			args: Answer([]byte(replacedAnswer)),
			want: outcome{Answer: replacedAnswer, Served: 1, Altered: 1},
		},
	}
	cluster := Start(t, oneNode)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			served := serve(t, cluster.URLs()[0])
			proxy := StartProxy(t, cluster.URLs()[0])
			proxy.Intercept(func(f *Frame) Verdict {
				if f.Subject == serviceSubject {
					return tt.args
				}
				return Forward()
			})
			client := connect(t, proxy.URL())

			answer, err := client.Request(serviceSubject, nil, requestWait)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Request = %v, want %v", err, tt.wantErr)
			}
			got := outcome{Served: served.Load(), Altered: proxy.AnswersAltered()}
			if answer != nil {
				got.Answer = string(answer.Data)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("outcome mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// serve answers every request on serviceSubject with streamAnswer and counts the requests it saw.
func serve(t *testing.T, url string) *atomic.Int32 {
	t.Helper()
	served := new(atomic.Int32)
	conn := connect(t, url)
	_, err := conn.Subscribe(serviceSubject, func(msg *nats.Msg) {
		served.Add(1)
		if err := msg.Respond([]byte(streamAnswer)); err != nil {
			t.Errorf("Respond: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return served
}

func connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}
