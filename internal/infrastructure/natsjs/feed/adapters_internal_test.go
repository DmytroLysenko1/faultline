package feed

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	closeWithin      = 5 * time.Second
	streamField      = "stream"
	secondPulseAfter = 1
)

// onNATS is a single-node server with the journal topology ensured, and the jetstream the feed's adapters use.
func onNATS(t *testing.T) (jetstream.JetStream, *Options) {
	t.Helper()
	cluster, options := ensured(t)
	return natsjstest.JetStream(t, strings.Join(cluster.URLs(), ",")), options
}

// onNATSBehind is onNATS with the adapters' jetstream going through a proxy that can alter what NATS answers.
func onNATSBehind(t *testing.T) (jetstream.JetStream, *Options, *natsjstest.Proxy) {
	t.Helper()
	cluster, options := ensured(t)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	return natsjstest.JetStream(t, proxy.URL()), options, proxy
}

func ensured(t *testing.T) (*natsjstest.Cluster, *Options) {
	t.Helper()
	cluster := natsjstest.Start(t, 1)
	conn, err := natsjs.Dial(t.Context(), &natsjs.ConnOptions{URLs: cluster.URLs(), Name: t.Name(),
		ConnectTimeout: time.Second, DrainTimeout: time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeWithin)
		defer cancel()
		if err := conn.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	options := testOptions()
	if err := conn.EnsureTopology(t.Context(), options.Topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	return cluster, options
}

func TestAHeartbeatLandsOnlyRightAfterTheLastRecordOnNATS(t *testing.T) {
	js, options := onNATS(t)
	pulses := partitionPulses(js, options)
	beat := &pulse{after: 0, writtenAt: at(0)}

	first := pulses.Pulse(t.Context(), beat)
	stale := pulses.Pulse(t.Context(), beat)
	forced := pulses.PulseUnconditionally(t.Context(), beat)

	if first != nil || !errors.Is(stale, errPulseConflict) || forced != nil {
		t.Errorf("Pulse = %v, the same base again = %v, unconditional = %v; want ok, a conflict and ok", first,
			stale, forced)
	}
	stream, err := js.Stream(t.Context(), options.Topology.Stream(0))
	if err != nil || stream.CachedInfo().State.Msgs != 2 {
		t.Errorf("stream = %v; want the conditional and the unconditional heartbeat", err)
	}
}

func TestHeartbeatsAfterTwoPositionsBothLandWithinTheDuplicateWindow(t *testing.T) {
	js, options := onNATS(t)
	pulses := partitionPulses(js, options)

	first := pulses.Pulse(t.Context(), &pulse{after: 0, writtenAt: at(0)})
	second := pulses.Pulse(t.Context(), &pulse{after: secondPulseAfter, writtenAt: at(time.Second)})

	if first != nil || second != nil {
		t.Fatalf("Pulse after 0 = %v, after 1 = %v; want both written", first, second)
	}
	stream, err := js.Stream(t.Context(), options.Topology.Stream(0))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if diff := cmp.Diff(uint64(2), stream.CachedInfo().State.Msgs); diff != "" {
		t.Errorf("heartbeats in the stream mismatch (-want +got):\n%s; a second one taken for a duplicate", diff)
	}
}

type foreignAckArgs struct {
	subject func(options *Options) string
	publish func(t *testing.T, js jetstream.JetStream, options *Options) error
}

// TestAnAnswerFromAnotherStreamIsRefused: WithExpectStream makes the server refuse a write to another stream, so
// only a proxy that alters the answer reaches the adapters' own check of the acknowledging stream.
func TestAnAnswerFromAnotherStreamIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		args    foreignAckArgs
		want    natsjs.Reason
		wantErr error
	}{
		{
			name: "a heartbeat",
			args: foreignAckArgs{
				subject: func(options *Options) string { return options.Topology.Subject(0) },
				publish: func(t *testing.T, js jetstream.JetStream, options *Options) error {
					return partitionPulses(js, options).Pulse(t.Context(), &pulse{after: 0, writtenAt: at(0)})
				},
			},
			want:    natsjs.ReasonStreamMismatch,
			wantErr: errForeignAck,
		},
		{
			name: "a dead letter",
			args: foreignAckArgs{
				subject: func(options *Options) string { return options.Topology.DeadLetterSubject(0) },
				publish: func(t *testing.T, js jetstream.JetStream, options *Options) error {
					return letterQueue(js, options).Publish(t.Context(), &deadLetter{partition: 0, sequence: 1,
						created: streamCreated(), reason: "crc", data: []byte(garbage), window: window{high: 1}})
				},
			},
			want:    natsjs.ReasonStreamMismatch,
			wantErr: errForeignAck,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js, options, proxy := onNATSBehind(t)
			subject := tt.args.subject(options)
			proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
				if f.Subject == subject {
					return natsjstest.AlterReply(streamField)
				}
				return natsjstest.Forward()
			})

			err := tt.args.publish(t, js, options)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			failure, ok := errors.AsType[*natsjs.PublishError](err)
			if !ok {
				t.Fatalf("err = %v, want a PublishError", err)
			}
			if diff := cmp.Diff(tt.want, failure.Reason); diff != "" {
				t.Errorf("reason mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheSeedFindsNoLetterWhereThereIsNone(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, js jetstream.JetStream)
	}{
		{name: "an empty DLQ", prepare: func(*testing.T, jetstream.JetStream) {}},
		{name: "no DLQ", prepare: func(t *testing.T, js jetstream.JetStream) {
			if err := js.DeleteStream(t.Context(), natsjs.DeadLetterStream); err != nil {
				t.Fatalf("delete the DLQ: %v", err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js, options := onNATS(t)
			tt.prepare(t, js)

			_, err := letterQueue(js, options).Last(t.Context(), 0)

			if !errors.Is(err, errNoLetter) {
				t.Errorf("Last = %v, want errNoLetter", err)
			}
		})
	}
}

func TestTheLatestCopyOfAPartitionComesBackFromNATS(t *testing.T) {
	js, options := onNATS(t)
	queue := letterQueue(js, options)
	older := &deadLetter{partition: 0, sequence: 2, created: streamCreated(), reason: "crc", data: []byte(garbage),
		window: window{high: 2}}
	newer := &deadLetter{partition: 0, sequence: 5, created: streamCreated(), reason: "crc", data: []byte(garbage),
		window: window{low: 2, high: 5}}
	other := &deadLetter{partition: 1, sequence: 9, created: recreated(), reason: "crc", data: []byte(garbage),
		window: window{high: 9}}
	for _, letter := range []*deadLetter{older, newer, other, older} {
		if err := queue.Publish(t.Context(), letter); err != nil {
			t.Fatalf("Publish %d: %v", letter.sequence, err)
		}
	}

	got, err := queue.Last(t.Context(), 0)

	want := lastLetter{created: time.Unix(0, streamCreated().UnixNano()), window: window{low: 2, high: 5}}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(lastLetter{}, window{})); err != nil || diff != "" {
		t.Errorf("Last = %v; want the newer copy, the repeated older one a duplicate (-want +got):\n%s", err, diff)
	}
}
