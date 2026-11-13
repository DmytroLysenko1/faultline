package natsjs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	tinyMessageLimit = 1024
	headerOverhead   = 100
	publishWithin    = 2 * time.Second
)

// TestAMessageOverTheStreamLimitIsTooLargeOnALiveServer: the server counts headers into the limit
// and answers 10054; that must be final (INVALID_ARGUMENT), not a retry that can never succeed.
func TestAMessageOverTheStreamLimitIsTooLargeOnALiveServer(t *testing.T) {
	cluster := natsjstest.Start(t, 1)
	nc, err := nats.Connect(cluster.URLs()[0])
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "TINY", Subjects: []string{"tiny"}, MaxMsgSize: tinyMessageLimit,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), publishWithin)
	defer cancel()

	_, publishErr := js.PublishMsg(ctx, &nats.Msg{Subject: "tiny", Data: make([]byte, tinyMessageLimit-headerOverhead/2)},
		jetstream.WithMsgID("a"), jetstream.WithExpectStream("TINY"))

	got := classify(publishErr)
	if got.Reason != ReasonTooLarge || got.Code != uint16(codeMessageTooLarge) || !errors.Is(got, ingest.ErrBatchTooLarge) {
		t.Errorf("classify(%v) = %+v, want too large with code %d", publishErr, got, codeMessageTooLarge)
	}
}

// TestEnsureRetriesOnlyWhatPassesOnItsOwn: a start may wait out a reconnect, an election or peers
// still marked offline; a conflict, a refusal or the caller's cancel ends it at once.
func TestEnsureRetriesOnlyWhatPassesOnItsOwn(t *testing.T) {
	tests := []struct {
		name string
		args error
		want bool
	}{
		{name: "nothing failed", args: nil, want: false},
		{name: "reconnecting", args: nats.ErrReconnectBufExceeded, want: true},
		{name: "no responders", args: nats.ErrNoResponders, want: true},
		{name: "no metadata leader", args: apiError(codeClusterUnavailable, "JetStream system temporarily unavailable"),
			want: true},
		{name: "peers still offline", args: apiError(codeNoSuitablePeers, "no suitable peers for placement"), want: true},
		{name: "request timeout", args: nats.ErrTimeout, want: true},
		{name: "the deadline", args: context.DeadlineExceeded, want: true},
		{name: "the caller cancelled", args: context.Canceled, want: false},
		{name: "closed by its owner — it never comes back", args: nats.ErrConnectionClosed, want: false},
		{name: "draining for shutdown", args: nats.ErrConnectionDraining, want: false},
		{name: "another config under the name", args: jetstream.ErrStreamNameAlreadyInUse, want: false},
		{name: "an account limit", args: apiError(codeAccountLimits, "resource limits exceeded for account"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := temporary(tt.args); got != tt.want {
				t.Errorf("temporary(%v) = %t, want %t", tt.args, got, tt.want)
			}
		})
	}
}
