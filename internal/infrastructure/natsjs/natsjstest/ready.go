package natsjstest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	SettleWithin = 30 * time.Second
	settlePoll   = 50 * time.Millisecond
	askWithin    = 2 * time.Second
)

var errNoStreamLeader = errors.New("stream has no leader yet")

func Eventually(tb testing.TB, what string, ready func(ctx context.Context) error) {
	tb.Helper()
	deadline := time.Now().Add(SettleWithin)
	for {
		err := ask(tb, ready)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("%s not ready after %s: %v", what, SettleWithin, err)
		}
		time.Sleep(settlePoll)
	}
}

func ask(tb testing.TB, ready func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(tb.Context(), askWithin)
	defer cancel()
	return ready(ctx)
}

func AwaitStreamLeader(tb testing.TB, js jetstream.JetStream, name string) {
	tb.Helper()
	Eventually(tb, "a leader of stream "+name, func(ctx context.Context) error {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			return err
		}
		if cluster := stream.CachedInfo().Cluster; cluster != nil && cluster.Leader == "" {
			return errNoStreamLeader
		}
		return nil
	})
}
