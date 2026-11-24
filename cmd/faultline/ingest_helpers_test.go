package main

import (
	"context"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	testStreamBytes   = 64 << 20
	testDedupWindow   = 5 * time.Minute
	testPublishBudget = 2 * time.Second
	closeWithin       = 5 * time.Second
	readyPath         = "/readyz"
)

func dialTest(t *testing.T, urls ...string) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), natsSetup(urls...).connOptions(nil))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeWithin)
		defer cancel()
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return conn
}
