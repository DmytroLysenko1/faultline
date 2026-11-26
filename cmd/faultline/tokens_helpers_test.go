package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const singleTokenReplica = 1

var testTokenIssuedAt = time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)

type fixedTap struct {
	gate *otlp.Tap
}

func (f fixedTap) tap() (*otlp.Tap, error) {
	return f.gate, nil
}

func admittingTaps() fixedTap {
	return fixedTap{
		gate: otlp.NewTap(otlptest.Admitting(), otlp.DefaultOptions(slog.New(slog.DiscardHandler), systemClock{})),
	}
}

func createTokenBucket(t *testing.T, urls ...string) {
	t.Helper()
	conn := dialOnce(t, urls...)
	defer closeOnce(t, conn)
	if err := tokens.CreateBucket(t.Context(), conn, singleTokenReplica); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func dialOnce(t *testing.T, urls ...string) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), natsSetup(urls...).connOptions(nil))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return conn
}

func closeOnce(t *testing.T, conn *natsjs.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeWithin)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

func grantTestToken(t *testing.T, tenant string, urls ...string) {
	t.Helper()
	grantToken(t, otlptest.Token, tenant, urls...)
}

func grantToken(t *testing.T, raw string, tenant string, urls ...string) {
	t.Helper()
	conn := dialOnce(t, urls...)
	defer closeOnce(t, conn)
	if err := tokens.CreateBucket(t.Context(), conn, singleTokenReplica); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	store, err := tokens.OpenStore(t.Context(), conn)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	token, err := credential.Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	grant, err := credential.NewGrant(token.ID, tenant, token.Digest(), testTokenIssuedAt)
	if err != nil {
		t.Fatalf("NewGrant: %v", err)
	}
	if err := store.Register(t.Context(), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
}
