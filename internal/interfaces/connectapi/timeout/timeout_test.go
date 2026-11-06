package timeout_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/timeout"
)

const limit = time.Minute

var errNext = errors.New("next failed")

func TestInterceptorBoundsTheCallAndReleasesIt(t *testing.T) {
	var deadline time.Time
	var bounded bool
	var released <-chan struct{}
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		deadline, bounded = ctx.Deadline()
		released = ctx.Done()
		return nil, errNext
	}
	started := time.Now()
	_, err := timeout.Interceptor(limit)(next)(t.Context(), nil)
	if !errors.Is(err, errNext) {
		t.Fatalf("err = %v, want the error of next %v", err, errNext)
	}
	if !bounded {
		t.Fatalf("next got a context without a deadline")
	}
	if deadline.Before(started.Add(limit)) || deadline.After(time.Now().Add(limit)) {
		t.Errorf("deadline %v is not %v after the call", deadline, limit)
	}
	select {
	case <-released:
	default:
		t.Errorf("context of next is still alive after the call")
	}
}

func TestInterceptorKeepsAnEarlierCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	callerDeadline, _ := ctx.Deadline()
	var got time.Time
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		got, _ = ctx.Deadline()
		return nil, nil
	}
	if _, err := timeout.Interceptor(limit)(next)(ctx, nil); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if !got.Equal(callerDeadline) {
		t.Errorf("deadline = %v, want the caller's %v", got, callerDeadline)
	}
}
