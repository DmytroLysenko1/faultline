package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
)

const (
	quietWindow  = 300 * time.Millisecond
	answerWithin = 2 * time.Second
	prefaceBytes = 1
)

type acceptingPublisher struct{}

func (acceptingPublisher) Publish(context.Context, uint16, []byte, string) (bool, error) {
	return false, nil
}

type openedJournal struct {
	journal *journal.Journal
}

func (o openedJournal) opened() *journal.Journal {
	return o.journal
}

func acceptingJournal(t *testing.T) openedJournal {
	t.Helper()
	opened, err := journal.New(acceptingPublisher{}, testJournalOptions())
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	return openedJournal{
		journal: opened,
	}
}

type servingOTLP struct {
	*otlpUnit
}

func (s servingOTLP) start(_ context.Context, sup unitSupervisor) error {
	s.sup = sup
	sup.spawn(componentOTLP, func() error {
		return withoutSentinel(s.server.Serve(nil), grpc.ErrServerStopped)
	})
	return nil
}

func stuckOTLPLayer(logs *logBuffer, server *fakeServer) layer {
	current := otlpLayer(&Config{}, nil, testLogger(logs))
	unit := current.unit.(*otlpUnit)
	unit.server = server
	current.unit = servingOTLP{
		otlpUnit: unit,
	}
	return current
}

type stuckStop struct {
	Code      int
	Forced    []any
	Abandoned bool
}

func forcedOrAbandoned(forced []any, abandoned bool) stuckStop {
	return stuckStop{
		Forced:    forced,
		Abandoned: abandoned,
	}
}

func TestAForcedOTLPStopIsAWarningOnlyWhenItsGoroutineExits(t *testing.T) {
	tests := []struct {
		name string
		args bool
		want stuckStop
	}{
		{
			name: "Stop ends the goroutine: one forced stop, code 0",
			want: stuckStop{
				Code:   exitOK,
				Forced: []any{layerOTLP},
			},
		},
		{
			name: "the goroutine outlives Stop: no forced stop, the step is abandoned, code 1",
			args: true,
			want: stuckStop{
				Code:      exitFailure,
				Forced:    []any{},
				Abandoned: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := &logBuffer{}
				server := newFakeIngest(&eventLog{})
				server.hangGraceful = true
				server.ignoreStop = tt.args
				role := &stack{
					layers: []layer{stuckOTLPLayer(logs, server)},
				}

				code := signalServed(t, role, logs)
				server.release()

				assertCode(t, code, tt.want.Code, logs)
				forced := valuesOf(logs.all(t, eventForcedStop), "layer")
				failed := logs.event(t, eventLayerStopFailed)
				abandoned := failed != nil
				if message, _ := failed["error"].(string); abandoned && !strings.Contains(message, "deadline exceeded") {
					t.Errorf("layer_stop_failed error = %q, want the GracefulStop deadline next to the abandoned step", message)
				}
				want := tt.want
				want.Code = 0
				assertDiff(t, "forced stop and abandoned", want, forcedOrAbandoned(forced, abandoned))
			})
		})
	}
}

func TestTheOTLPLayerReturnsItsServersStopError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logBuffer{}
		server := newFakeServer(&eventLog{}, componentOTLP, errServe)
		current := stuckOTLPLayer(logs, server)
		view := newLayerSupervisor(t, logs, &current)
		if err := current.unit.start(t.Context(), view); err != nil {
			t.Fatalf("start: %v", err)
		}

		err := stopLayer(t, &current, view)

		if !errors.Is(err, errServe) {
			t.Errorf("stop = %v, want the server's own error", err)
		}
	})
}

func TestTheOTLPLayerAcceptsOneConnectionBeforeServing(t *testing.T) {
	logs := &logBuffer{}
	address := localAddress(t)
	current := otlpLayer(&Config{
		OTLPAddr: address,
	}, acceptingJournal(t), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	if err := current.unit.start(t.Context(), view); err != nil {
		t.Fatalf("start: %v", err)
	}
	first := dialRawOTLP(t, address)
	if !answers(t, first, answerWithin) {
		t.Fatal("the first connection got no server preface")
	}
	second := dialRawOTLP(t, address)

	if answers(t, second, quietWindow) {
		t.Error("the second connection was served while the first is open")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close the first: %v", err)
	}
	if !answers(t, second, answerWithin) {
		t.Error("the second connection was not served after the first closed")
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close the second: %v", err)
	}
	if err := stopLayer(t, &current, view); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
}

func TestTheOTLPLayerKeepsASignalDuringItsStart(t *testing.T) {
	logs := &logBuffer{}
	current := otlpLayer(&Config{
		OTLPAddr: loopback,
	}, acceptingJournal(t), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	signalled, cancel := context.WithCancelCause(t.Context())
	cancel(errTestSignal)

	err := current.unit.start(signalled, view)

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("start = %v, want nil or a cancel", err)
	}
	if err := stopLayer(t, &current, view); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
}

func dialRawOTLP(t *testing.T, address string) net.Conn {
	t.Helper()
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), network, address)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return conn
}

func answers(t *testing.T, conn net.Conn, within time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	read, err := io.ReadAtLeast(conn, make([]byte, prefaceBytes), prefaceBytes)
	return err == nil && read >= prefaceBytes
}
