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
	current := otlpLayer(&Config{}, testIngest(), nil, nil, testLogger(logs))
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

func TestTheOTLPLayerServesWithinItsConnectionLimit(t *testing.T) {
	logs := &logBuffer{}
	address := localAddress(t)
	limits := testIngest()
	limits.MaxConnections = 1
	current := otlpLayer(&Config{
		OTLPAddr: address,
	}, limits, acceptingJournal(t), admittingTaps(), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	if err := current.unit.start(t.Context(), view); err != nil {
		t.Fatalf("start: %v", err)
	}
	first := dialRawOTLP(t, address)
	if !answers(t, first) {
		t.Fatal("the first connection got no server preface")
	}
	second := dialRawOTLP(t, address)
	type want struct {
		SecondServed bool
		FirstClosed  bool
		Connections  map[string]any
	}
	got := want{
		SecondServed: answers(t, second),
		FirstClosed:  closedByThePeer(t, first),
		Connections:  groupNamed(t, current.statsGroups(t.Context()), "connections"),
	}
	if err := stopLayer(t, &current, view); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
	assertDiff(t, "a silent second connection at the limit of one", want{
		SecondServed: true,
		FirstClosed:  true,
		Connections: map[string]any{
			"open":                    int64(1),
			"accepted":                uint64(2),
			"evicted_unauthenticated": uint64(1),
			"evicted_lapsed":          uint64(0),
			"evicted_over_share":      uint64(0),
			"refused":                 uint64(0),
		},
	}, got)
}

func closedByThePeer(t *testing.T, conn net.Conn) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(answerWithin)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	_, err := io.Copy(io.Discard, conn)
	return err == nil
}

func TestTheOTLPLayerRefusesToStartBeforeTheTokenDirectory(t *testing.T) {
	logs := &logBuffer{}
	address := localAddress(t)
	current := otlpLayer(&Config{
		OTLPAddr: address,
	}, testIngest(), acceptingJournal(t), &directoryUnit{}, testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)

	err := current.unit.start(t.Context(), view)

	failure, found := errors.AsType[*stageError](err)
	if !found {
		t.Fatalf("start = %v, want a stage error", err)
	}
	assertDiff(t, "stage", stageOTLPTap, failure.stage)
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), network, address)
	if err != nil {
		t.Fatalf("the refused layer holds its port: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestTheOTLPLayerKeepsASignalDuringItsStart(t *testing.T) {
	logs := &logBuffer{}
	current := otlpLayer(&Config{
		OTLPAddr: loopback,
	}, testIngest(), acceptingJournal(t), admittingTaps(), testLogger(logs))
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

func answers(t *testing.T, conn net.Conn) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(answerWithin)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	read, err := io.ReadAtLeast(conn, make([]byte, prefaceBytes), prefaceBytes)
	return err == nil && read >= prefaceBytes
}
