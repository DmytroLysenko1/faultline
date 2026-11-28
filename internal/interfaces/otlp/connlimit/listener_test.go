package connlimit_test

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/connlimit"
)

const (
	refusedNewcomers = 1000
	refusalStep      = 4 * time.Millisecond
	warnInterval     = 5 * time.Second
	sharedRemote     = fakeAddr("10.9.9.9:50000")
	eventCloseFailed = "otlp.connections.close_failed"
)

var errBrokenSocket = errors.New("broken socket")

func TestAFreshConnectionAloneIsNeverEvicted(t *testing.T) {
	type want struct {
		Stats       connlimit.Stats
		FreshCloses int
		Closes      int
		UnderLock   int
	}
	f := newFixture(t, 1)
	f.offer("c1")
	f.mark("c1", "A")
	closes := 0
	for index := range refusedNewcomers {
		name := fmt.Sprintf("n%d", index)
		if f.offer(name) != nil {
			t.Fatalf("newcomer %d was accepted next to the only fresh connection", index)
		}
		closes += f.conns[name].closes
	}
	got := want{
		Stats:       f.listener.Stats(),
		FreshCloses: f.conns["c1"].closes,
		Closes:      closes,
		UnderLock:   f.locked,
	}
	wanted := want{
		Stats: connlimit.Stats{
			Open:     1,
			Accepted: 1,
			Refused:  refusedNewcomers,
		},
		Closes: refusedNewcomers,
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTheStatsCountEveryOutcome(t *testing.T) {
	f := newFixture(t, 2)
	f.offer("c1")
	f.offer("c2")
	if err := f.accepted["c1"].Close(); err != nil {
		t.Fatalf("close c1: %v", err)
	}
	openAfterClose := f.listener.Stats().Open
	f.offer("c3")
	f.offer("c4")
	f.mark("c3", "A")
	f.mark("c4", "A")
	f.clock.Advance(markLifetime)
	f.offer("c5")
	f.mark("c4", "A")
	f.mark("c5", "A")
	f.offer("c6")
	f.mark("c6", "B")
	f.offer("c7")
	type want struct {
		OpenAfterClose int
		Closed         []string
		Stats          connlimit.Stats
	}
	got := want{
		OpenAfterClose: openAfterClose,
		Closed:         sorted(f.closedByTheListener()),
		Stats:          f.listener.Stats(),
	}
	wanted := want{
		OpenAfterClose: 1,
		Closed:         []string{"c1", "c2", "c3", "c4", "c7"},
		Stats: connlimit.Stats{
			Open:                   2,
			Accepted:               6,
			EvictedUnauthenticated: 1,
			EvictedLapsed:          1,
			EvictedOverShare:       1,
			Refused:                1,
		},
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestEvictionClosesTheVictimOnceAndOutsideTheLock(t *testing.T) {
	type want struct {
		VictimCloses        int
		ClosedUnderLock     int
		OpenAfterStaleClose int
		NextEvictionClass   string
		NextEvictionVictims []string
	}
	f := newFixture(t, 1)
	f.offerFrom("c1", sharedRemote)
	f.mark("c1", "A")
	f.clock.Advance(markLifetime)
	if f.offerFrom("c2", sharedRemote) == nil {
		t.Fatal("the newcomer was refused next to a lapsed connection")
	}
	victimCloses := f.conns["c1"].closes
	staleAddr := f.accepted["c1"].RemoteAddr()
	if err := f.accepted["c1"].Close(); err != nil {
		t.Fatalf("the transport closes the evicted connection: %v", err)
	}
	openAfterStaleClose := f.listener.Stats().Open
	f.listener.Authenticated(staleAddr, "A")
	before := f.listener.Stats()
	f.offer("c3")
	got := want{
		VictimCloses:        victimCloses,
		ClosedUnderLock:     f.locked,
		OpenAfterStaleClose: openAfterStaleClose,
		NextEvictionClass:   classOf(before, f.listener.Stats()),
		NextEvictionVictims: sorted(f.closedByTheListener()),
	}
	wanted := want{
		VictimCloses:        1,
		OpenAfterStaleClose: 1,
		NextEvictionClass:   "unauthenticated",
		NextEvictionVictims: []string{"c1", "c2"},
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

type refusal struct {
	Level      string
	Peer       string
	Open       float64
	Max        float64
	Suppressed float64
}

func refusalsOf(t *testing.T, f *fixture) []refusal {
	t.Helper()
	var refusals []refusal
	for _, record := range f.logs.events(t, eventRefused) {
		refusals = append(refusals, refusal{
			Level:      fmt.Sprint(record["level"]),
			Peer:       fmt.Sprint(record["peer"]),
			Open:       numberOf(record["open"]),
			Max:        numberOf(record["max_connections"]),
			Suppressed: numberOf(record["suppressed_connections"]),
		})
	}
	return refusals
}

func numberOf(value any) float64 {
	number, isNumber := value.(float64)
	if !isNumber {
		return -1
	}
	return number
}

func TestRefusedNewcomersWriteOneWarnPerInterval(t *testing.T) {
	type want struct {
		Refusals  []refusal
		UnderLock int
	}
	f := newFixture(t, 1)
	f.offer("c1")
	f.mark("c1", "A")
	base := f.clock.now
	for index := range refusedNewcomers {
		f.clock.now = base.Add(time.Duration(index) * refusalStep)
		f.offer(fmt.Sprintf("n%d", index))
	}
	f.clock.now = base.Add(warnInterval)
	f.offerFrom("late", sharedRemote)
	got := want{
		Refusals:  refusalsOf(t, f),
		UnderLock: f.logs.locked,
	}
	wanted := want{
		Refusals: []refusal{
			{
				Level: slog.LevelWarn.String(),
				Peer:  f.conns["n0"].remote.String(),
				Open:  1,
				Max:   1,
			},
			{
				Level:      slog.LevelWarn.String(),
				Peer:       string(sharedRemote),
				Open:       1,
				Max:        1,
				Suppressed: refusedNewcomers - 1,
			},
		},
	}
	if diff := cmp.Diff(wanted, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestAFailedCloseIsLoggedAtDebug(t *testing.T) {
	tests := []struct {
		name string
		args func(f *fixture)
		want []string
	}{
		{
			name: "a refused newcomer",
			args: func(f *fixture) {
				f.offer("c1")
				f.mark("c1", "A")
				f.inner.queue = append(f.inner.queue, &fakeConn{
					remote:   sharedRemote,
					closeErr: errBrokenSocket,
				})
				if _, err := f.listener.Accept(); !errors.Is(err, errDrained) {
					t.Fatalf("Accept = %v, want the newcomer refused", err)
				}
			},
			want: []string{string(sharedRemote) + " " + errBrokenSocket.Error()},
		},
		{
			name: "an evicted connection",
			args: func(f *fixture) {
				f.offerFrom("c1", sharedRemote)
				f.conns["c1"].closeErr = errBrokenSocket
				f.offer("c2")
			},
			want: []string{string(sharedRemote) + " " + errBrokenSocket.Error()},
		},
		{
			name: "a clean close",
			args: func(f *fixture) {
				f.offer("c1")
				f.offer("c2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, 1)
			tt.args(f)
			var got []string
			for _, record := range f.logs.events(t, eventCloseFailed) {
				got = append(got, fmt.Sprint(record["peer"], " ", record["error"]))
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAnAddressFromElsewhereMarksNothing(t *testing.T) {
	tests := []struct {
		name string
		args func(t *testing.T, ours *fixture) (net.Addr, *fixture)
	}{
		{
			name: "no peer address",
			args: func(_ *testing.T, ours *fixture) (net.Addr, *fixture) {
				return nil, ours
			},
		},
		{
			name: "an address the listener did not hand out",
			args: func(_ *testing.T, ours *fixture) (net.Addr, *fixture) {
				return sharedRemote, ours
			},
		},
		{
			name: "an address another listener handed out",
			args: func(t *testing.T, _ *fixture) (net.Addr, *fixture) {
				other := newFixture(t, 1)
				return other.offer("c1").RemoteAddr(), other
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ours := newFixture(t, 1)
			ours.offer("c1")
			remote, owner := tt.args(t, ours)
			ours.listener.Authenticated(remote, "A")
			if diff := cmp.Diff(eviction{
				Closed: []string{"c1"},
				Class:  "unauthenticated",
			}, evictionAt(owner, 0)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheListenerHandsCloseAndAddrToItsInner(t *testing.T) {
	type want struct {
		Addr        string
		InnerCloses int
	}
	f := newFixture(t, 1)
	if err := f.listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := want{
		Addr:        f.listener.Addr().String(),
		InnerCloses: f.inner.closes,
	}
	if diff := cmp.Diff(want{
		Addr:        "127.0.0.1:4317",
		InnerCloses: 1,
	}, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestNewListenerRefusesLimitsThatCannotHoldAConnection(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	valid := connlimit.Limits{
		MaxConnections:     1,
		MarkLifetime:       time.Nanosecond,
		RefusalLogInterval: time.Nanosecond,
	}
	type args struct {
		inner  net.Listener
		limits connlimit.Limits
		clock  *fakeClock
		logger *slog.Logger
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "the smallest limits",
			args: args{
				inner:  &queuedListener{},
				limits: valid,
				clock:  newClock(),
				logger: logger,
			},
		},
		{
			name: "no connection",
			args: args{
				inner: &queuedListener{},
				limits: connlimit.Limits{
					MarkLifetime:       time.Nanosecond,
					RefusalLogInterval: time.Nanosecond,
				},
				clock:  newClock(),
				logger: logger,
			},
			wantErr: connlimit.ErrInvalidLimits,
		},
		{
			name: "a mark that never lives",
			args: args{
				inner: &queuedListener{},
				limits: connlimit.Limits{
					MaxConnections:     1,
					RefusalLogInterval: time.Nanosecond,
				},
				clock:  newClock(),
				logger: logger,
			},
			wantErr: connlimit.ErrInvalidLimits,
		},
		{
			name: "a refusal log that never pauses",
			args: args{
				inner: &queuedListener{},
				limits: connlimit.Limits{
					MaxConnections: 1,
					MarkLifetime:   time.Nanosecond,
				},
				clock:  newClock(),
				logger: logger,
			},
			wantErr: connlimit.ErrInvalidLimits,
		},
		{
			name: "no inner listener",
			args: args{
				limits: valid,
				clock:  newClock(),
				logger: logger,
			},
			wantErr: connlimit.ErrInvalidLimits,
		},
		{
			name: "no clock",
			args: args{
				inner:  &queuedListener{},
				limits: valid,
				logger: logger,
			},
			wantErr: connlimit.ErrInvalidLimits,
		},
		{
			name: "no logger",
			args: args{
				inner:  &queuedListener{},
				limits: valid,
				clock:  newClock(),
			},
			wantErr: connlimit.ErrInvalidLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var clock interface {
				Now() time.Time
			}
			if tt.args.clock != nil {
				clock = tt.args.clock
			}
			_, err := connlimit.NewListener(tt.args.inner, tt.args.limits, clock, tt.args.logger)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
