package admission_test

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const (
	racers         = 100
	racerAttempts  = 1000
	slotsUnderRace = 10
)

func TestElevenTokensDoNotExceedTheSlots(t *testing.T) {
	gate := newGate(t, defaultLimits(), newClock())
	for index := range 10 {
		hold(t, gate, numbered(index), 1)
	}
	_, eleventh := enter(gate, numbered(10))
	type outcome struct {
		Eleventh string
		InUse    int
	}
	want := outcome{
		Eleventh: refusedBySlots,
		InUse:    10,
	}
	got := outcome{
		Eleventh: eleventh,
		InUse:    gate.Admission.Stats().InUse,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("eleven tokens on ten slots (-want +got):\n%s", diff)
	}
}

func TestAdmitNeverExceedsTheSlots(t *testing.T) {
	gate := newGate(t, unlimitedRate(), newClock())
	var most, entered atomic.Int64
	var racing sync.WaitGroup
	for index := range racers {
		racing.Go(func() {
			for range racerAttempts {
				release, outcome := enter(gate, numbered(index))
				if outcome != admitted {
					continue
				}
				entered.Add(1)
				raiseTo(&most, int64(gate.Admission.Stats().InUse))
				release()
			}
		})
	}
	racing.Wait()
	type outcome struct {
		OverTheSlots bool
		FinalInUse   int
		Admitted     bool
	}
	want := outcome{
		OverTheSlots: false,
		FinalInUse:   0,
		Admitted:     true,
	}
	got := outcome{
		OverTheSlots: most.Load() > slotsUnderRace,
		FinalInUse:   gate.Admission.Stats().InUse,
		Admitted:     entered.Load() > racers,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("%d racers on %d slots, at most %d in use (-want +got):\n%s", racers, slotsUnderRace, most.Load(), diff)
	}
}

func raiseTo(most *atomic.Int64, seen int64) {
	for {
		current := most.Load()
		if seen <= current || most.CompareAndSwap(current, seen) {
			return
		}
	}
}

func TestASecondReleaseChangesNothing(t *testing.T) {
	gate := newGate(t, defaultLimits(), newClock())
	first := hold(t, gate, token("tokenA000000"), 2)[0]
	first()
	first()
	type outcome struct {
		InUse    int
		Admitted uint64
	}
	want := outcome{
		InUse:    1,
		Admitted: 2,
	}
	stats := gate.Admission.Stats()
	got := outcome{
		InUse:    stats.InUse,
		Admitted: stats.Admitted,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("a release called twice (-want +got):\n%s", diff)
	}
}

func TestEnterMarksTheConnectionEvenWhenRefused(t *testing.T) {
	limits := admission.Limits{
		Slots:             2,
		MinShare:          1,
		ActiveWindow:      10 * time.Second,
		WaitingWindow:     60 * time.Second,
		RequestsPerSecond: 3,
	}
	books, err := admission.New(limits, newClock())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	marks := &recordingMarks{}
	gate := admission.Gate{
		Admission: books,
		Marks:     marks,
	}
	remote := &net.TCPAddr{
		IP:   net.IPv4(10, 0, 0, 7),
		Port: 40001,
	}
	tokens := []string{"tokenA000000", "tokenA000000", "tokenA000000", "tokenB000000", "tokenC000000"}
	outcomes := make([]string, 0, len(tokens))
	for _, name := range tokens {
		_, err := gate.Enter(token(name), remote)
		outcomes = append(outcomes, outcomeOf(err))
	}
	type outcome struct {
		Outcomes []string
		Marks    []mark
		Stats    admission.Stats
	}
	want := outcome{
		Outcomes: []string{admitted, admitted, refusedByShare, refusedBySlots, refusedBySlots},
		Marks:    marksOf(remote.String(), tokens),
		Stats: admission.Stats{
			InUse:        2,
			ActiveTokens: 3,
			Admitted:     2,
			Rejected:     [admission.ReasonCount]uint64{1, 2, 0},
		},
	}
	got := outcome{
		Outcomes: outcomes,
		Marks:    marks.marks,
		Stats:    books.Stats(),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("five entries on two slots (-want +got):\n%s", diff)
	}
}

func marksOf(remote string, tokens []string) []mark {
	marks := make([]mark, 0, len(tokens))
	for _, name := range tokens {
		marks = append(marks, mark{
			Remote: remote,
			Token:  token(name),
		})
	}
	return marks
}

func TestNewRefusesLimitsThatCannotAdmit(t *testing.T) {
	valid := defaultLimits()
	tests := []struct {
		name    string
		args    func(limits *admission.Limits)
		wantErr error
	}{
		{
			name: "the default limits",
			args: func(*admission.Limits) {},
		},
		{
			name: "no slots",
			args: func(limits *admission.Limits) {
				limits.Slots = 0
			},
			wantErr: admission.ErrInvalidLimits,
		},
		{
			name: "no share",
			args: func(limits *admission.Limits) {
				limits.MinShare = 0
			},
			wantErr: admission.ErrInvalidLimits,
		},
		{
			name: "no active window",
			args: func(limits *admission.Limits) {
				limits.ActiveWindow = 0
			},
			wantErr: admission.ErrInvalidLimits,
		},
		{
			name: "a waiting window shorter than the active window",
			args: func(limits *admission.Limits) {
				limits.WaitingWindow = limits.ActiveWindow - time.Nanosecond
			},
			wantErr: admission.ErrInvalidLimits,
		},
		{
			name: "a waiting window equal to the active window",
			args: func(limits *admission.Limits) {
				limits.WaitingWindow = limits.ActiveWindow
			},
		},
		{
			name: "no rate",
			args: func(limits *admission.Limits) {
				limits.RequestsPerSecond = 0
			},
			wantErr: admission.ErrInvalidLimits,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := valid
			tt.args(&limits)
			_, err := admission.New(limits, newClock())
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("New(%+v) = %v, want %v", limits, err, tt.wantErr)
			}
		})
	}
}

func TestNewRefusesAMissingClock(t *testing.T) {
	if _, err := admission.New(defaultLimits(), nil); !errors.Is(err, admission.ErrInvalidLimits) {
		t.Errorf("New without a clock = %v, want %v", err, admission.ErrInvalidLimits)
	}
}
