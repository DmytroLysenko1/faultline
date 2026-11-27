package admission_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const (
	bookEntries     = 2048
	holderRefusals  = 20
	refusedNewcomer = "tokenD000000"
)

type topHolder struct {
	Reason admission.Reason
	Token  credential.ID
	ID     credential.ID
	Held   int
}

func rejectionOf(t *testing.T, gate admission.Gate, id credential.ID) topHolder {
	t.Helper()
	_, err := gate.Enter(id, nil)
	rejection, refused := errors.AsType[admission.RejectionError](err)
	if !refused {
		t.Fatalf("%s: %v, want a rejection", id, err)
	}
	return topHolder{
		Reason: rejection.Reason,
		Token:  rejection.TokenID,
		ID:     rejection.TopHolderID,
		Held:   rejection.TopHolderHeld,
	}
}

func repeatedRejections(t *testing.T, gate admission.Gate, id credential.ID) []topHolder {
	t.Helper()
	seen := make([]topHolder, 0, holderRefusals)
	for range holderRefusals {
		seen = append(seen, rejectionOf(t, gate, id))
	}
	return seen
}

func slotsHeldBy(holder credential.ID, held int) []topHolder {
	want := make([]topHolder, 0, holderRefusals)
	for range holderRefusals {
		want = append(want, topHolder{
			Reason: admission.ReasonSlots,
			Token:  token(refusedNewcomer),
			ID:     holder,
			Held:   held,
		})
	}
	return want
}

func TestTheTopHolderIsTheLargestHolder(t *testing.T) {
	limits := unlimitedRate()
	limits.MinShare = 5
	gate := newGate(t, limits, newClock())
	for index := range bookEntries - 3 {
		enterAndLeave(gate, numbered(index))
	}
	hold(t, gate, tokenA, 3)
	heldByB := hold(t, gate, tokenB, 5)
	hold(t, gate, tokenC, 2)
	whileBHoldsFive := repeatedRejections(t, gate, token(refusedNewcomer))
	heldByB[0]()
	hold(t, gate, token("tokenE000000"), 1)
	whileBHoldsFour := repeatedRejections(t, gate, token(refusedNewcomer))
	releaseAll(heldByB[1:])
	for _, name := range []string{"tokenF000000", "tokenG000000", "tokenH000000", "tokenI000000"} {
		hold(t, gate, token(name), 1)
	}
	afterBLeft := repeatedRejections(t, gate, token(refusedNewcomer))
	want := [][]topHolder{slotsHeldBy(tokenB, 5), slotsHeldBy(tokenB, 4), slotsHeldBy(tokenA, 3)}
	got := [][]topHolder{whileBHoldsFive, whileBHoldsFour, afterBLeft}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("top holder among %d entries (-want +got):\n%s", bookEntries, diff)
	}
}

func TestATieAmongTheTopHoldersNamesTheSmallerTokenID(t *testing.T) {
	limits := unlimitedRate()
	limits.Slots = 4
	gate := newGate(t, limits, newClock())
	hold(t, gate, token("tokenY000000"), 2)
	hold(t, gate, token("tokenX000000"), 2)
	want := slotsHeldBy(token("tokenX000000"), 2)
	if diff := cmp.Diff(want, repeatedRejections(t, gate, token(refusedNewcomer))); diff != "" {
		t.Errorf("two holders of 2 (-want +got):\n%s", diff)
	}
}

func TestAShareRefusalNamesTheRefusedTokenAsTheHolder(t *testing.T) {
	gate := newGate(t, unlimitedRate(), newClock())
	hold(t, gate, tokenA, 10)
	want := topHolder{
		Reason: admission.ReasonShare,
		Token:  tokenA,
		ID:     tokenA,
		Held:   10,
	}
	if diff := cmp.Diff(want, rejectionOf(t, gate, tokenA)); diff != "" {
		t.Errorf("A over its share (-want +got):\n%s", diff)
	}
}

func TestARateRefusalNamesNoHolder(t *testing.T) {
	limits := defaultLimits()
	limits.RequestsPerSecond = 1
	gate := newGate(t, limits, newClock())
	hold(t, gate, tokenA, 1)
	want := topHolder{
		Reason: admission.ReasonRate,
		Token:  tokenB,
	}
	if diff := cmp.Diff(want, rejectionOf(t, gate, tokenB)); diff != "" {
		t.Errorf("B after the budget is spent (-want +got):\n%s", diff)
	}
}

func TestEveryRefusalReasonHasItsOwnNameAndError(t *testing.T) {
	type want struct {
		Name    string
		Outcome string
	}
	tests := []struct {
		name string
		args admission.Reason
		want want
	}{
		{
			name: "share",
			args: admission.ReasonShare,
			want: want{
				Name:    "share",
				Outcome: refusedByShare,
			},
		},
		{
			name: "slots",
			args: admission.ReasonSlots,
			want: want{
				Name:    "slots",
				Outcome: refusedBySlots,
			},
		},
		{
			name: "rate",
			args: admission.ReasonRate,
			want: want{
				Name:    "rate",
				Outcome: refusedByRate,
			},
		},
		{
			name: "past the last reason",
			args: admission.ReasonCount,
			want: want{
				Name:    "unknown",
				Outcome: admission.ErrUnknownReason.Error(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := want{
				Name: tt.args.String(),
				Outcome: outcomeOf(admission.RejectionError{
					Reason: tt.args,
				}),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("reason %d (-want +got):\n%s", tt.args, diff)
			}
		})
	}
}
