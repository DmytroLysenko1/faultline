package cardinality_test

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

const (
	perService   = 500
	uniquePaths  = 2_000
	demoOwners   = 10_000
	checkout     = handle.Node(3)
	payment      = handle.Node(7)
	smallLimit   = 2
	smallOwners  = 16
	allocRuns    = 100
	maxIDOwners  = math.MaxInt32 / perService
	tooManyOwner = maxIDOwners + 1

	wiringPanicPrefix = "cardinality: "
)

func newRegistry(t testing.TB, limit, owners int) *cardinality.Registry {
	t.Helper()
	registry, err := cardinality.NewRegistry(cardinality.Limits{MaxPerService: limit}, owners)
	if err != nil {
		t.Fatalf("NewRegistry(%d, %d): %v", limit, owners, err)
	}
	return registry
}

func path(n int) operation.Operation {
	return operation.Operation{Protocol: operation.ProtocolHTTP, Name: fmt.Sprintf("GET /item-%04d", n)}
}

func mustAcquire(t *testing.T, registry *cardinality.Registry, owner handle.Node, op operation.Operation) cardinality.ID {
	t.Helper()
	id, ok := registry.Acquire(owner, op)
	if !ok {
		t.Fatalf("Acquire(%d, %q) refused, want admitted", owner, op.Name)
	}
	return id
}

func TestNewRegistryRejectsLimitsThatCannotHoldAnOperation(t *testing.T) {
	tests := []struct {
		Name    string
		Args    [2]int
		WantErr error
	}{
		{Name: "valid", Args: [2]int{perService, demoOwners}},
		{Name: "every owner at the limit still fits an int32 id", Args: [2]int{perService, maxIDOwners}},
		{Name: "zero per service", Args: [2]int{0, demoOwners}, WantErr: cardinality.ErrInvalidLimits},
		{Name: "negative per service", Args: [2]int{-1, demoOwners}, WantErr: cardinality.ErrInvalidLimits},
		{Name: "zero owners", Args: [2]int{perService, 0}, WantErr: cardinality.ErrInvalidLimits},
		{Name: "ids would overflow int32", Args: [2]int{perService, tooManyOwner}, WantErr: cardinality.ErrInvalidLimits},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := cardinality.NewRegistry(cardinality.Limits{MaxPerService: tt.Args[0]}, tt.Args[1])
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestTwoThousandPathsToOneServiceKeepExactlyFiveHundredOperations(t *testing.T) {
	registry := newRegistry(t, perService, demoOwners)
	admitted := 0
	for n := range uniquePaths {
		if _, ok := registry.Acquire(checkout, path(n)); ok {
			admitted++
		}
	}
	_, otherOwnerAdmitted := registry.Acquire(payment, path(0))

	type observed struct {
		Admitted           int
		OtherOwnerAdmitted bool
		Stats              cardinality.Stats
		OverLimit          []handle.Node
	}
	want := observed{
		Admitted:           perService,
		OtherOwnerAdmitted: true,
		Stats:              cardinality.Stats{Operations: perService + 1, OwnersOverLimit: 1},
		OverLimit:          []handle.Node{checkout},
	}
	got := observed{admitted, otherOwnerAdmitted, registry.Stats(), registry.OwnersOverLimit()}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("after %d unique paths (-want +got):\n%s", uniquePaths, diff)
	}
}

func TestAKnownOperationIsAdmittedAtTheLimitAndCountsReferences(t *testing.T) {
	registry := newRegistry(t, 1, smallOwners)
	first := mustAcquire(t, registry, checkout, path(1))
	again := mustAcquire(t, registry, checkout, path(1))

	registry.Release(checkout, first)
	_, stillKnown := registry.Lookup(checkout, path(1))
	registry.Release(checkout, again)
	_, knownAfterLast := registry.Lookup(checkout, path(1))

	type observed struct {
		SecondID             cardinality.ID
		KnownAfterOneRelease bool
		KnownAfterBoth       bool
		Stats                cardinality.Stats
	}
	want := observed{SecondID: first, KnownAfterOneRelease: true}
	got := observed{again, stillKnown, knownAfterLast, registry.Stats()}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("references (-want +got):\n%s", diff)
	}
}

func TestTheSameOperationIsSeparateForEachOwner(t *testing.T) {
	registry := newRegistry(t, 1, smallOwners)
	atCheckout := mustAcquire(t, registry, checkout, path(1))
	atPayment := mustAcquire(t, registry, payment, path(1))

	if atCheckout == atPayment {
		t.Errorf("both owners got id %d, want one id per (owner, operation)", atCheckout)
	}
	if diff := cmp.Diff(cardinality.Stats{Operations: 2}, registry.Stats()); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

func TestLookupHasNoSideEffects(t *testing.T) {
	registry := newRegistry(t, smallLimit, smallOwners)
	mustAcquire(t, registry, checkout, path(1))
	mustAcquire(t, registry, checkout, path(2))

	_, unknown := registry.Lookup(checkout, path(3))
	_, refused := registry.Acquire(checkout, path(3))

	if unknown || refused {
		t.Fatalf("Lookup of an unknown operation = %v, Acquire at the limit = %v; want both false", unknown, refused)
	}
	if diff := cmp.Diff(cardinality.Stats{Operations: smallLimit, OwnersOverLimit: 1}, registry.Stats()); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

func TestOperationReturnsWhatTheIDStandsFor(t *testing.T) {
	registry := newRegistry(t, smallLimit, smallOwners)
	first := mustAcquire(t, registry, checkout, path(1))
	second := mustAcquire(t, registry, payment, path(2))

	got := [2]operation.Operation{registry.Operation(first), registry.Operation(second)}
	if diff := cmp.Diff([2]operation.Operation{path(1), path(2)}, got); diff != "" {
		t.Errorf("Operation(id) (-want +got):\n%s", diff)
	}
}

func TestAForgottenIDIsReusedAndALiveOneIsNot(t *testing.T) {
	registry := newRegistry(t, smallLimit+1, smallOwners)
	kept := mustAcquire(t, registry, checkout, path(1))
	forgotten := mustAcquire(t, registry, checkout, path(2))
	registry.Release(checkout, forgotten)

	reused := mustAcquire(t, registry, checkout, path(3))
	fresh := mustAcquire(t, registry, checkout, path(4))

	if reused != forgotten {
		t.Errorf("new operation got id %d, want the forgotten id %d reused", reused, forgotten)
	}
	if fresh == kept || fresh == reused {
		t.Errorf("new operation got id %d, already held by a live operation (%d, %d)", fresh, kept, reused)
	}
}

func TestOwnerIsOverLimitFromTheFirstRefusalUntilAnOperationIsForgotten(t *testing.T) {
	registry := newRegistry(t, 1, smallOwners)
	mustAcquire(t, registry, payment, path(1))
	atCheckout := mustAcquire(t, registry, checkout, path(1))
	beforeRefusal := registry.OwnersOverLimit()
	registry.Acquire(payment, path(2))
	registry.Acquire(checkout, path(2))
	registry.Acquire(checkout, path(3))
	bothRefused := registry.OwnersOverLimit()
	extraReference := mustAcquire(t, registry, checkout, path(1))
	registry.Release(checkout, extraReference)
	stillHeld := registry.OwnersOverLimit()
	registry.Release(checkout, atCheckout)
	afterForget := registry.OwnersOverLimit()

	type fronts struct {
		BeforeRefusal, BothRefused, ReferenceDropped, OperationForgotten []handle.Node
	}
	want := fronts{
		BothRefused:        []handle.Node{checkout, payment},
		ReferenceDropped:   []handle.Node{checkout, payment},
		OperationForgotten: []handle.Node{payment},
	}
	got := fronts{beforeRefusal, bothRefused, stillHeld, afterForget}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("owners over limit (-want +got):\n%s", diff)
	}
}

func TestMisuseIsAWiringPanic(t *testing.T) {
	tests := []struct {
		Name string
		Args func(t *testing.T, registry *cardinality.Registry)
	}{
		{Name: "released twice", Args: func(t *testing.T, registry *cardinality.Registry) {
			id := mustAcquire(t, registry, checkout, path(1))
			registry.Release(checkout, id)
			registry.Release(checkout, id)
		}},
		{Name: "released by another owner", Args: func(t *testing.T, registry *cardinality.Registry) {
			registry.Release(payment, mustAcquire(t, registry, checkout, path(1)))
		}},
		{Name: "never acquired", Args: func(_ *testing.T, registry *cardinality.Registry) {
			registry.Release(checkout, cardinality.ID(0))
		}},
		{Name: "negative id released", Args: func(t *testing.T, registry *cardinality.Registry) {
			mustAcquire(t, registry, checkout, path(1))
			registry.Release(checkout, cardinality.ID(-1))
		}},
		{Name: "operation of a forgotten id", Args: func(t *testing.T, registry *cardinality.Registry) {
			id := mustAcquire(t, registry, checkout, path(1))
			registry.Release(checkout, id)
			registry.Operation(id)
		}},
		{Name: "acquire for no owner", Args: func(_ *testing.T, registry *cardinality.Registry) {
			registry.Acquire(handle.None, path(1))
		}},
		{Name: "acquire for an owner past the registry", Args: func(_ *testing.T, registry *cardinality.Registry) {
			registry.Acquire(handle.Node(smallOwners), path(1))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			defer func() {
				message, wiring := recover().(string)
				if !wiring || !strings.HasPrefix(message, wiringPanicPrefix) {
					t.Errorf("panic = %q, want a wiring panic starting with %q", message, wiringPanicPrefix)
				}
			}()
			tt.Args(t, newRegistry(t, smallLimit, smallOwners))
		})
	}
}

func TestHotPathsDoNotAllocate(t *testing.T) {
	registry := newRegistry(t, smallLimit, smallOwners)
	known, unknown := path(1), path(9)
	held := mustAcquire(t, registry, checkout, known)
	tests := []struct {
		Name string
		Args func()
	}{
		{Name: "Lookup of a known operation", Args: func() { registry.Lookup(checkout, known) }},
		{Name: "Lookup of an unknown operation", Args: func() { registry.Lookup(checkout, unknown) }},
		{Name: "Acquire and Release of a known operation", Args: func() {
			id, _ := registry.Acquire(checkout, known)
			registry.Release(checkout, id)
		}},
		{Name: "Operation", Args: func() { registry.Operation(held) }},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if allocations := testing.AllocsPerRun(allocRuns, tt.Args); allocations != 0 {
				t.Errorf("allocates %.0f times per call, want 0", allocations)
			}
		})
	}
}
