package tombstone_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
)

func newSet(tb testing.TB, limit int) tombstone.Set {
	tb.Helper()
	stones, err := tombstone.New(limit)
	if err != nil {
		tb.Fatalf("tombstone.New: %v", err)
	}
	return stones
}

func TestNewRejectsNonPositiveLimit(t *testing.T) {
	tests := []struct {
		Name    string
		Args    int
		Want    string
		WantErr error
	}{
		{
			Name:    "zero",
			Args:    0,
			Want:    "invalid tombstone limit: max tombstones 0 must be positive",
			WantErr: tombstone.ErrInvalidLimit,
		},
		{
			Name:    "negative",
			Args:    -1,
			Want:    "invalid tombstone limit: max tombstones -1 must be positive",
			WantErr: tombstone.ErrInvalidLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := tombstone.New(tt.Args)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewAcceptsLimitOfOne(t *testing.T) {
	stones := newSet(t, 1)
	stones.Add(numberedKey(1), tombstone.BothSides, 1)
	stones.Add(numberedKey(2), tombstone.BothSides, 2)
	if diff := cmp.Diff(keyRange(2, 2), ringKeys(t, &stones)); diff != "" {
		t.Errorf("kept keys mismatch (-want +got):\n%s", diff)
	}
}

func numberedKey(n int) ids.CallKey {
	return ids.NewCallKey(ids.TraceID{
		byte(n),
		byte(n >> 8),
	}, ids.SpanID{
		1,
	})
}

func ringKeys(t *testing.T, stones *tombstone.Set) []ids.CallKey {
	t.Helper()
	if len(stones.ScopeByKey) != stones.Size {
		t.Fatalf("index has %d keys, ring has %d entries", len(stones.ScopeByKey), stones.Size)
	}
	keys := make([]ids.CallKey, 0, stones.Size)
	for i := range stones.Size {
		key := stones.Ring[(stones.Head+i)%len(stones.Ring)].Key
		if _, indexed := stones.ScopeByKey[key]; !indexed {
			t.Fatalf("ring entry %d is missing from the index", i)
		}
		keys = append(keys, key)
	}
	return keys
}

func keyRange(from, to int) []ids.CallKey {
	return lo.Map(lo.RangeFrom(from, to-from+1), func(n, _ int) ids.CallKey {
		return numberedKey(n)
	})
}

func TestTombstonesKeepExpiryOrderAcrossWrapAndGrowth(t *testing.T) {
	stones := newSet(t, 100)
	for n := 1; n <= 20; n++ {
		stones.Add(numberedKey(n), tombstone.BothSides, time.Duration(n))
	}
	stones.Expire(10)
	if diff := cmp.Diff(keyRange(11, 20), ringKeys(t, &stones)); diff != "" {
		t.Fatalf("after first expiry (-want +got):\n%s", diff)
	}

	for n := 21; n <= 60; n++ {
		stones.Add(numberedKey(n), tombstone.BothSides, time.Duration(n))
	}
	stones.Expire(25)
	if diff := cmp.Diff(keyRange(26, 60), ringKeys(t, &stones)); diff != "" {
		t.Errorf("after wrap and growth (-want +got):\n%s", diff)
	}
}

func TestTombstonesStayUsableAfterFullExpiry(t *testing.T) {
	stones := newSet(t, 10)
	stones.Add(numberedKey(1), tombstone.BothSides, 1)
	stones.Add(numberedKey(2), tombstone.BothSides, 2)
	stones.Expire(5)
	stones.Expire(6)
	stones.Add(numberedKey(3), tombstone.BothSides, 10)
	if diff := cmp.Diff(keyRange(3, 3), ringKeys(t, &stones)); diff != "" {
		t.Errorf("kept keys mismatch (-want +got):\n%s", diff)
	}
}

func TestTombstonesEvictOldestAtLimit(t *testing.T) {
	stones := newSet(t, 3)
	var evicted int
	for n := 1; n <= 5; n++ {
		if stones.Add(numberedKey(n), tombstone.BothSides, time.Duration(n)) {
			evicted++
		}
	}
	if diff := cmp.Diff(2, evicted); diff != "" {
		t.Errorf("evicted mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(keyRange(3, 5), ringKeys(t, &stones)); diff != "" {
		t.Errorf("kept keys mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(stones.Limit, len(stones.Ring)); diff != "" {
		t.Errorf("ring grew past the limit (-limit +ring):\n%s", diff)
	}
}

func TestScopeSuppresses(t *testing.T) {
	type args struct {
		Scope tombstone.Scope
		Side  role.Role
	}
	tests := []struct {
		Name string
		Args args
		Want bool
	}{
		{
			Name: "pair tombstone stops a caller",
			Args: args{
				Scope: tombstone.BothSides,
				Side:  role.Caller,
			},
			Want: true,
		},
		{
			Name: "pair tombstone stops a callee",
			Args: args{
				Scope: tombstone.BothSides,
				Side:  role.Callee,
			},
			Want: true,
		},
		{
			Name: "expired caller tombstone stops a retried caller",
			Args: args{
				Scope: tombstone.CallerOnly,
				Side:  role.Caller,
			},
			Want: true,
		},
		{
			Name: "expired caller tombstone lets a late callee pass",
			Args: args{
				Scope: tombstone.CallerOnly,
				Side:  role.Callee,
			},
			Want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, tt.Args.Scope.Suppresses(tt.Args.Side)); diff != "" {
				t.Errorf("Suppresses mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTombstoneScope(t *testing.T) {
	stones := newSet(t, 10)
	stones.Add(numberedKey(1), tombstone.BothSides, time.Second)
	stones.Add(numberedKey(2), tombstone.CallerOnly, time.Second)
	type want struct {
		BothCaller, BothCallee, CallerOnlyCaller, CallerOnlyCallee, Absent bool
	}
	got := want{
		BothCaller:       stones.Suppresses(numberedKey(1), role.Caller),
		BothCallee:       stones.Suppresses(numberedKey(1), role.Callee),
		CallerOnlyCaller: stones.Suppresses(numberedKey(2), role.Caller),
		CallerOnlyCallee: stones.Suppresses(numberedKey(2), role.Callee),
		Absent:           stones.Suppresses(numberedKey(3), role.Caller),
	}
	if diff := cmp.Diff(want{
		BothCaller:       true,
		BothCallee:       true,
		CallerOnlyCaller: true,
	}, got); diff != "" {
		t.Errorf("suppression mismatch (-want +got):\n%s", diff)
	}
}
