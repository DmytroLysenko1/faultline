package stream

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
)

type (
	subjectCell   [5]uint64
	operationCell [2]uint64
)

func TestSlotsStayWithinTheMemoryBudgetAndHoldNoPointers(t *testing.T) {
	type layout struct {
		SubjectWithinBudget, OperationWithinBudget, PointerFree bool
	}
	want := layout{SubjectWithinBudget: true, OperationWithinBudget: true, PointerFree: true}
	got := layout{
		SubjectWithinBudget:   unsafe.Sizeof(slot[subjectCell]{}) <= 48,
		OperationWithinBudget: unsafe.Sizeof(slot[operationCell]{}) <= 24,
		PointerFree:           pointerFree(reflect.TypeFor[slot[subjectCell]]()),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("slot layout mismatch (-want +got):\n%s", diff)
	}
}

func pointerFree(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Struct:
		for field := range typ.Fields() {
			if !pointerFree(field.Type) {
				return false
			}
		}
		return true
	case reflect.Array:
		return pointerFree(typ.Elem())
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.String, reflect.Interface, reflect.Chan, reflect.Func,
		reflect.UnsafePointer:
		return false
	}
	return true
}

func TestRevisionSaturatesInsteadOfOverflowingIntoTheFlags(t *testing.T) {
	var s slot[operationCell]
	s.flags = maxRevision<<revisionShift | occupiedFlag

	got := [2]uint32{s.finalize(), s.flags & (1<<revisionShift - 1)}

	if diff := cmp.Diff([2]uint32{maxRevision, occupiedFlag}, got); diff != "" {
		t.Errorf("[revision, flags] at the maximum revision (-want +got):\n%s", diff)
	}
}
