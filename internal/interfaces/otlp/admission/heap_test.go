package admission_test

import (
	"math"
	"runtime"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const heapEntries = 20_000

func TestABookEntryFitsItsHeapBudget(t *testing.T) {
	ids := make([]credential.ID, heapEntries)
	for index := range ids {
		ids[index] = numbered(index)
	}
	gate := newGate(t, unlimitedRate(), newClock())
	before := heapInUse()
	for _, id := range ids {
		enterAndLeave(gate, id)
	}
	grown := heapInUse() - before
	runtime.KeepAlive(gate)
	if perEntry := grown / heapEntries; perEntry > admission.EntryHeapBytes {
		t.Errorf("%d entries grew the heap by %d B, %d B an entry, want at most %d", heapEntries, grown, perEntry,
			admission.EntryHeapBytes)
	}
}

func heapInUse() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(min(stats.HeapAlloc, math.MaxInt64))
}
