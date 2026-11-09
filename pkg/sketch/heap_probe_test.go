//go:build !race

package sketch_test

import (
	"runtime"
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

const overestimateSlackBytes = 32 << 10

func TestWorstCaseHeapBytesBoundsTheMeasuredArena(t *testing.T) {
	for _, budget := range []int{smallBudget, defaultBudget} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		arena := newArena(t, budget)
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(arena)

		measured := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		bound := sketch.WorstCaseHeapBytes(budget)
		if measured > bound {
			t.Errorf("budget %d: measured heap %d B > WorstCaseHeapBytes %d B", budget, measured, bound)
		}
		if slack := measured/100 + overestimateSlackBytes; bound > measured+slack {
			t.Errorf("budget %d: WorstCaseHeapBytes %d B overestimates the measured %d B by more than %d B", budget,
				bound, measured, slack)
		}
	}
}
