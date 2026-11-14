package atomicmax_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/atomicmax"
)

const raisers = 64

func TestRaiseKeepsTheLargestValue(t *testing.T) {
	tests := []struct {
		name  string
		start uint64
		value uint64
		want  uint64
	}{
		{name: "a larger value", start: 5, value: 9, want: 9},
		{name: "a smaller value", start: 9, value: 5, want: 9},
		{name: "an equal value", start: 9, value: 9, want: 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var value atomic.Uint64
			value.Store(tt.start)

			atomicmax.Raise(&value, tt.value)

			if got := value.Load(); got != tt.want {
				t.Errorf("value %d, want %d", got, tt.want)
			}
		})
	}
}

func TestConcurrentRaisesLoseNoMaximum(t *testing.T) {
	var value atomic.Int64
	var raising sync.WaitGroup

	for index := range int64(raisers) {
		raising.Go(func() { atomicmax.Raise(&value, index) })
	}
	raising.Wait()

	if got := value.Load(); got != raisers-1 {
		t.Errorf("value %d, want the largest %d", got, raisers-1)
	}
}
