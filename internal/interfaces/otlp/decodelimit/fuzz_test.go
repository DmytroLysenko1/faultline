package decodelimit_test

import (
	"errors"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

func FuzzWeighNeverPanics(f *testing.F) {
	for _, line := range otlptest.DemoLines(f) {
		f.Add(otlptest.Encode(f, line))
	}
	for _, hostile := range hostileRequests() {
		f.Add(hostile[:min(len(hostile), maxChargeBytes)])
	}
	f.Add(nestedArrays(realDepth + 1))
	f.Fuzz(func(t *testing.T, wire []byte) {
		weight, err := production().Weigh(wire)
		known := errors.Is(err, decodelimit.ErrTooHeavy) ||
			errors.Is(err, decodelimit.ErrTooDeep) ||
			errors.Is(err, decodelimit.ErrMalformed)
		if err != nil && !known {
			t.Fatalf("Weigh returned an unclassified error %v", err)
		}
		if weight < 0 {
			t.Fatalf("Weigh returned a negative weight %d", weight)
		}
	})
}
