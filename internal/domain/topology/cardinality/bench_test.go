package cardinality_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func benchmarkRegistry(b *testing.B) (known, unknown operation.Operation, lookup func(operation.Operation) bool) {
	b.Helper()
	registry := newRegistry(b, perService, demoOwners)
	for n := range perService {
		if _, ok := registry.Acquire(checkout, path(n)); !ok {
			b.Fatalf("Acquire(%d) refused", n)
		}
	}
	return path(perService / 2), path(uniquePaths), func(op operation.Operation) bool {
		_, ok := registry.Lookup(checkout, op)
		return ok
	}
}

func BenchmarkLookupKnown(b *testing.B) {
	known, _, lookup := benchmarkRegistry(b)
	b.ReportAllocs()
	for b.Loop() {
		lookup(known)
	}
}

func BenchmarkLookupUnknown(b *testing.B) {
	_, unknown, lookup := benchmarkRegistry(b)
	b.ReportAllocs()
	for b.Loop() {
		lookup(unknown)
	}
}
