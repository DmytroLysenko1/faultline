package decodelimit_test

import (
	"runtime"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	unboundedWeight = 1 << 40
	realLimit       = 32 << 20
	realDepth       = 32
)

func unbounded() *decodelimit.Weigher {
	return decodelimit.NewWeigher(decodelimit.Limits{
		MaxDecodedBytes: unboundedWeight,
		MaxNestingDepth: realDepth,
	})
}

func production() *decodelimit.Weigher {
	return decodelimit.NewWeigher(decodelimit.Limits{
		MaxDecodedBytes: realLimit,
		MaxNestingDepth: realDepth,
	})
}

func decodedHeap(wire []byte) (int64, bool) {
	before := otlptest.HeapInUse()
	request := ptraceotlp.NewExportRequest()
	if err := request.UnmarshalProto(wire); err != nil {
		return 0, false
	}
	grown := otlptest.HeapInUse() - before
	runtime.KeepAlive(request)
	return grown, true
}

func weight(t *testing.T, weigher *decodelimit.Weigher, wire []byte) int64 {
	t.Helper()
	got, err := weigher.Weigh(wire)
	if err != nil {
		t.Fatalf("Weigh: %v", err)
	}
	return got
}

func assertBound(t *testing.T, label string, wire []byte) bool {
	t.Helper()
	actual, decoded := decodedHeap(wire)
	if !decoded {
		return false
	}
	if bound := weight(t, unbounded(), wire); actual > bound {
		t.Errorf("%s: pdata built %d bytes, weight %d: the weight is not an upper bound", label, actual, bound)
	}
	return true
}

func assertDecodes(t *testing.T, label string, wire []byte) {
	t.Helper()
	if _, decoded := decodedHeap(wire); !decoded {
		t.Fatalf("%s: pdata rejected a legitimate request", label)
	}
	assertBound(t, label, wire)
}

func message(number protowire.Number, body []byte) []byte {
	return otlptest.Message(number, body)
}

func varint(number protowire.Number) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, number, protowire.VarintType), 0)
}

func repeat(part []byte, count int) []byte {
	return otlptest.Repeat(part, count)
}

func text(length int) []byte {
	out := make([]byte, length)
	for index := range out {
		out[index] = 'a'
	}
	return out
}

func minimalSpansRequest(t *testing.T) []byte {
	t.Helper()
	return otlptest.Encode(t, otlptest.MinimalSpans(t))
}

func demoRequest(t *testing.T) []byte {
	t.Helper()
	return otlptest.Encode(t, otlptest.DemoSpans(t))
}
