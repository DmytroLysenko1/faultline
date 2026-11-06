package decodelimit_test

import (
	"errors"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
)

const (
	probeCopies      = 8_000
	maxProbedField   = 32
	deprecatedField  = 1000
	spreadWireTarget = 256 << 10
	bombBytes        = 4 << 20
	emptyMessage     = 2
	maxChargeBytes   = 4096
)

type path func(inner []byte) []byte

func inRequest(inner []byte) []byte {
	return inner
}

func inResourceSpans(inner []byte) []byte {
	return message(1, inner)
}

func inScopeSpans(inner []byte) []byte {
	return message(1, message(2, inner))
}

func inSpan(inner []byte) []byte {
	return message(1, message(2, message(2, inner)))
}

func inSpanAttribute(inner []byte) []byte {
	return inSpan(message(9, inner))
}

func inArrayValue(inner []byte) []byte {
	return inSpanAttribute(message(2, message(5, inner)))
}

func TestLegitimateRequestsPassUnderTheRealLimit(t *testing.T) {
	for label, wire := range map[string][]byte{
		"largest request of minimal spans": minimalSpansRequest(t),
		"OTel Demo request":                demoRequest(t),
	} {
		t.Run(label, func(t *testing.T) {
			assertDecodes(t, label, wire)
			if _, err := production().Weigh(wire); err != nil {
				t.Errorf("legitimate %d-byte request rejected: %v", len(wire), err)
			}
		})
	}
}

func hostileRequests() map[string][]byte {
	return map[string][]byte{
		"empty spans":                 inScopeSpans(repeat(message(2, nil), bombBytes/emptyMessage)),
		"empty spans through 1000":    message(1, message(deprecatedField, repeat(message(2, nil), bombBytes/emptyMessage))),
		"empty entity refs":           message(1, message(1, repeat(message(3, nil), bombBytes/emptyMessage))),
		"array of empty bytes values": inArrayValue(repeat(message(1, message(7, nil)), bombBytes/(2*emptyMessage))),
		"scopes of 36 attributes": inResourceSpans(repeat(
			message(2, message(1, repeat(message(3, nil), 36))), bombBytes/80)),
	}
}

func TestHostileRequestsAreRejectedBeforeDecoding(t *testing.T) {
	for label, wire := range hostileRequests() {
		t.Run(label, func(t *testing.T) {
			got, err := production().Weigh(wire)
			if !errors.Is(err, decodelimit.ErrTooHeavy) {
				t.Fatalf("err = %v, want %v", err, decodelimit.ErrTooHeavy)
			}
			if got > realLimit+maxChargeBytes {
				t.Errorf("walk stopped at weight %d, want it to stop right after the %d limit", got, realLimit)
			}
		})
	}
}

func nestedArrays(depth int) []byte {
	value := message(3, nil)
	for range depth - 1 {
		value = message(5, message(1, value))
	}
	return inSpan(message(9, message(2, value)))
}

func TestNestingDepthLimit(t *testing.T) {
	if _, err := production().Weigh(nestedArrays(realDepth)); err != nil {
		t.Errorf("depth %d rejected: %v", realDepth, err)
	}
	if _, err := production().Weigh(nestedArrays(realDepth + 1)); !errors.Is(err, decodelimit.ErrTooDeep) {
		t.Errorf("depth %d: err = %v, want %v", realDepth+1, err, decodelimit.ErrTooDeep)
	}
}

func TestTruncatedWireIsMalformed(t *testing.T) {
	valid := inSpan(message(5, text(emptyMessage)))
	for cut := 1; cut < len(valid); cut++ {
		if _, err := production().Weigh(valid[:cut]); err != nil && !errors.Is(err, decodelimit.ErrMalformed) {
			t.Errorf("cut at %d: err = %v, want nil or %v", cut, err, decodelimit.ErrMalformed)
		}
	}
	if _, err := production().Weigh(valid[:len(valid)-1]); !errors.Is(err, decodelimit.ErrMalformed) {
		t.Errorf("request missing its last byte: err = %v, want %v", err, decodelimit.ErrMalformed)
	}
}
