package record_test

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	testTenant    = "tenant-a"
	testPartition = uint16(3)
	noWeightLimit = int64(1) << 40
)

var writtenAt = time.Date(2026, 11, 12, 10, 0, 0, 123456789, time.UTC)

func traceOf(n uint64) ids.TraceID {
	var trace ids.TraceID
	binary.BigEndian.PutUint64(trace[:8], n*0x9e3779b97f4a7c15|1)
	binary.BigEndian.PutUint64(trace[8:], n)
	return trace
}

func spanIDOf(n uint64) ids.SpanID {
	var id ids.SpanID
	binary.BigEndian.PutUint64(id[:], n)
	return id
}

func endAt(offset time.Duration) uint64 {
	return uint64(writtenAt.Add(offset).UnixNano())
}

// fullSpan sets every optional field so the round trip covers each flag.
func fullSpan(n uint64) span.Span {
	return span.Span{
		Service: resource.ServiceRef{
			Namespace: "shop",
			Name:      "checkout",
		},
		TraceID:      traceOf(n),
		SpanID:       spanIDOf(2*n + 1),
		ParentSpanID: spanIDOf(2 * n),
		Interval:     eventtime.FromUnixNanos(endAt(-5*time.Millisecond), endAt(0)),
		Operation: operation.Operation{
			Protocol: operation.ProtocolHTTP,
			Name:     "GET /api/cart",
		},
		Kind:                kind.Client,
		Failed:              true,
		CallsExternalSystem: true,
	}
}

// minimalSpan has only what span.New requires: ids and a service name.
func minimalSpan(n uint64) span.Span {
	return span.Span{
		Service: resource.ServiceRef{
			Name: "s",
		},
		TraceID: traceOf(n),
		SpanID:  spanIDOf(n + 1),
	}
}

// syntheticSpans is the E0 load shape: services × operations, parents, intervals, ~2 % failures.
func syntheticSpans(count, services, operations int, seed uint64) []span.Span {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	spans := make([]span.Span, 0, count)
	for i := range count {
		service := rng.IntN(services)
		end := endAt(time.Duration(i) * time.Microsecond)
		spans = append(spans, span.Span{
			Service: resource.ServiceRef{
				Namespace: fmt.Sprintf("ns-%02d", service%40),
				Name:      fmt.Sprintf("svc-%03d", service),
			},
			TraceID:      traceOf(uint64(i/8) + seed*1_000_000),
			SpanID:       spanIDOf(uint64(i) + 1),
			ParentSpanID: spanIDOf(uint64(i)),
			Interval:     eventtime.FromUnixNanos(end-uint64(rng.IntN(50_000_000)+1), end),
			Operation: operation.Operation{
				Protocol: operation.ProtocolHTTP,
				Name:     fmt.Sprintf("GET /api/v1/svc%03d/op%03d", service, rng.IntN(operations)),
			},
			Kind:   kind.Kind(2 + i%2),
			Failed: rng.IntN(50) == 0,
		})
	}
	return spans
}

// encoded returns a copy of the record bytes, which publish only borrows.
func encoded(t testing.TB, spans []span.Span) ([]byte, string) {
	t.Helper()
	var data []byte
	var msgID string
	err := new(record.Encoder).Encode(testTenant, testPartition, spans, writtenAt, 7, func(borrowed []byte, id string) error {
		data, msgID = append([]byte(nil), borrowed...), id
		return nil
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return data, msgID
}

func weightOf(spans []span.Span) int64 {
	return batch.Batch{
		Spans: spans,
	}.Footprint()
}

// reseal recomputes the CRC32C exactly as the format defines it, so a test can reach checks past the checksum.
func reseal(data []byte) []byte {
	const offsetCRC, crcBytes = 24, 4
	sealed := append([]byte(nil), data...)
	clear(sealed[offsetCRC : offsetCRC+crcBytes])
	sum := crc32.Checksum(sealed, crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(sealed[offsetCRC:], sum)
	return sealed
}

// rawSpan and rawRecord write format v1 straight from the spec, independently of the encoder,
// so a test can build exactly the malformed record it needs.
type rawSpan struct {
	flags    byte
	kind     byte
	protocol byte
	trace    ids.TraceID
	id       ids.SpanID
	parent   []byte
	end      []byte
	latency  []byte
	indexes  [3]uint64
	rawIndex []byte
}

type rawRecord struct {
	version   byte
	kind      byte
	flags     byte
	tenant    string
	partition uint16
	weight    uint32
	reserved  uint32
	strings   []string
	spans     []rawSpan
	spanCount *uint64
	trailing  []byte

	stringCount  *uint64
	stringLength *uint64
}

// validRawSpan is what validRaw decodes to.
func validRawSpan() span.Span {
	return span.Span{
		Service: resource.ServiceRef{
			Namespace: "shop",
			Name:      "checkout",
		},
		TraceID:  traceOf(1),
		SpanID:   spanIDOf(2),
		Interval: eventtime.FromUnixNanos(endAt(0)-1000, endAt(0)),
		Operation: operation.Operation{
			Protocol: operation.ProtocolHTTP,
			Name:     "GET /api/cart",
		},
		Kind: kind.Server,
	}
}

func validRaw() rawRecord {
	return rawRecord{
		version:   1,
		weight:    uint32(weightOf([]span.Span{validRawSpan()})),
		tenant:    testTenant,
		partition: testPartition,
		strings: []string{
			"shop",
			"checkout",
			"GET /api/cart",
		},
		spans: []rawSpan{
			{
				flags:    2 | 4,
				kind:     byte(kind.Server),
				protocol: byte(operation.ProtocolHTTP),
				trace:    traceOf(1),
				id:       spanIDOf(2),
				end:      binary.LittleEndian.AppendUint64(nil, endAt(0)),
				latency:  binary.AppendUvarint(nil, 1000),
				indexes:  [3]uint64{0, 1, 2},
			},
		},
	}
}

func (r *rawRecord) bytes() []byte {
	out := []byte{'F', 'J', r.version, r.kind, r.flags, byte(len(r.tenant))}
	out = binary.LittleEndian.AppendUint16(out, r.partition)
	out = binary.LittleEndian.AppendUint32(out, r.weight)
	out = binary.LittleEndian.AppendUint64(out, uint64(writtenAt.UnixNano()))
	out = binary.LittleEndian.AppendUint32(out, 0)
	out = binary.LittleEndian.AppendUint32(out, 0)
	out = binary.LittleEndian.AppendUint32(out, r.reserved)
	out = append(out, r.tenant...)
	if r.kind == byte(record.KindHeartbeat) {
		return reseal(append(out, r.trailing...))
	}
	out = binary.AppendUvarint(out, valueOr(r.stringCount, uint64(len(r.strings))))
	for _, value := range r.strings {
		out = binary.AppendUvarint(out, valueOr(r.stringLength, uint64(len(value))))
		out = append(out, value...)
	}
	out = binary.AppendUvarint(out, valueOr(r.spanCount, uint64(len(r.spans))))
	for i := range r.spans {
		out = r.spans[i].append(out)
	}
	return reseal(append(out, r.trailing...))
}

func (s *rawSpan) append(out []byte) []byte {
	out = append(out, s.flags, s.kind, s.protocol)
	out = append(out, s.trace[:]...)
	out = append(out, s.id[:]...)
	out = append(out, s.parent...)
	out = append(out, s.end...)
	out = append(out, s.latency...)
	if s.rawIndex != nil {
		return append(out, s.rawIndex...)
	}
	for _, index := range s.indexes {
		out = binary.AppendUvarint(out, index)
	}
	return out
}

func valueOr(override *uint64, actual uint64) uint64 {
	if override != nil {
		return *override
	}
	return actual
}
