package record_test

import (
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

// readRecord runs the reader the way the feed will: header first, then the body.
func readRecord(data []byte, maxWeight int64) ([]span.Span, error) {
	header, err := record.ReadHeader(data, testPartition, maxWeight)
	if err != nil {
		return nil, err
	}
	return record.Decode(data, header)
}

func TestTheSpecRecordDecodesToItsSpan(t *testing.T) {
	raw := validRaw()
	got, err := readRecord(raw.bytes(), noWeightLimit)
	if err != nil {
		t.Fatalf("a record written from the spec does not decode: %v", err)
	}
	if diff := cmp.Diff([]span.Span{validRawSpan()}, got); diff != "" {
		t.Errorf("spec record decodes differently (-want +got):\n%s", diff)
	}
}

func corruptions() []struct {
	name   string
	mutate func(*rawRecord)
} {
	return []struct {
		name   string
		mutate func(*rawRecord)
	}{
		{name: "string index past the table", mutate: func(r *rawRecord) { r.spans[0].indexes[2] = 3 }},
		{name: "zero trace id", mutate: func(r *rawRecord) { r.spans[0].trace = ids.TraceID{} }},
		{name: "zero span id", mutate: func(r *rawRecord) { r.spans[0].id = ids.SpanID{} }},
		{name: "parent flag with a zero parent", mutate: func(r *rawRecord) {
			r.spans[0].flags |= 1
			r.spans[0].parent = make([]byte, ids.SpanIDBytes)
		}},
		{name: "reserved overflow operation name", mutate: func(r *rawRecord) { r.strings[2] = operation.OverflowName }},
		{name: "operation name without a protocol", mutate: func(r *rawRecord) { r.spans[0].protocol = 0 }},
		{name: "unknown protocol", mutate: func(r *rawRecord) { r.spans[0].protocol = 9 }},
		{name: "unknown span kind", mutate: func(r *rawRecord) { r.spans[0].kind = 6 }},
		{name: "empty service name", mutate: func(r *rawRecord) { r.strings[1] = "" }},
		{name: "invalid UTF-8 operation name", mutate: func(r *rawRecord) { r.strings[2] = "\xff\xfe" }},
		{name: "latency without an end", mutate: func(r *rawRecord) {
			r.spans[0].flags = 4
			r.spans[0].end = nil
		}},
		{name: "latency not shorter than the end", mutate: func(r *rawRecord) {
			r.spans[0].latency = binary.AppendUvarint(nil, endAt(0))
		}},
		{name: "end past int64", mutate: func(r *rawRecord) {
			r.spans[0].end = binary.LittleEndian.AppendUint64(nil, math.MaxUint64)
		}},
		{name: "zero end with the end flag", mutate: func(r *rawRecord) {
			r.spans[0].flags = 2
			r.spans[0].latency = nil
			r.spans[0].end = make([]byte, 8)
		}},
		{name: "trailing bytes after the last span", mutate: func(r *rawRecord) { r.trailing = []byte{0} }},
		{name: "zero spans in a batch", mutate: func(r *rawRecord) { r.spans = nil }},
		{name: "span count beyond the bytes", mutate: func(r *rawRecord) { r.spanCount = new(uint64(1 << 40)) }},
		{name: "string count beyond the bytes", mutate: func(r *rawRecord) { r.stringCount = new(uint64(1 << 40)) }},
		{name: "string longer than the record", mutate: func(r *rawRecord) { r.stringLength = new(uint64(1 << 20)) }},
		{name: "weight differs from the decoded footprint", mutate: func(r *rawRecord) { r.weight++ }},
		{name: "a table string nobody uses", mutate: func(r *rawRecord) {
			r.spans = append(r.spans, r.spans[0])
			r.spans[1].id = spanIDOf(9)
			r.strings = append(r.strings, "unused")
			r.weight += uint32(256 + len("unused"))
		}},
		{name: "table strings used out of first-appearance order", mutate: func(r *rawRecord) {
			r.strings = []string{"a", "b", "c"}
			r.spans = append(r.spans, r.spans[0])
			r.spans[1].id = spanIDOf(9)
			r.spans[0].indexes = [3]uint64{1, 0, 1}
			r.spans[1].indexes = [3]uint64{2, 2, 2}
			r.weight = 2*256 + 3
		}},
		{name: "span count that overflows the footprint", mutate: func(r *rawRecord) { r.spanCount = new(uint64(1<<56 + 1)) }},
		{name: "a repeated table string, every copy used", mutate: func(r *rawRecord) {
			r.spans = append(r.spans, r.spans[0])
			r.spans[1].id = spanIDOf(9)
			r.strings = append(r.strings, r.strings...)
			r.spans[1].indexes = [3]uint64{3, 4, 5}
			r.weight += uint32(256 + len("shop") + len("checkout") + len("GET /api/cart"))
		}},
		{name: "a repeated empty table string", mutate: func(r *rawRecord) {
			r.spans = append(r.spans, r.spans[0])
			r.spans[1].id = spanIDOf(9)
			r.strings = []string{"", "checkout", "GET /api/cart", ""}
			r.spans[1].indexes = [3]uint64{3, 1, 2}
			r.weight = 2*256 + uint32(len("checkout")+len("GET /api/cart"))
		}},
		{name: "non-minimal varint index", mutate: func(r *rawRecord) { r.spans[0].rawIndex = []byte{0x80, 0x00, 1, 2} }},
		{name: "batch without weight", mutate: func(r *rawRecord) { r.weight = 0 }},
		{name: "record of another partition", mutate: func(r *rawRecord) { r.partition = testPartition + 1 }},
		{name: "empty tenant", mutate: func(r *rawRecord) { r.tenant = "" }},
		{name: "tenant that is not a DNS label", mutate: func(r *rawRecord) { r.tenant = "Tenant A" }},
		{name: "heartbeat with a body", mutate: func(r *rawRecord) {
			r.kind = byte(record.KindHeartbeat)
			r.weight = 0
			r.trailing = []byte{1}
		}},
		{name: "heartbeat with a weight", mutate: func(r *rawRecord) {
			r.kind = byte(record.KindHeartbeat)
			r.weight = 1
		}},
	}
}

func TestEveryCorruptionIsErrCorrupt(t *testing.T) {
	for _, tt := range corruptions() {
		t.Run(tt.name, func(t *testing.T) {
			raw := validRaw()
			tt.mutate(&raw)

			_, err := readRecord(raw.bytes(), noWeightLimit)

			if !errors.Is(err, record.ErrCorrupt) || errors.Is(err, record.ErrUnknownVersion) {
				t.Errorf("err = %v, want ErrCorrupt and not ErrUnknownVersion", err)
			}
		})
	}
}

func TestEnvelopeDamageIsErrCorrupt(t *testing.T) {
	valid, _ := encoded(t, []span.Span{fullSpan(1)})
	badMagic := reseal(valid)
	badMagic[0] = 'X'
	badMagic = reseal(badMagic)
	flipped := append([]byte(nil), valid...)
	flipped[len(flipped)-1] ^= 1
	tests := []struct {
		name      string
		data      []byte
		maxWeight int64
	}{
		{name: "empty", data: nil, maxWeight: noWeightLimit},
		{name: "shorter than the header", data: valid[:record.HeaderBytes-1], maxWeight: noWeightLimit},
		{name: "bad magic", data: badMagic, maxWeight: noWeightLimit},
		{name: "one flipped body bit without a new checksum", data: flipped, maxWeight: noWeightLimit},
		{name: "weight above the largest batch footprint", data: valid, maxWeight: weightOf([]span.Span{fullSpan(1)}) - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readRecord(tt.data, tt.maxWeight)

			if !errors.Is(err, record.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

// TestATruncatedRecordIsErrCorruptAtEveryLength cuts a valid record at every byte, with and without a fresh
// checksum, so every field boundary of the header, tenant, string table and spans is crossed.
func TestATruncatedRecordIsErrCorruptAtEveryLength(t *testing.T) {
	valid, _ := encoded(t, syntheticSpans(4, 3, 3, 9))
	for cut := range len(valid) {
		for _, data := range [][]byte{valid[:cut], resealIfLong(valid[:cut])} {
			_, err := readRecord(data, noWeightLimit)
			if !errors.Is(err, record.ErrCorrupt) {
				t.Fatalf("record cut to %d of %d bytes: err = %v, want ErrCorrupt", cut, len(valid), err)
			}
		}
	}
}

func resealIfLong(data []byte) []byte {
	if len(data) < record.HeaderBytes {
		return data
	}
	return reseal(data)
}

// TestAFlippedVersionByteIsCorruptNotUnknown: the checksum is checked before the version, so a damaged
// version byte sends the record to the DLQ instead of stopping the partition forever.
func TestAFlippedVersionByteIsCorruptNotUnknown(t *testing.T) {
	valid, _ := encoded(t, []span.Span{fullSpan(1)})
	damaged := append([]byte(nil), valid...)
	damaged[2] ^= 0b10

	_, err := readRecord(damaged, noWeightLimit)

	if !errors.Is(err, record.ErrCorrupt) || errors.Is(err, record.ErrUnknownVersion) {
		t.Errorf("err = %v, want ErrCorrupt and not ErrUnknownVersion", err)
	}
}

// TestAFutureFormatIsUnknownNotCorrupt: with a valid checksum an unknown version, kind or flag bit is a newer
// writer, so the reader must stop rather than skip the record as garbage.
func TestAFutureFormatIsUnknownNotCorrupt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*rawRecord)
	}{
		{name: "version 2", mutate: func(r *rawRecord) { r.version = 2 }},
		{name: "kind 2", mutate: func(r *rawRecord) { r.kind = 2 }},
		{name: "header flag bit", mutate: func(r *rawRecord) { r.flags = 1 }},
		{name: "reserved header field", mutate: func(r *rawRecord) { r.reserved = 1 }},
		{name: "unknown span flag bit", mutate: func(r *rawRecord) { r.spans[0].flags |= 32 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := validRaw()
			tt.mutate(&raw)

			_, err := readRecord(raw.bytes(), noWeightLimit)

			if !errors.Is(err, record.ErrUnknownVersion) {
				t.Errorf("err = %v, want ErrUnknownVersion", err)
			}
		})
	}
}

// TestCountsBeyondTheWeightAllocateNothing: string and span counts are bounded by the header weight before any
// allocation, so a record that claims a small footprint cannot make the processor allocate past its credit.
func TestCountsBeyondTheWeightAllocateNothing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*rawRecord)
	}{
		{name: "five million table strings", mutate: func(r *rawRecord) {
			r.strings = make([]string, 5<<20)
			r.spans = nil
		}},
		{name: "a span count matching a huge weight with no bytes behind it", mutate: func(r *rawRecord) {
			r.strings = nil
			r.spans = nil
			r.spanCount = new(uint64(400_000))
			r.weight = 400_000 * 256
		}},
		{name: "a billion spans", mutate: func(r *rawRecord) { r.spanCount = new(uint64(1 << 30)) }},
		{name: "a five megabyte table string", mutate: func(r *rawRecord) { r.strings[2] = strings.Repeat("o", 5<<20) }},
		{name: "a tiny record claiming a huge weight", mutate: func(r *rawRecord) {
			r.weight = 1 << 31
			r.stringCount = new(uint64(1 << 31 / 256 * 3))
			r.strings = nil
			r.spans = nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := validRaw()
			tt.mutate(&raw)
			data := raw.bytes()
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			_, err := readRecord(data, noWeightLimit)

			runtime.ReadMemStats(&after)
			if !errors.Is(err, record.ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
				t.Errorf("rejecting the record allocated %d KiB, want under 1 MiB", allocated>>10)
			}
		})
	}
}

func TestReadHeaderRefusesARecordLongerThanTheGuard(t *testing.T) {
	valid, _ := encoded(t, []span.Span{fullSpan(1)})
	oversized := reseal(append(append([]byte(nil), valid...), make([]byte, record.MaxRecordBytes)...))

	_, err := record.ReadHeader(oversized, testPartition, noWeightLimit)

	if !errors.Is(err, record.ErrCorrupt) {
		t.Errorf("err = %v, want ErrCorrupt before reading the body", err)
	}
}

func TestDecodeRefusesAHeaderOfOtherBytes(t *testing.T) {
	data, _ := encoded(t, []span.Span{fullSpan(1)})
	header, err := record.ReadHeader(data, testPartition, noWeightLimit)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	header.Tenant = "tenant-b"

	spans, err := record.Decode(data, header)

	if !errors.Is(err, record.ErrCorrupt) || spans != nil {
		t.Errorf("Decode = %d spans, %v; want nil and ErrCorrupt", len(spans), err)
	}
}
