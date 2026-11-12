package record_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

// FuzzRecordDecode: any bytes from the stream either decode or fail with one of the two reader sentinels — never
// a panic, never another error the feed would not know how to route (DLQ or stop).
func FuzzRecordDecode(f *testing.F) {
	valid, _ := encoded(f, syntheticSpans(8, 4, 3, 1))
	heartbeat, err := record.EncodeHeartbeat(testTenant, testPartition, writtenAt)
	if err != nil {
		f.Fatalf("EncodeHeartbeat: %v", err)
	}
	raw := validRaw()
	f.Add(valid)
	f.Add(heartbeat)
	f.Add(raw.bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, candidate := range [][]byte{data, resealIfLong(data)} {
			_, err := readRecord(candidate, noWeightLimit)
			if err != nil && !errors.Is(err, record.ErrCorrupt) && !errors.Is(err, record.ErrUnknownVersion) {
				t.Fatalf("err = %v, want nil, ErrCorrupt or ErrUnknownVersion", err)
			}
		}
	})
}

// FuzzRecordRoundTrip: whatever valid spans go in come back unchanged.
func FuzzRecordRoundTrip(f *testing.F) {
	f.Add(uint8(1), uint64(1), uint8(0))
	f.Add(uint8(64), uint64(7), uint8(0xff))
	f.Fuzz(func(t *testing.T, count uint8, seed uint64, variant uint8) {
		spans := syntheticSpans(int(count)+1, 1+int(variant%7), 1+int(variant%5), seed)
		for i := range spans {
			shape(&spans[i], variant>>uint(i%8))
		}
		data, _ := encoded(t, spans)

		header, err := record.ReadHeader(data, testPartition, noWeightLimit)
		if err != nil {
			t.Fatalf("ReadHeader: %v", err)
		}
		got, err := record.Decode(data, header)

		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if header.Weight != weightOf(spans) {
			t.Fatalf("weight %d differs from batch.Footprint %d", header.Weight, weightOf(spans))
		}
		if diff := cmp.Diff(spans, got); diff != "" {
			t.Fatalf("round trip changed the spans (-want +got):\n%s", diff)
		}
	})
}

func shape(observed *span.Span, bits uint8) {
	full := fullSpan(1)
	if bits&1 != 0 {
		observed.ParentSpanID = full.ParentSpanID
		observed.ParentSpanID[0] = 0
	}
	if bits&2 != 0 {
		observed.Interval = minimalSpan(1).Interval
	}
	observed.Failed = bits&4 != 0
	observed.CallsExternalSystem = bits&8 != 0
	if bits&16 != 0 {
		observed.Operation = minimalSpan(1).Operation
	}
}
