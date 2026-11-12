package record_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

func decoded(t *testing.T, data []byte) (record.Header, []span.Span) {
	t.Helper()
	header, err := record.ReadHeader(data, testPartition, noWeightLimit)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	spans, err := record.Decode(data, header)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return header, spans
}

func TestDecodeReturnsTheEncodedSpans(t *testing.T) {
	endOnly := minimalSpan(5)
	endOnly.Interval = eventtime.FromUnixNanos(0, endAt(0))
	zeroLatency := minimalSpan(6)
	zeroLatency.Interval = eventtime.FromUnixNanos(endAt(0), endAt(0))
	longest := fullSpan(7)
	longest.Service.Name = strings.Repeat("ж", resource.MaxServiceNameBytes/2)
	longest.Operation.Name = strings.Repeat("o", operation.MaxNameBytes)
	everyKind := make([]span.Span, 0, kind.Consumer+1)
	for k := range kind.Consumer + 1 {
		observed := minimalSpan(uint64(10 + k))
		observed.Kind = k
		everyKind = append(everyKind, observed)
	}
	tests := []struct {
		name  string
		spans []span.Span
	}{
		{name: "one span with only the required fields", spans: []span.Span{minimalSpan(1)}},
		{name: "every optional field and flag set", spans: []span.Span{fullSpan(2)}},
		{name: "end known, latency unknown", spans: []span.Span{endOnly}},
		{name: "zero latency keeps start equal to end", spans: []span.Span{zeroLatency}},
		{name: "longest service and operation names, multibyte UTF-8", spans: []span.Span{longest}},
		{name: "every span kind", spans: everyKind},
		{name: "synthetic 500 x 200 load shape", spans: syntheticSpans(512, 500, 200, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, _ := encoded(t, tt.spans)

			header, got := decoded(t, data)

			if diff := cmp.Diff(tt.spans, got); diff != "" {
				t.Errorf("decoded spans mismatch (-want +got):\n%s", diff)
			}
			if header.Weight != weightOf(tt.spans) {
				t.Errorf("weight = %d, want the footprint %d", header.Weight, weightOf(tt.spans))
			}
		})
	}
}

func TestReadHeaderReturnsWhatTheWriterStamped(t *testing.T) {
	spans := []span.Span{fullSpan(1)}
	data, _ := encoded(t, spans)

	got, _ := decoded(t, data)

	want := record.Header{
		Version:      record.Version,
		Kind:         record.KindBatch,
		Tenant:       testTenant,
		Partition:    testPartition,
		Weight:       weightOf(spans),
		WrittenAt:    writtenAt,
		RulesVersion: 7,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("header mismatch (-want +got):\n%s", diff)
	}
}

func TestEncodingIsAFunctionOfTheSpans(t *testing.T) {
	spans := syntheticSpans(256, 50, 20, 3)
	first, firstID := encoded(t, spans)
	reused := new(record.Encoder)
	var second []byte
	var secondID string
	for range 3 {
		later := writtenAt.Add(time.Hour)
		err := reused.Encode(testTenant, testPartition, spans, later, 99, func(data []byte, id string) error {
			second, secondID = append(second[:0], data...), id
			return nil
		})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}

	bodyStart := record.HeaderBytes + len(testTenant)
	if !bytes.Equal(first[bodyStart:], second[bodyStart:]) {
		t.Errorf("the same spans gave different bodies")
	}
	if firstID != secondID {
		t.Errorf("msg id changed with the writer stamp or rules version: %s vs %s", firstID, secondID)
	}
}

func TestMsgIDIsTheSpecifiedHashOfTenantPartitionAndBody(t *testing.T) {
	data, gotID := encoded(t, syntheticSpans(64, 10, 5, 4))
	body := data[record.HeaderBytes+len(testTenant):]
	hasher := sha256.New()
	hasher.Write([]byte(testTenant))
	hasher.Write([]byte{0})
	hasher.Write(binary.LittleEndian.AppendUint16(nil, testPartition))
	hasher.Write([]byte{0})
	hasher.Write(body)

	want := hex.EncodeToString(hasher.Sum(nil)[:16])

	if gotID != want || len(gotID) != record.MsgIDLength {
		t.Errorf("msg id = %q, want %q (%d hex characters)", gotID, want, record.MsgIDLength)
	}
}

func TestMsgIDSeparatesTenantsAndPartitions(t *testing.T) {
	body := []byte("the same body")
	base := record.MsgID("tenant-a", 1, body)
	tests := []struct {
		name string
		got  string
	}{
		{name: "another tenant", got: record.MsgID("tenant-b", 1, body)},
		{name: "another partition", got: record.MsgID("tenant-a", 2, body)},
		{name: "tenant prefix shifted into the partition", got: record.MsgID("tenant-a\x00", 1, body[1:])},
		{name: "another body", got: record.MsgID("tenant-a", 1, []byte("another body"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got == base {
				t.Errorf("msg id %s collides with the base record", tt.got)
			}
		})
	}
}

func TestAHeartbeatIsAnEmptyRecordWithItsStamp(t *testing.T) {
	data, err := record.EncodeHeartbeat(testTenant, testPartition, writtenAt)
	if err != nil {
		t.Fatalf("EncodeHeartbeat: %v", err)
	}

	header, spans := decoded(t, data)

	if header.Kind != record.KindHeartbeat || header.Weight != 0 || !header.WrittenAt.Equal(writtenAt) {
		t.Errorf("heartbeat header = %+v, want kind heartbeat, weight 0, stamp %v", header, writtenAt)
	}
	if len(spans) != 0 || len(data) != record.HeaderBytes+len(testTenant) {
		t.Errorf("heartbeat carries %d spans and %d bytes, want none beyond the header and tenant", len(spans), len(data))
	}
}

func TestEncodeRejectsWhatCannotBeARecord(t *testing.T) {
	one := []span.Span{minimalSpan(1)}
	tests := []struct {
		name    string
		tenant  string
		spans   []span.Span
		wantErr error
	}{
		{name: "empty tenant", tenant: "", spans: one, wantErr: record.ErrInvalidTenant},
		{name: "upper case tenant", tenant: "Tenant", spans: one, wantErr: record.ErrInvalidTenant},
		{name: "tenant starting with a hyphen", tenant: "-a", spans: one, wantErr: record.ErrInvalidTenant},
		{name: "tenant ending with a hyphen", tenant: "a-", spans: one, wantErr: record.ErrInvalidTenant},
		{
			name:    "tenant longer than a DNS label",
			tenant:  strings.Repeat("a", record.MaxTenantBytes+1),
			spans:   one,
			wantErr: record.ErrInvalidTenant,
		},
		{name: "no spans", tenant: testTenant, spans: nil, wantErr: record.ErrEmpty},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			published := false

			err := new(record.Encoder).Encode(tt.tenant, 0, tt.spans, writtenAt, 0, func([]byte, string) error {
				published = true
				return nil
			})

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if published {
				t.Errorf("a rejected record reached publish")
			}
		})
	}
}

func TestEncodeReturnsThePublishError(t *testing.T) {
	failure := errors.New("stream unavailable")

	err := new(record.Encoder).Encode(testTenant, 0, []span.Span{minimalSpan(1)}, writtenAt, 0, func([]byte, string) error {
		return failure
	})

	if !errors.Is(err, failure) {
		t.Errorf("err = %v, want the publish error", err)
	}
}

// TestConcurrentEncodesNeverShareABuffer: every publish must see exactly its own record while other goroutines
// encode on the same encoder (run with -race: a buffer handed to two calls is a data race).
func TestConcurrentEncodesNeverShareABuffer(t *testing.T) {
	encoder := new(record.Encoder)
	var wg sync.WaitGroup
	failures := make(chan string, 64)
	for worker := range 8 {
		wg.Go(func() {
			for round := range 50 {
				spans := syntheticSpans(16+worker, 20, 10, uint64(worker*1000+round))
				err := encoder.Encode(testTenant, testPartition, spans, writtenAt, 0, func(data []byte, _ string) error {
					header, err := record.ReadHeader(data, testPartition, noWeightLimit)
					if err != nil {
						return err
					}
					got, err := record.Decode(data, header)
					if err != nil {
						return err
					}
					if !cmp.Equal(spans, got) {
						return errors.New("published bytes are another call's record")
					}
					return nil
				})
				if err != nil {
					failures <- err.Error()
					return
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Errorf("worker failed: %s", failure)
	}
}
