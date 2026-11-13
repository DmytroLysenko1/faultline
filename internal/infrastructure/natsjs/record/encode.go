package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

const maxSpanBytes = spanPrefixBytes +
	ids.TraceIDBytes +
	2*ids.SpanIDBytes +
	endBytes +
	(1+spanIdentityFields)*binary.MaxVarintLen64

const countFields = 2

const (
	labelAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789-"
	labelHyphen   = "-"
)

type Encoder struct {
	pool sync.Pool
}

type encoding struct {
	record      []byte
	spans       []byte
	indexOf     map[string]uint64
	strings     []string
	headBytes   int
	stringBytes int64
}

func (e *Encoder) Encode(
	tenant string,
	partition uint16,
	spans []span.Span,
	writtenAt time.Time,
	rulesVersion uint32,
	publish Publish,
) error {
	if err := checkBatch(tenant, spans); err != nil {
		return err
	}
	work := e.borrow()
	defer e.release(work)
	data, err := work.encode(Header{
		Version:      Version,
		Kind:         KindBatch,
		Tenant:       tenant,
		Partition:    partition,
		WrittenAt:    writtenAt,
		RulesVersion: rulesVersion,
	}, spans)
	if err != nil {
		return err
	}
	return publish(data, MsgID(tenant, partition, data[HeaderBytes+len(tenant):]))
}

func EncodeHeartbeat(tenant string, partition uint16, writtenAt time.Time) ([]byte, error) {
	if err := ValidateTenant(tenant); err != nil {
		return nil, err
	}
	data := appendHeader(make([]byte, 0, HeaderBytes+len(tenant)), Header{
		Version:   Version,
		Kind:      KindHeartbeat,
		Tenant:    tenant,
		Partition: partition,
		WrittenAt: writtenAt,
	})
	data = append(data, tenant...)
	seal(data)
	return data, nil
}

func checkBatch(tenant string, spans []span.Span) error {
	if err := ValidateTenant(tenant); err != nil {
		return err
	}
	if len(spans) == 0 {
		return ErrEmpty
	}
	return nil
}

func (e *Encoder) borrow() *encoding {
	if work, pooled := e.pool.Get().(*encoding); pooled {
		return work
	}
	return &encoding{
		indexOf: make(map[string]uint64),
	}
}

func (e *Encoder) release(work *encoding) {
	if cap(work.record) > pooledBufferMax || cap(work.spans) > pooledBufferMax {
		return
	}
	e.pool.Put(work)
}

func (w *encoding) encode(header Header, spans []span.Span) ([]byte, error) {
	w.reset(header.Tenant, len(spans))
	for i := range spans {
		w.appendSpan(&spans[i])
		if w.headBytes+len(w.spans) > MaxRecordBytes {
			return nil, fmt.Errorf("%w: %d spans exceed %d bytes", ErrTooLarge, len(spans), MaxRecordBytes)
		}
	}
	header.Weight = int64(len(spans))*spanFootprintBytes + w.stringBytes
	w.record = appendHeader(slices.Grow(w.record[:0], w.headBytes+len(w.spans)), header)
	w.record = append(w.record, header.Tenant...)
	w.record = w.appendStrings(w.record)
	w.record = binary.AppendUvarint(w.record, uint64(len(spans)))
	w.record = append(w.record, w.spans...)
	seal(w.record)
	return w.record, nil
}

func (w *encoding) reset(tenant string, spans int) {
	clear(w.indexOf)
	w.strings = w.strings[:0]
	w.headBytes = HeaderBytes + len(tenant) + countFields*binary.MaxVarintLen64
	w.stringBytes = 0
	w.spans = slices.Grow(w.spans[:0], min(spans*maxSpanBytes, MaxRecordBytes+maxSpanBytes))
}

func (w *encoding) appendSpan(observed *span.Span) {
	end, hasEnd := observed.Interval.End()
	latency, hasLatency := observed.Interval.Latency()
	w.spans = append(w.spans,
		byte(flagsOf(observed, hasEnd, hasLatency)),
		byte(observed.Kind),
		byte(observed.Operation.Protocol),
	)
	w.spans = append(w.spans, observed.TraceID[:]...)
	w.spans = append(w.spans, observed.SpanID[:]...)
	if !observed.ParentSpanID.IsZero() {
		w.spans = append(w.spans, observed.ParentSpanID[:]...)
	}
	if hasEnd {
		w.spans = binary.LittleEndian.AppendUint64(w.spans, unsigned(end.UnixNano()))
	}
	if hasLatency {
		w.spans = binary.AppendUvarint(w.spans, unsigned(int64(latency)))
	}
	w.spans = binary.AppendUvarint(w.spans, w.index(observed.Service.Namespace))
	w.spans = binary.AppendUvarint(w.spans, w.index(observed.Service.Name))
	w.spans = binary.AppendUvarint(w.spans, w.index(observed.Operation.Name))
}

func flagsOf(observed *span.Span, hasEnd, hasLatency bool) spanFlag {
	return flagIf(!observed.ParentSpanID.IsZero(), flagParent) |
		flagIf(hasEnd, flagEnd) |
		flagIf(hasLatency, flagLatency) |
		flagIf(observed.Failed, flagFailed) |
		flagIf(observed.CallsExternalSystem, flagExternal)
}

func flagIf(set bool, flag spanFlag) spanFlag {
	if set {
		return flag
	}
	return 0
}

func (w *encoding) index(value string) uint64 {
	if at, seen := w.indexOf[value]; seen {
		return at
	}
	at := uint64(len(w.strings))
	w.indexOf[value] = at
	w.strings = append(w.strings, value)
	w.headBytes += uvarintBytes(len(value)) + len(value)
	w.stringBytes += int64(len(value))
	return at
}

func (w *encoding) appendStrings(out []byte) []byte {
	out = binary.AppendUvarint(out, uint64(len(w.strings)))
	for _, value := range w.strings {
		out = binary.AppendUvarint(out, uint64(len(value)))
		out = append(out, value...)
	}
	return out
}

func appendHeader(out []byte, header Header) []byte {
	out = append(out,
		magicFirst,
		magicSecond,
		header.Version,
		byte(header.Kind),
		0,
		tenantLength(header.Tenant),
	)
	out = binary.LittleEndian.AppendUint16(out, header.Partition)
	out = binary.LittleEndian.AppendUint32(out, weightField(header.Weight))
	out = binary.LittleEndian.AppendUint64(out, unsigned(header.WrittenAt.UnixNano()))
	out = binary.LittleEndian.AppendUint32(out, header.RulesVersion)
	out = binary.LittleEndian.AppendUint32(out, 0)
	return binary.LittleEndian.AppendUint32(out, 0)
}

func seal(record []byte) {
	binary.LittleEndian.PutUint32(record[offsetCRC:], checksum(record))
}

func checksum(record []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	var zero [crcBytes]byte
	sum := crc32.Update(0, table, record[:offsetCRC])
	sum = crc32.Update(sum, table, zero[:])
	return crc32.Update(sum, table, record[offsetCRC+crcBytes:])
}

func ValidateTenant(tenant string) error {
	if !lengthFits(tenant) || !isLabel(tenant) {
		return fmt.Errorf("%w: %q is not a DNS label of at most %d bytes", ErrInvalidTenant, tenant, MaxTenantBytes)
	}
	return nil
}

func lengthFits(tenant string) bool {
	return len(tenant) > 0 && len(tenant) <= MaxTenantBytes
}

func isLabel(tenant string) bool {
	return strings.Trim(tenant, labelAlphabet) == "" &&
		!strings.HasPrefix(tenant, labelHyphen) &&
		!strings.HasSuffix(tenant, labelHyphen)
}

func tenantLength(tenant string) byte {
	length := len(tenant)
	if length < 0 || length > math.MaxUint8 {
		return 0
	}
	return byte(length)
}

func weightField(weight int64) uint32 {
	if weight < 0 || weight > math.MaxUint32 {
		return 0
	}
	return uint32(weight)
}

func uvarintBytes(value int) int {
	var scratch [binary.MaxVarintLen64]byte
	return binary.PutUvarint(scratch[:], unsigned(int64(value)))
}

func unsigned(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
