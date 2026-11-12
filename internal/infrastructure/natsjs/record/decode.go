package record

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

const (
	spanPrefixBytes    = 3
	spanIdentityFields = 3
	endBytes           = 8
	spanFootprintBytes = 256
	minSpanBytes       = spanPrefixBytes + ids.TraceIDBytes + ids.SpanIDBytes + spanIdentityFields
)

func Decode(data []byte, header Header) ([]span.Span, error) {
	if header.Kind == KindHeartbeat {
		return nil, nil
	}
	body, err := bodyOf(data, header)
	if err != nil {
		return nil, err
	}
	reader := cursor{
		data:   body,
		weight: unsigned(header.Weight),
	}
	table := reader.stringTable()
	spans := reader.spans(table)
	if err := reader.finish(table); err != nil {
		return nil, err
	}
	return spans, nil
}

func bodyOf(data []byte, header Header) ([]byte, error) {
	bodyStart := HeaderBytes + len(header.Tenant)
	if bodyStart > len(data) || string(data[HeaderBytes:bodyStart]) != header.Tenant {
		return nil, fmt.Errorf("%w: header does not belong to these bytes", ErrCorrupt)
	}
	return data[bodyStart:], nil
}

type cursor struct {
	data        []byte
	err         error
	weight      uint64
	stringBytes uint64
	referenced  uint64
}

func (c *cursor) fail(cause error, reason string) {
	if c.err == nil {
		c.err = fmt.Errorf("%w: %s", cause, reason)
	}
}

func (c *cursor) finish(table []string) error {
	switch {
	case c.err != nil:
		return c.err
	case len(c.data) != 0:
		return fmt.Errorf("%w: trailing bytes after the last span", ErrCorrupt)
	case c.referenced != uint64(len(table)):
		return fmt.Errorf("%w: only %d of %d table strings are used", ErrCorrupt, c.referenced, len(table))
	}
	return nil
}

func (c *cursor) take(count uint64) []byte {
	if c.err != nil || count > uint64(len(c.data)) {
		c.fail(ErrCorrupt, "truncated")
		return nil
	}
	taken := c.data[:count]
	c.data = c.data[count:]
	return taken
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	value, read := binary.Uvarint(c.data)
	if read <= 0 || read > 1 && c.data[read-1] == 0 {
		c.fail(ErrCorrupt, "bad or non-minimal varint")
		return 0
	}
	c.data = c.data[read:]
	return value
}

func (c *cursor) readByte() byte {
	if taken := c.take(1); taken != nil {
		return taken[0]
	}
	return 0
}

func (c *cursor) fixed64() uint64 {
	if taken := c.take(endBytes); taken != nil {
		return binary.LittleEndian.Uint64(taken)
	}
	return 0
}

func (c *cursor) stringTable() []string {
	count := c.uvarint()
	if count > c.weight/spanFootprintBytes*spanIdentityFields || count > uint64(len(c.data)) {
		c.fail(ErrCorrupt, "more table strings than the weight or the bytes allow")
		return nil
	}
	table := make([]string, 0, count)
	distinct := make(map[string]struct{}, count)
	for range count {
		value := c.text()
		if _, repeated := distinct[value]; repeated {
			c.fail(ErrCorrupt, "duplicate table string")
			return nil
		}
		distinct[value] = struct{}{}
		table = append(table, value)
	}
	return table
}

func (c *cursor) text() string {
	length := c.uvarint()
	if length > c.weight-c.stringBytes {
		c.fail(ErrCorrupt, "table strings outweigh the record")
		return ""
	}
	c.stringBytes += length
	return string(c.take(length))
}

func (c *cursor) spans(table []string) []span.Span {
	count := c.uvarint()
	if !c.weighs(count) {
		return nil
	}
	spans := make([]span.Span, count)
	for i := range spans {
		c.span(&spans[i], table)
	}
	return spans
}

func (c *cursor) weighs(count uint64) bool {
	if c.err != nil || !c.fits(count) {
		c.fail(ErrCorrupt, "span count does not fit the weight or the bytes")
		return false
	}
	if count*spanFootprintBytes+c.stringBytes != c.weight {
		c.fail(ErrCorrupt, "weight differs from the decoded footprint")
		return false
	}
	return true
}

func (c *cursor) fits(spans uint64) bool {
	return spans > 0 && spans <= uint64(len(c.data)/minSpanBytes)
}

func (c *cursor) span(out *span.Span, table []string) {
	flags := spanFlag(c.readByte())
	out.Kind = kind.Kind(c.readByte())
	out.Operation.Protocol = operation.Protocol(c.readByte())
	if flags&^knownSpanFlags != 0 {
		c.fail(ErrUnknownVersion, "span flags")
	}
	copy(out.TraceID[:], c.take(ids.TraceIDBytes))
	copy(out.SpanID[:], c.take(ids.SpanIDBytes))
	if flags&flagParent != 0 {
		copy(out.ParentSpanID[:], c.take(ids.SpanIDBytes))
	}
	out.Interval = c.interval(flags)
	out.Service.Namespace = c.lookup(table)
	out.Service.Name = c.lookup(table)
	out.Operation.Name = c.lookup(table)
	out.Failed = flags&flagFailed != 0
	out.CallsExternalSystem = flags&flagExternal != 0
	c.validate(out, flags)
}

func (c *cursor) interval(flags spanFlag) eventtime.Interval {
	end := c.end(flags)
	if flags&flagLatency == 0 {
		return eventtime.FromUnixNanos(0, end)
	}
	return c.timed(end)
}

func (c *cursor) end(flags spanFlag) uint64 {
	if flags&flagEnd == 0 {
		return 0
	}
	end := c.fixed64()
	if end == 0 || end > math.MaxInt64 {
		c.fail(ErrCorrupt, "span end out of range")
		return 0
	}
	return end
}

func (c *cursor) timed(end uint64) eventtime.Interval {
	latency := c.uvarint()
	if end == 0 || latency >= end {
		c.fail(ErrCorrupt, "span latency without a matching end")
		return eventtime.Interval{}
	}
	return eventtime.FromUnixNanos(end-latency, end)
}

func (c *cursor) lookup(table []string) string {
	index := c.uvarint()
	if index > c.referenced || index >= uint64(len(table)) {
		c.fail(ErrCorrupt, "table string used out of first-appearance order")
		return ""
	}
	if index == c.referenced {
		c.referenced++
	}
	return table[index]
}

func (c *cursor) validate(observed *span.Span, flags spanFlag) {
	if c.err != nil {
		return
	}
	if err := checkSpan(observed, flags); err != nil {
		c.err = fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
}

func checkSpan(observed *span.Span, flags spanFlag) error {
	if err := checkIDs(observed, flags); err != nil {
		return err
	}
	if observed.Kind.Normalized() != observed.Kind {
		return errUnknownKind
	}
	source := resource.Resource{
		ServiceName:      observed.Service.Name,
		ServiceNamespace: observed.Service.Namespace,
	}
	if err := source.Validate(); err != nil {
		return err
	}
	return observed.Operation.Validate()
}

func checkIDs(observed *span.Span, flags spanFlag) error {
	switch {
	case observed.TraceID.IsZero() || observed.SpanID.IsZero():
		return errZeroID
	case flags&flagParent != 0 && observed.ParentSpanID.IsZero():
		return errZeroParent
	}
	return nil
}
