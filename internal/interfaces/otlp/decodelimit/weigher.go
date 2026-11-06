package decodelimit

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

var (
	ErrTooHeavy  = errors.New("request decodes beyond the memory limit")
	ErrTooDeep   = errors.New("request nests values beyond the depth limit")
	ErrMalformed = errors.New("malformed OTLP request")
)

const (
	smallTextBytes      = 256
	largeTextSlackShift = 2
	textHeaderBytes     = 16
	slotSlackShift      = 2
	slotGrowthFactor    = 2
)

type Limits struct {
	MaxDecodedBytes int64
	MaxNestingDepth int
}

type Weigher struct {
	schema *schema
	limits Limits
}

func NewWeigher(limits Limits) *Weigher {
	return &Weigher{
		schema: newSchema(),
		limits: limits,
	}
}

func (w *Weigher) Weigh(wire []byte) (int64, error) {
	state := &walk{
		Weigher: w,
	}
	err := state.message(wire, requestMessageKind)
	return state.total, err
}

type walk struct {
	*Weigher
	total int64
	depth int
}

func (w *walk) message(wire []byte, of messageKind) error {
	if err := w.charge(w.schema[of].own); err != nil {
		return err
	}
	for len(wire) > 0 {
		consumed, err := w.field(wire, w.schema[of])
		if err != nil {
			return err
		}
		wire = wire[consumed:]
	}
	return nil
}

func (w *walk) field(wire []byte, parent message) (int, error) {
	number, wireType, tagBytes := protowire.ConsumeTag(wire)
	if tagBytes < 0 {
		return 0, ErrMalformed
	}
	spec, known := parent.lookup(number)
	if !known || wireType != protowire.BytesType {
		valueBytes, err := w.skip(number, wireType, wire[tagBytes:], spec)
		return tagBytes + valueBytes, err
	}
	value, valueBytes := protowire.ConsumeBytes(wire[tagBytes:])
	if valueBytes < 0 {
		return 0, ErrMalformed
	}
	return tagBytes + valueBytes, w.lengthDelimited(spec, value)
}

func (w *walk) skip(
	number protowire.Number,
	wireType protowire.Type,
	wire []byte,
	spec field,
) (int, error) {
	valueBytes := protowire.ConsumeFieldValue(number, wireType, wire)
	if valueBytes < 0 {
		return 0, ErrMalformed
	}
	return valueBytes, w.charge(spec.wrapper)
}

func (w *walk) lengthDelimited(spec field, value []byte) error {
	if err := w.charge(spec.wrapper + slotCost(spec.slot)); err != nil {
		return err
	}
	switch spec.role {
	case nested:
		return w.nested(value, spec.child)
	case text:
		return w.charge(textCost(len(value)))
	case scalar:
		return nil
	}
	return nil
}

func (w *walk) nested(wire []byte, of messageKind) error {
	if of != anyValueMessageKind {
		return w.message(wire, of)
	}
	w.depth++
	defer func() {
		w.depth--
	}()
	if w.depth > w.limits.MaxNestingDepth {
		return fmt.Errorf("%w: depth %d > %d", ErrTooDeep, w.depth, w.limits.MaxNestingDepth)
	}
	return w.message(wire, of)
}

func (w *walk) charge(bytes int64) error {
	w.total += bytes
	if w.total > w.limits.MaxDecodedBytes {
		return fmt.Errorf("%w: weight over %d bytes", ErrTooHeavy, w.limits.MaxDecodedBytes)
	}
	return nil
}

func slotCost(slot int64) int64 {
	return slot*slotGrowthFactor + slot>>slotSlackShift
}

func textCost(length int) int64 {
	size := int64(length)
	if length > smallTextBytes {
		return size + size>>largeTextSlackShift + textHeaderBytes
	}
	return size + textHeaderBytes
}
