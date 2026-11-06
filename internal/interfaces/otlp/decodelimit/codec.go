package decodelimit

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

var errNoDelegate = errors.New("proto codec is not registered")

const codecName = "proto"

type protoUnmarshaler interface {
	UnmarshalProto(wire []byte) error
}

type Codec struct {
	weigher  *Weigher
	onReject func(err error, weight int64)
}

func NewCodec(weigher *Weigher, onReject func(err error, weight int64)) *Codec {
	return &Codec{
		weigher:  weigher,
		onReject: onReject,
	}
}

func (c *Codec) Name() string {
	return codecName
}

func (c *Codec) Marshal(value any) (mem.BufferSlice, error) {
	delegate, err := protoDelegate()
	if err != nil {
		return nil, err
	}
	return delegate.Marshal(value)
}

func (c *Codec) Unmarshal(data mem.BufferSlice, value any) error {
	target, ok := value.(protoUnmarshaler)
	if !ok {
		return c.delegateUnmarshal(data, value)
	}
	buffer := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buffer.Free()
	return c.decode(buffer.ReadOnlyData(), target)
}

func (c *Codec) decode(wire []byte, target protoUnmarshaler) error {
	weight, err := c.weigher.Weigh(wire)
	if err == nil {
		err = unmarshal(wire, target)
	}
	if err != nil {
		c.onReject(err, weight)
		return fmt.Errorf("decode %d bytes: %w", len(wire), err)
	}
	return nil
}

func (c *Codec) delegateUnmarshal(data mem.BufferSlice, value any) error {
	delegate, err := protoDelegate()
	if err != nil {
		return err
	}
	return delegate.Unmarshal(data, value)
}

func unmarshal(wire []byte, target protoUnmarshaler) error {
	if err := target.UnmarshalProto(wire); err != nil {
		return fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	return nil
}

func protoDelegate() (encoding.CodecV2, error) {
	delegate := encoding.GetCodecV2(codecName)
	if delegate == nil {
		return nil, errNoDelegate
	}
	return delegate, nil
}
