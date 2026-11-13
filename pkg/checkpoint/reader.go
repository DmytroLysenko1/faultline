package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

type Reader struct {
	in      io.Reader
	frame   [frameBytes]byte
	chunk   []byte
	payload []byte
	tag     Tag
	ended   bool
	version uint32
	err     error
}

func NewReader(in io.Reader) *Reader {
	r := &Reader{
		in:    in,
		chunk: make([]byte, maxChunkBytes+crcBytes),
		ended: true,
	}
	var header [len(magic) + versionBytes]byte
	r.fill(header[:])
	if r.err == nil && string(header[:len(magic)]) != magic {
		r.Fail("not a checkpoint")
	}
	r.version = binary.LittleEndian.Uint32(header[len(magic):])
	return r
}

func (r *Reader) Version() uint32 {
	return r.version
}

func (r *Reader) Err() error {
	return r.err
}

func (r *Reader) Fail(reason string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrCorrupt, reason)
	}
}

func (r *Reader) Begin(tag Tag) {
	if !r.ended {
		r.Fail("a section begins inside another")
		return
	}
	r.tag, r.ended = tag, false
	r.next()
}

func (r *Reader) End() {
	r.refill()
	if !r.ended || len(r.payload) != 0 {
		r.Fail(fmt.Sprintf("section %d has unread values", r.tag))
	}
	r.ended = true
}

func (r *Reader) Finish() error {
	var probe [1]byte
	read, err := io.ReadFull(r.in, probe[:])
	switch {
	case r.err != nil || read == 0 && errors.Is(err, io.EOF):
	case read != 0:
		r.Fail("trailing bytes after the last section")
	default:
		r.err = fmt.Errorf("read checkpoint: %w", err)
	}
	return r.err
}

func (r *Reader) Uint() uint64 {
	if !r.ready() {
		return 0
	}
	value, read := binary.Uvarint(r.payload)
	if read <= 0 {
		r.Fail("bad varint")
		return 0
	}
	r.payload = r.payload[read:]
	return value
}

func (r *Reader) Int() int64 {
	if !r.ready() {
		return 0
	}
	value, read := binary.Varint(r.payload)
	if read <= 0 {
		r.Fail("bad varint")
		return 0
	}
	r.payload = r.payload[read:]
	return value
}

func (r *Reader) Bool() bool {
	switch value := r.byteValue(); value {
	case trueByte:
		return true
	case falseByte:
		return false
	}
	r.Fail("bad bool")
	return false
}

func (r *Reader) Text(limit int) string {
	length := r.Count(min(limit, MaxStringBytes))
	if r.err != nil || length > len(r.payload) {
		r.Fail("string runs past its chunk")
		return ""
	}
	value := string(r.payload[:length])
	r.payload = r.payload[length:]
	return value
}

func (r *Reader) Fixed(out []byte) {
	if !r.ready() {
		return
	}
	if len(out) > len(r.payload) {
		r.Fail("fixed bytes run past their chunk")
		return
	}
	r.payload = r.payload[copy(out, r.payload):]
}

func (r *Reader) Duration() time.Duration {
	return time.Duration(r.Int())
}

func (r *Reader) Time() time.Time {
	seconds, nanos := r.Int(), r.Int()
	if nanos < 0 || nanos >= nanosPerSecond {
		r.Fail("bad nanoseconds")
		return time.Time{}
	}
	if seconds == zeroTimeUnix && nanos == 0 {
		return time.Time{}
	}
	return time.Unix(seconds, nanos).UTC()
}

func (r *Reader) Count(limit int) int {
	value := r.Uint()
	if limit < 0 || value > uint64(limit) || value > math.MaxInt {
		r.Fail(fmt.Sprintf("count %d is over %d", value, limit))
		return 0
	}
	return int(value)
}

func (r *Reader) Index(length int) int {
	if length <= 0 {
		r.Fail("index into nothing")
		return 0
	}
	return r.Count(length - 1)
}

func (r *Reader) Int32(limit int32) int32 {
	value := r.Count(int(limit))
	if value < 0 || value > math.MaxInt32 {
		return 0
	}
	return int32(value)
}

func (r *Reader) Uint32(limit uint32) uint32 {
	value := r.Count(int(limit))
	if value < 0 || value > math.MaxUint32 {
		return 0
	}
	return uint32(value)
}

func (r *Reader) Uint16(limit uint16) uint16 {
	value := r.Count(int(limit))
	if value < 0 || value > math.MaxUint16 {
		return 0
	}
	return uint16(value)
}

func (r *Reader) Uint8(limit uint8) uint8 {
	value := r.Count(int(limit))
	if value < 0 || value > math.MaxUint8 {
		return 0
	}
	return uint8(value)
}

func (r *Reader) byteValue() byte {
	if !r.ready() {
		return 0
	}
	value := r.payload[0]
	r.payload = r.payload[1:]
	return value
}

func (r *Reader) ready() bool {
	r.refill()
	if r.err == nil && r.ended {
		r.Fail(fmt.Sprintf("value read outside section %d", r.tag))
	}
	return r.err == nil
}

func (r *Reader) refill() {
	if r.err == nil && !r.ended && len(r.payload) == 0 {
		r.next()
	}
}

func (r *Reader) next() {
	r.fill(r.frame[:])
	length := binary.LittleEndian.Uint32(r.frame[tagBytes:])
	switch {
	case r.err != nil:
		return
	case Tag(r.frame[0]) != r.tag:
		r.Fail(fmt.Sprintf("section %d where %d was expected", r.frame[0], r.tag))
		return
	case length > maxChunkBytes:
		r.Fail(fmt.Sprintf("chunk of %d bytes is over %d", length, maxChunkBytes))
		return
	}
	r.load(int(length))
}

func (r *Reader) load(length int) {
	chunk := r.chunk[:length+crcBytes]
	r.fill(chunk)
	payload, sum := chunk[:length], binary.LittleEndian.Uint32(chunk[length:])
	if r.err == nil && checksum(r.frame[:], payload) != sum {
		r.Fail(fmt.Sprintf("section %d checksum mismatch", r.tag))
	}
	r.payload, r.ended = payload, length == 0
}

func (r *Reader) fill(out []byte) {
	if r.err != nil {
		return
	}
	_, err := io.ReadFull(r.in, out)
	switch {
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		r.Fail("truncated")
	case err != nil:
		r.err = fmt.Errorf("read checkpoint: %w", err)
	}
}
