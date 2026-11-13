package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	errNotInSection  = errors.New("checkpoint writer: value outside a section")
	errValueTooLong  = errors.New("checkpoint writer: value too long")
	errNegativeCount = errors.New("checkpoint writer: negative count")
)

type Writer struct {
	out   io.Writer
	chunk []byte
	tag   Tag
	open  bool
	err   error
}

func NewWriter(out io.Writer, version uint32) *Writer {
	w := &Writer{
		out:   out,
		chunk: make([]byte, frameBytes, frameBytes+maxChunkBytes+crcBytes),
	}
	header := binary.LittleEndian.AppendUint32([]byte(magic), version)
	w.write(header)
	return w
}

func (w *Writer) Err() error {
	return w.err
}

func (w *Writer) Begin(tag Tag) {
	if w.open {
		w.fail(errNotInSection)
		return
	}
	w.tag, w.open = tag, true
}

func (w *Writer) End() {
	if !w.open {
		w.fail(errNotInSection)
	}
	w.flush()
	w.emit()
	w.open = false
}

func (w *Writer) Uint(value uint64) {
	if w.ready() {
		w.chunk = binary.AppendUvarint(w.chunk, value)
	}
}

func (w *Writer) Int(value int64) {
	if w.ready() {
		w.chunk = binary.AppendVarint(w.chunk, value)
	}
}

func (w *Writer) Count(value int) {
	if value < 0 {
		w.fail(fmt.Errorf("%w: %d", errNegativeCount, value))
		return
	}
	w.Uint(uint64(value))
}

func (w *Writer) Bool(value bool) {
	if !w.ready() {
		return
	}
	if value {
		w.chunk = append(w.chunk, trueByte)
		return
	}
	w.chunk = append(w.chunk, falseByte)
}

func (w *Writer) Text(value string) {
	if w.fits(len(value)) && w.ready() {
		w.chunk = binary.AppendUvarint(w.chunk, uint64(len(value)))
		w.chunk = append(w.chunk, value...)
	}
}

func (w *Writer) Fixed(value []byte) {
	if w.fits(len(value)) && w.ready() {
		w.chunk = append(w.chunk, value...)
	}
}

func (w *Writer) Duration(value time.Duration) {
	w.Int(int64(value))
}

func (w *Writer) Time(value time.Time) {
	w.Int(value.Unix())
	w.Int(int64(value.Nanosecond()))
}

func (w *Writer) fits(length int) bool {
	if length > MaxStringBytes {
		w.fail(fmt.Errorf("%w: %d bytes is over %d", errValueTooLong, length, MaxStringBytes))
		return false
	}
	return true
}

func (w *Writer) ready() bool {
	if w.err == nil && !w.open {
		w.fail(errNotInSection)
	}
	if w.payloadBytes() >= flushAtBytes {
		w.flush()
	}
	return w.err == nil
}

func (w *Writer) payloadBytes() int {
	return len(w.chunk) - frameBytes
}

func (w *Writer) flush() {
	if w.payloadBytes() > 0 {
		w.emit()
	}
}

func (w *Writer) emit() {
	if w.err != nil {
		return
	}
	w.chunk[0] = byte(w.tag)
	binary.LittleEndian.PutUint32(w.chunk[tagBytes:frameBytes], chunkLength(w.payloadBytes()))
	sum := checksum(w.chunk[:frameBytes], w.chunk[frameBytes:])
	w.write(binary.LittleEndian.AppendUint32(w.chunk, sum))
	w.chunk = w.chunk[:frameBytes]
}

func (w *Writer) write(data []byte) {
	if w.err != nil {
		return
	}
	if _, err := w.out.Write(data); err != nil {
		w.fail(fmt.Errorf("write checkpoint: %w", err))
	}
}

func (w *Writer) fail(err error) {
	if w.err == nil {
		w.err = err
	}
}

func chunkLength(length int) uint32 {
	if length < 0 || length > maxChunkBytes {
		return 0
	}
	return uint32(length)
}
