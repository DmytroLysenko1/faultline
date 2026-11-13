package operation

import (
	"math"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func (o Operation) WriteCheckpoint(w *checkpoint.Writer) {
	w.Uint(uint64(o.Protocol))
	w.Text(o.Name)
}

func ReadOperation(r *checkpoint.Reader) Operation {
	read := Operation{
		Protocol: Protocol(r.Uint8(math.MaxUint8)),
		Name:     r.Text(MaxNameBytes),
	}
	if err := read.Validate(); err != nil && r.Err() == nil {
		r.Fail(err.Error())
	}
	return read
}
