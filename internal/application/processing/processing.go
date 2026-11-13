package processing

import (
	"errors"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

var (
	ErrFeedClosed     = errors.New("span feed closed")
	ErrAlreadyStarted = errors.New("pipeline already started")
)

type Position struct {
	Partition uint16
	Sequence  uint64
}

func (p Position) Next() Position {
	return Position{
		Partition: p.Partition,
		Sequence:  p.Sequence + 1,
	}
}

type Delivery interface {
	Batch() batch.Batch
	Bytes() int64
	Position() Position
	Release()
}
