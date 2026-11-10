package processing

import (
	"errors"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

var (
	ErrFeedClosed     = errors.New("span feed closed")
	ErrAlreadyStarted = errors.New("pipeline already started")
)

type Delivery interface {
	Batch() batch.Batch
	Bytes() int64
	Release()
}
