package processing

import (
	"errors"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

var (
	ErrFeedClosed     = errors.New("span feed closed")
	ErrAlreadyStarted = errors.New("pipeline already started")
	ErrEmptyReceive   = errors.New("span feed returned no deliveries")
)

type Delivery interface {
	Batch() batch.Batch
	Release()
}
