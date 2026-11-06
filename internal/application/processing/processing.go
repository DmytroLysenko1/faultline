package processing

import (
	"errors"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

var ErrFeedClosed = errors.New("span feed closed")

type Delivery interface {
	Batch() batch.Batch
	Release()
}
