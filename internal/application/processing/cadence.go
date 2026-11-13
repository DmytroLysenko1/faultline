package processing

import (
	"errors"
	"fmt"
	"time"
)

var ErrInvalidCadence = errors.New("invalid pipeline cadence")

type clock interface {
	Now() time.Time
}

type Cadence struct {
	FirePeriod         time.Duration
	MaxHeldBytes       int64
	CheckpointInterval time.Duration
}

func (c Cadence) Validate() error {
	if c.FirePeriod <= 0 || c.MaxHeldBytes <= 0 {
		return fmt.Errorf("%w: fire period %s and max held bytes %d must be positive", ErrInvalidCadence,
			c.FirePeriod, c.MaxHeldBytes)
	}
	if c.CheckpointInterval < 0 {
		return fmt.Errorf("%w: checkpoint interval %s is negative", ErrInvalidCadence, c.CheckpointInterval)
	}
	return nil
}
