package stream

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var ErrInvalidConfig = errors.New("invalid window config")

const maxSlots = 256

type Window struct {
	ID    int64
	Start time.Time
	End   time.Time
}

type Config struct {
	Size            time.Duration
	Delay           time.Duration
	AllowedLateness time.Duration
	SlackWindows    int
}

func (c Config) Validate() error {
	if err := c.validateDurations(); err != nil {
		return err
	}
	return c.validateHorizon()
}

func (c Config) validateDurations() error {
	switch {
	case c.Size <= 0:
		return fmt.Errorf("%w: window size %s must be positive", ErrInvalidConfig, c.Size)
	case c.Delay < 0 || c.AllowedLateness < 0 || c.Delay > math.MaxInt64-c.AllowedLateness:
		return fmt.Errorf("%w: delay %s and allowed lateness %s must not be negative or overflow", ErrInvalidConfig,
			c.Delay, c.AllowedLateness)
	}
	return nil
}

func (c Config) validateHorizon() error {
	switch {
	case c.SlackWindows < 1 || c.SlackWindows > maxSlots:
		return fmt.Errorf("%w: slack %d windows must be in [1, %d]", ErrInvalidConfig, c.SlackWindows, maxSlots)
	case c.Delay/c.Size+c.AllowedLateness/c.Size >= maxSlots || c.Slots() > maxSlots:
		return fmt.Errorf("%w: delay %s and lateness %s need more than %d live windows of %s per series",
			ErrInvalidConfig, c.Delay, c.AllowedLateness, maxSlots, c.Size)
	}
	return nil
}

func (c Config) Slots() int {
	return windowsIn(c.Delay+c.AllowedLateness, c.Size) + 1 + c.SlackWindows
}

func (c Config) lateWindows() int {
	return windowsIn(c.AllowedLateness, c.Size)
}

func (c Config) WindowOf(t time.Time) Window {
	return c.window(c.windowID(t))
}

func (c Config) windowID(t time.Time) int64 {
	return floorDiv(t.UnixNano(), int64(c.Size))
}

func (c Config) window(id int64) Window {
	seconds, nanos := int64(c.Size/time.Second), int64(c.Size%time.Second)
	start := time.Unix(id*seconds, id*nanos)
	return Window{
		ID:    id,
		Start: start,
		End:   start.Add(c.Size),
	}
}

func windowsIn(span, size time.Duration) int {
	whole := int(span / size)
	if span%size != 0 {
		whole++
	}
	return whole
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor != 0 && (value < 0) != (divisor < 0) {
		quotient--
	}
	return quotient
}

func floorMod(value, divisor int64) int64 {
	return value - floorDiv(value, divisor)*divisor
}
