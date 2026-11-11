package stream_test

import (
	"math"
	"time"
)

// everything is a reach no window starts after, for tests that fire every unfinished window.
func everything() time.Time {
	return time.Unix(0, math.MaxInt64)
}
