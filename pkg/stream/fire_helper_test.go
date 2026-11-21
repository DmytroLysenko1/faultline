package stream_test

import (
	"math"
	"time"
)

func everything() time.Time {
	return time.Unix(0, math.MaxInt64)
}
