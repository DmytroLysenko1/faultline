package selfalert

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

type Sample struct {
	Now                time.Time
	Watermark          time.Time
	WatermarkKnown     bool
	Offered            uint64
	Reassigned         uint64
	LateDropped        uint64
	OwnersOverLimit    int
	Owners             []resource.ServiceRef
	SeriesLimitReached bool
	DroppedSamples     uint64
}

type Thresholds struct {
	WatermarkLag time.Duration
}

func grown(current, previous uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}
