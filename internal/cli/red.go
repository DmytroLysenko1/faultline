package cli

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

const (
	liveMarker      = "~"
	rateFormat      = "%s%.1f/s"
	errorFormat     = "err %.1f%%"
	errorBelow      = "err <0.1%"
	errorAbove      = "err >99.9%"
	p95Format       = "p95 %s"
	partSeparator   = " "
	percent         = 100
	smallestShare   = 0.1
	largestShare    = 99.9
	decimalBase     = 10
	threeDigitLimit = 1000
)

func redSummary(final, live *faultlinev1.WindowStats) string {
	pane, marker := newestUsable(final, live)
	if pane == nil {
		return ""
	}
	parts := []string{
		fmt.Sprintf(rateFormat, marker, rate(pane)),
		errorShare(pane),
	}
	if p95, found := measuredP95(pane); found {
		parts = append(parts, fmt.Sprintf(p95Format, rounded(p95)))
	}
	return strings.Join(parts, partSeparator)
}

func newestUsable(final, live *faultlinev1.WindowStats) (*faultlinev1.WindowStats, string) {
	if positive(final.GetObserved()) {
		return final, ""
	}
	if positive(live.GetObserved()) {
		return live, liveMarker
	}
	return nil, ""
}

func positive(duration *durationpb.Duration) bool {
	return duration.CheckValid() == nil && duration.AsDuration() > 0
}

func rate(pane *faultlinev1.WindowStats) float64 {
	return float64(pane.GetRequests()) / pane.GetObserved().AsDuration().Seconds()
}

func errorShare(pane *faultlinev1.WindowStats) string {
	requests := pane.GetRequests()
	failed := min(pane.GetFailedRequests(), requests)
	if failed == 0 {
		return fmt.Sprintf(errorFormat, 0.0)
	}
	share := percent * float64(failed) / float64(requests)
	if share < smallestShare {
		return errorBelow
	}
	if failed < requests && share > largestShare {
		return errorAbove
	}
	return fmt.Sprintf(errorFormat, share)
}

func measuredP95(pane *faultlinev1.WindowStats) (time.Duration, bool) {
	if pane.GetLatencySamples() == 0 || !positive(pane.GetP95()) {
		return 0, false
	}
	return pane.GetP95().AsDuration(), true
}

func rounded(latency time.Duration) time.Duration {
	step := time.Duration(1)
	for latency/step >= threeDigitLimit {
		step *= decimalBase
	}
	return latency.Round(step)
}
