package selfalert

import (
	"slices"
	"time"
)

const (
	ClockSkewRatioHigh      = "windows.clock_skew_ratio_high"
	LateDropRatioHigh       = "windows.late_drop_ratio_high"
	WatermarkLagHigh        = "windows.watermark_lag_high"
	CardinalityLimitReached = "windows.cardinality_limit_reached"
	SeriesLimitReached      = "windows.series_limit_reached"
	SketchBudgetExhausted   = "windows.sketch_budget_exhausted"
	AuthDirectoryStale      = "auth.token_directory_stale"
	AuthSequenceRegressed   = "auth.token_sequence_regressed"
	AuthRevokeBlocked       = "auth.token_revoke_blocked"
	AuthBucketDrift         = "auth.token_bucket_drift"
	AuthEpochChanged        = "auth.token_epoch_changed"
	AuthMassRevoke          = "auth.token_mass_revoke"

	minOffered      = 1_000
	raiseRatio      = 0.01
	clearRatio      = 0.005
	lagRaiseReports = 3
	clearReports    = 2
	countHold       = time.Hour
	oneReport       = 1
	limitReached    = 1
)

type verdict uint8

const (
	verdictSteady verdict = iota
	verdictHigh
	verdictLow
	verdictInsufficient
)

type Reading struct {
	Value     float64
	Threshold float64
	Volume    uint64
}

type reader func(previous, current *Sample, thresholds Thresholds) (Reading, verdict)

type rule struct {
	event      string
	raiseAfter int
	clearAfter int
	holdFor    time.Duration
	escalates  bool
	listOwners bool
	read       reader
}

func rules() []rule {
	return slices.Concat(ratioRules(), stateRules(), authRules())
}

func ratioRules() []rule {
	return []rule{
		{
			event:      ClockSkewRatioHigh,
			raiseAfter: oneReport,
			clearAfter: clearReports,
			read: ratioOf(func(sample *Sample) uint64 {
				return sample.Reassigned
			}),
		},
		{
			event:      LateDropRatioHigh,
			raiseAfter: oneReport,
			clearAfter: clearReports,
			read: ratioOf(func(sample *Sample) uint64 {
				return sample.LateDropped
			}),
		},
	}
}

func stateRules() []rule {
	return []rule{
		{
			event:      WatermarkLagHigh,
			raiseAfter: lagRaiseReports,
			clearAfter: clearReports,
			read:       watermarkLag,
		},
		{
			event:      CardinalityLimitReached,
			raiseAfter: oneReport,
			clearAfter: clearReports,
			escalates:  true,
			listOwners: true,
			read:       ownersOverLimit,
		},
		flagRule(SeriesLimitReached, func(sample *Sample) bool {
			return sample.SeriesLimitReached
		}),
		countRule(SketchBudgetExhausted, clearReports, func(sample *Sample) uint64 {
			return sample.DroppedSamples
		}),
	}
}

func ratioOf(part func(*Sample) uint64) reader {
	return func(previous, current *Sample, _ Thresholds) (Reading, verdict) {
		offered := grown(current.Offered, previous.Offered)
		reading := Reading{
			Threshold: raiseRatio,
			Volume:    offered,
		}
		if offered == 0 {
			return reading, verdictInsufficient
		}
		reading.Value = float64(grown(part(current), part(previous))) / float64(offered)
		if offered < minOffered {
			return reading, verdictInsufficient
		}
		return reading, ratioVerdict(reading.Value)
	}
}

func ratioVerdict(ratio float64) verdict {
	switch {
	case ratio > raiseRatio:
		return verdictHigh
	case ratio < clearRatio:
		return verdictLow
	}
	return verdictSteady
}

func watermarkLag(_, current *Sample, thresholds Thresholds) (Reading, verdict) {
	reading := Reading{
		Threshold: thresholds.WatermarkLag.Seconds(),
	}
	if !current.WatermarkKnown {
		return reading, verdictLow
	}
	lag := current.Now.Sub(current.Watermark)
	reading.Value = lag.Seconds()
	return reading, highWhen(lag > thresholds.WatermarkLag)
}

func ownersOverLimit(_, current *Sample, _ Thresholds) (Reading, verdict) {
	return Reading{
		Value: float64(current.OwnersOverLimit),
	}, highWhen(current.OwnersOverLimit > 0)
}

func flagRule(event string, flag func(*Sample) bool) rule {
	return rule{
		event:      event,
		raiseAfter: oneReport,
		clearAfter: clearReports,
		read:       flagOf(flag),
	}
}

func countRule(event string, clearAfter int, count func(*Sample) uint64) rule {
	return rule{
		event:      event,
		raiseAfter: oneReport,
		clearAfter: clearAfter,
		read:       grownOf(count),
	}
}

func heldCountRule(event string, hold time.Duration, count func(*Sample) uint64) rule {
	held := countRule(event, oneReport, count)
	held.holdFor = hold
	return held
}

func flagOf(flag func(*Sample) bool) reader {
	return func(_, current *Sample, _ Thresholds) (Reading, verdict) {
		if !flag(current) {
			return Reading{}, verdictLow
		}
		return Reading{
			Value: limitReached,
		}, verdictHigh
	}
}

func grownOf(count func(*Sample) uint64) reader {
	return func(previous, current *Sample, _ Thresholds) (Reading, verdict) {
		increase := grown(count(current), count(previous))
		return Reading{
			Value: float64(increase),
		}, highWhen(increase > 0)
	}
}

func highWhen(high bool) verdict {
	if high {
		return verdictHigh
	}
	return verdictLow
}
