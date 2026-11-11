package topologytest

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	limitPairingTTL      = 10 * time.Second
	limitPending         = 100
	limitTombstoneTTL    = time.Minute
	limitTombstones      = 100
	limitNodes           = 100
	limitEdges           = 100
	limitWindowSize      = 10 * time.Second
	limitWatermarkDelay  = 30 * time.Second
	limitAllowedLateness = 30 * time.Second
	limitSlackWindows    = 2
	limitEarlyFirePeriod = time.Second
	limitSeries          = 1_000
	limitOperationsEach  = 50
	limitSketchEntries   = 1 << 16
)

func Limits() topology.Limits {
	return topology.Limits{
		PairingTTL:    limitPairingTTL,
		MaxPending:    limitPending,
		TombstoneTTL:  limitTombstoneTTL,
		MaxTombstones: limitTombstones,
		MaxNodes:      limitNodes,
		MaxEdges:      limitEdges,
		Windows: stream.Config{
			Size:            limitWindowSize,
			Delay:           limitWatermarkDelay,
			AllowedLateness: limitAllowedLateness,
			SlackWindows:    limitSlackWindows,
		},
		EarlyFirePeriod:         limitEarlyFirePeriod,
		MaxSeries:               limitSeries,
		MaxOperationsPerService: limitOperationsEach,
		SketchEntries:           limitSketchEntries,
	}
}
