package topology

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

const (
	permille               = 1000
	pendingImagePermille   = 770
	tombstoneImagePermille = 280
	graphImagePermille     = 220
	trafficImagePermille   = 360
	arenaImagePermille     = 1340
)

func CheckpointSections() []checkpoint.Tag {
	return []checkpoint.Tag{sectionLimits, sectionBuilder, sectionMatcher, sectionGraph, sectionTraffic}
}

func (l *Limits) WorstCheckpointBytes() int64 {
	matcherLimits := l.matcherLimits()
	graphLimits := l.graphLimits()
	trafficLimits := l.trafficLimits()
	tombstones := tombstone.WorstCaseHeapBytes(matcherLimits.MaxTombstones)
	arena := sketch.WorstCaseHeapBytes(trafficLimits.SketchEntries)
	parts := []struct {
		bytes, permille int64
	}{
		{bytes: matcherLimits.WorstCaseHeapBytes() - tombstones, permille: pendingImagePermille},
		{bytes: tombstones, permille: tombstoneImagePermille},
		{bytes: graphLimits.WorstCaseHeapBytes() + graphLimits.ViewHeapBytes(), permille: graphImagePermille},
		{bytes: trafficLimits.WorstCaseHeapBytes() - arena, permille: trafficImagePermille},
		{bytes: arena, permille: arenaImagePermille},
	}
	var worst int64
	for _, part := range parts {
		worst += (part.bytes*part.permille + permille - 1) / permille
	}
	return worst
}
