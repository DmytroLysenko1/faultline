package topology

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

const (
	permille               = 1000
	pendingImagePermille   = 784
	tombstoneImagePermille = 292
	graphImagePermille     = 220
	trafficImagePermille   = 360
	arenaImagePermille     = 1340
	matcherFixedBytes      = 64
)

func CheckpointSections() []checkpoint.Tag {
	return []checkpoint.Tag{sectionLimits, sectionBuilder, sectionMatcher, sectionGraph, sectionTraffic}
}

func (l *Limits) WorstCheckpointBytes() int64 {
	var worst int64
	for _, bytes := range l.worstSectionBytes() {
		worst += bytes
	}
	return worst
}

func (l *Limits) worstSectionBytes() map[checkpoint.Tag]int64 {
	matcherLimits := l.matcherLimits()
	graphLimits := l.graphLimits()
	trafficLimits := l.trafficLimits()
	tombstones := tombstone.WorstCaseHeapBytes(matcherLimits.MaxTombstones)
	arena := sketch.WorstCaseHeapBytes(trafficLimits.SketchEntries)
	return map[checkpoint.Tag]int64{
		sectionMatcher: matcherFixedBytes +
			imageShare(matcherLimits.WorstCaseHeapBytes()-tombstones, pendingImagePermille) +
			imageShare(tombstones, tombstoneImagePermille),
		sectionGraph: imageShare(graphLimits.WorstCaseHeapBytes()+graphLimits.ViewHeapBytes(), graphImagePermille),
		sectionTraffic: imageShare(trafficLimits.WorstCaseHeapBytes()-arena, trafficImagePermille) +
			imageShare(arena, arenaImagePermille),
	}
}

func imageShare(heapBytes, partPermille int64) int64 {
	return (heapBytes*partPermille + permille - 1) / permille
}
