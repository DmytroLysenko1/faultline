package traffic

import (
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const MaxOwnersOverLimitListed = 64

type counters struct {
	Untimed        uint64
	LatencyUnknown uint64
	Heartbeats     uint64
	Finals         uint64
}

type Stats struct {
	Subjects           stream.Stats
	Operations         stream.Stats
	Overflow           stream.Stats
	Sketch             sketch.ArenaStats
	Cardinality        cardinality.Stats
	OwnersOverLimit    []resource.ServiceRef
	SeriesLimitReached bool
	Untimed            uint64
	LatencyUnknown     uint64
	Heartbeats         uint64
	Finals             uint64
}

func (t *Traffic) Stats() Stats {
	return Stats{
		Subjects:           t.subjects.Stats(),
		Operations:         t.operations.Stats(),
		Overflow:           t.overflow.Stats(),
		Sketch:             t.arena.Stats,
		Cardinality:        t.registry.Stats(),
		OwnersOverLimit:    t.ownersOverLimit(),
		SeriesLimitReached: t.operations.Len() >= t.operations.Capacity(),
		Untimed:            t.counters.Untimed,
		LatencyUnknown:     t.counters.LatencyUnknown,
		Heartbeats:         t.counters.Heartbeats,
		Finals:             t.counters.Finals,
	}
}

func (t *Traffic) ownersOverLimit() []resource.ServiceRef {
	owners := t.registry.OwnersOverLimit()
	listed := make([]resource.ServiceRef, 0, min(len(owners), MaxOwnersOverLimitListed))
	for _, owner := range owners[:cap(listed)] {
		listed = append(listed, t.book.nodes[owner])
	}
	return listed
}
