package topology

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
)

type Snapshot struct {
	Revision     uint64
	GeneratedAt  time.Time
	Nodes        []graph.Node
	Edges        []graph.Edge
	PairingStats matcher.Stats
	GraphStats   graph.Stats
}
