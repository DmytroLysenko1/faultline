package cardinality

import (
	"fmt"
	"maps"
	"slices"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

type owners struct {
	limit      int
	operations []int
	overLimit  map[handle.Node]struct{}
}

func newOwners(maxOwners, limit int) owners {
	return owners{
		limit:      limit,
		operations: make([]int, maxOwners),
		overLimit:  make(map[handle.Node]struct{}),
	}
}

func (o *owners) admit(owner handle.Node) bool {
	if owner < 0 || int(owner) >= len(o.operations) {
		panic(fmt.Sprintf("cardinality: owner %d is outside the registry's %d owners", owner, len(o.operations)))
	}
	if o.operations[owner] < o.limit {
		o.operations[owner]++
		return true
	}
	o.overLimit[owner] = struct{}{}
	return false
}

func (o *owners) release(owner handle.Node) {
	o.operations[owner]--
	delete(o.overLimit, owner)
}

func (o *owners) overLimitCount() int {
	return len(o.overLimit)
}

func (o *owners) overLimitList() []handle.Node {
	return slices.Sorted(maps.Keys(o.overLimit))
}
