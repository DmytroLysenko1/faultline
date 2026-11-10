package cardinality

import (
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

func (r *Registry) Lookup(owner handle.Node, op operation.Operation) (ID, bool) {
	id, known := r.index[key{
		owner:     owner,
		operation: op,
	}]
	return id, known
}

func (r *Registry) Acquire(owner handle.Node, op operation.Operation) (ID, bool) {
	wanted := key{
		owner:     owner,
		operation: op,
	}
	if id, known := r.index[wanted]; known {
		r.entries[id].references++
		return id, true
	}
	if !r.owners.admit(owner) {
		return 0, false
	}
	return r.add(wanted), true
}

func (r *Registry) Release(owner handle.Node, id ID) {
	released := r.live(id)
	if released.key.owner != owner {
		panic(fmt.Sprintf("cardinality: owner %d releases operation %d of owner %d", owner, id, released.key.owner))
	}
	released.references--
	if released.references == 0 {
		r.forget(id)
	}
}

func (r *Registry) Operation(id ID) operation.Operation {
	return r.live(id).key.operation
}

func (r *Registry) live(id ID) *entry {
	if id < 0 || int(id) >= len(r.entries) || r.entries[id].references == 0 {
		panic(fmt.Sprintf("cardinality: operation %d is not live", id))
	}
	return &r.entries[id]
}

func (r *Registry) add(wanted key) ID {
	id := r.reserve()
	r.entries[id] = entry{
		key:        wanted,
		references: 1,
	}
	r.index[wanted] = id
	return id
}

func (r *Registry) reserve() ID {
	if last := len(r.free) - 1; last >= 0 {
		id := r.free[last]
		r.free = r.free[:last]
		return id
	}
	id := r.next
	r.next++
	r.entries = append(r.entries, entry{})
	return id
}

func (r *Registry) forget(id ID) {
	forgotten := r.entries[id].key
	delete(r.index, forgotten)
	r.entries[id] = entry{}
	r.free = append(r.free, id)
	r.owners.release(forgotten.owner)
}
