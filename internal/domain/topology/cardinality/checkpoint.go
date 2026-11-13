package cardinality

import (
	"math"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func (r *Registry) WriteCheckpoint(w *checkpoint.Writer) {
	w.Count(len(r.entries))
	w.Count(len(r.index))
	for id := range r.entries {
		if live := &r.entries[id]; live.references > 0 {
			w.Count(id)
			w.Count(int(live.key.owner))
			live.key.operation.WriteCheckpoint(w)
			w.Count(live.references)
		}
	}
	w.Count(len(r.free))
	for _, id := range r.free {
		w.Count(int(id))
	}
	owners := r.owners.overLimitList()
	w.Count(len(owners))
	for _, owner := range owners {
		w.Count(int(owner))
	}
}

func RestoreRegistry(limits Limits, maxOwners, maxOperations int, r *checkpoint.Reader) (*Registry, error) {
	restored, err := NewRegistry(limits, maxOwners)
	if err != nil {
		return nil, err
	}
	restored.entries = make([]entry, r.Count(maxOperations))
	restored.next = idOf(len(restored.entries))
	restored.readEntries(r)
	restored.readFree(r)
	restored.readOverLimit(r)
	return restored, nil
}

func (r *Registry) readEntries(in *checkpoint.Reader) {
	live, previous := in.Count(len(r.entries)), -1
	for range live {
		id := in.Index(len(r.entries))
		if id <= previous {
			in.Fail("operations are repeated or out of order")
			return
		}
		r.readEntry(in, id)
		previous = id
	}
}

func (r *Registry) readEntry(in *checkpoint.Reader, id int) {
	read := key{
		owner:     r.owners.readOwner(in),
		operation: operation.ReadOperation(in),
	}
	references := in.Count(math.MaxInt32)
	if _, repeated := r.index[read]; repeated || references == 0 || !r.owners.admit(read.owner) {
		in.Fail("an operation is repeated, unreferenced or over its owner's limit")
		return
	}
	r.entries[id] = entry{
		key:        read,
		references: references,
	}
	r.index[read] = idOf(id)
}

func (r *Registry) readFree(in *checkpoint.Reader) {
	count := in.Count(len(r.entries))
	if count != len(r.entries)-len(r.index) {
		in.Fail("free operation ids do not complete the live ones")
		return
	}
	r.free = make([]ID, 0, count)
	freed := make([]bool, len(r.entries))
	for range count {
		r.readFreeID(in, freed)
	}
}

func (r *Registry) readFreeID(in *checkpoint.Reader, freed []bool) {
	id := in.Index(len(r.entries))
	if in.Err() != nil || freed[id] || r.entries[id].references != 0 {
		in.Fail("a free operation id is live or repeated")
		return
	}
	freed[id] = true
	r.free = append(r.free, idOf(id))
}

func (r *Registry) readOverLimit(in *checkpoint.Reader) {
	count := in.Count(len(r.owners.operations))
	for range count {
		owner := r.owners.readOwner(in)
		if _, repeated := r.owners.overLimit[owner]; repeated {
			in.Fail("an owner over its limit is repeated")
			return
		}
		r.owners.overLimit[owner] = struct{}{}
	}
}

func (r *Registry) Holds(id ID, owner handle.Node, references int) bool {
	return id >= 0 && int(id) < len(r.entries) &&
		r.entries[id].references == references &&
		r.entries[id].key.owner == owner
}

func (o *owners) readOwner(in *checkpoint.Reader) handle.Node {
	return handle.Node(idOf(in.Index(len(o.operations))))
}

func idOf(index int) ID {
	if index < 0 || index > math.MaxInt32 {
		return 0
	}
	return ID(index)
}
