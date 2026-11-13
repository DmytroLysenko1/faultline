package traffic

import (
	"math"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
)

type subjectKeys struct {
	book *subjectBook
}

func (subjectKeys) WriteKey(w *checkpoint.Writer, key subjectKey) {
	w.Uint(uint64(key.Kind))
	w.Count(int(key.Subject))
}

func (k subjectKeys) ReadKey(r *checkpoint.Reader) subjectKey {
	kind := SubjectKind(r.Uint8(uint8(SubjectEdge)))
	known := len(k.book.edges)
	if kind == SubjectNode {
		known = len(k.book.nodes)
	}
	key := subjectKey{
		Subject: r.Int32(subjectLimit(known)),
		Kind:    kind,
	}
	if kind != SubjectNode && kind != SubjectEdge {
		r.Fail("unknown subject kind")
	}
	return key
}

func subjectLimit(known int) int32 {
	return int32(min(max(known-1, -1), math.MaxInt32))
}

type subjectCells struct {
	subjectKeys
	arena   *sketch.Arena
	restore *sketch.ArenaRestore
}

func (c subjectCells) WriteCell(w *checkpoint.Writer, cell *red.Cell) {
	cell.WriteCheckpoint(w, c.arena)
}

func (c subjectCells) ReadCell(r *checkpoint.Reader, cell *red.Cell) {
	cell.ReadCheckpoint(r, c.restore)
}

type subjectCounts struct {
	subjectKeys
}

func (subjectCounts) WriteCell(w *checkpoint.Writer, counts *red.Counts) {
	counts.WriteCheckpoint(w)
}

func (subjectCounts) ReadCell(r *checkpoint.Reader, counts *red.Counts) {
	counts.ReadCheckpoint(r)
}

type operationCounts struct {
	subjectCounts
	references map[cardinality.ID]int
	owners     map[cardinality.ID]handle.Node
}

func newOperationCounts(book *subjectBook) *operationCounts {
	return &operationCounts{
		subjectCounts: subjectCounts{
			subjectKeys{
				book: book,
			},
		},
		references: make(map[cardinality.ID]int),
		owners:     make(map[cardinality.ID]handle.Node),
	}
}

func (c *operationCounts) WriteKey(w *checkpoint.Writer, key operationKey) {
	c.subjectKeys.WriteKey(w, key.subject())
	w.Count(int(key.Operation))
}

func (c *operationCounts) ReadKey(r *checkpoint.Reader) operationKey {
	key := c.subjectKeys.ReadKey(r).withOperation(cardinality.ID(r.Int32(math.MaxInt32)))
	if r.Err() != nil {
		return key
	}
	owner := c.book.owner(key)
	if previous, seen := c.owners[key.Operation]; seen && previous != owner {
		r.Fail("one operation id belongs to two owners")
	}
	c.owners[key.Operation] = owner
	c.references[key.Operation]++
	return key
}

func (c *operationCounts) check(r *checkpoint.Reader, registry *cardinality.Registry) {
	if r.Err() != nil {
		return
	}
	if len(c.references) != registry.Stats().Operations {
		r.Fail("open operation series and live operations differ")
		return
	}
	for id, references := range c.references {
		if !registry.Holds(id, c.owners[id], references) {
			r.Fail("an operation's owner or references differ from its open series")
			return
		}
	}
}
