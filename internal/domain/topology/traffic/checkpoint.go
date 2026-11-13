package traffic

import (
	"fmt"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

func (t *Traffic) WriteCheckpoint(w *checkpoint.Writer) {
	for _, counter := range t.counters.all() {
		w.Uint(*counter)
	}
	w.Time(t.untimedAt)
	t.book.write(w)
	t.views.write(w)
	t.watermark.WriteCheckpoint(w)
	t.arena.WriteCheckpoint(w)
	t.registry.WriteCheckpoint(w)
	t.subjects.WriteCheckpoint(w, subjectCells{
		arena: t.arena,
	})
	t.operations.WriteCheckpoint(w, &operationCounts{})
	t.overflow.WriteCheckpoint(w, subjectCounts{})
}

type Subjects struct {
	Nodes []resource.ServiceRef
	Edges []EdgeSubject
}

type EdgeSubject struct {
	Caller handle.Node
	Callee handle.Node
}

func Restore(limits *Limits, sink FinalSink, known *Subjects, r *checkpoint.Reader) (*Traffic, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	restored := &Traffic{
		limits: *limits,
		sink:   sink,
		book:   newSubjectBook(limits.MaxNodes, limits.MaxEdges),
		views:  newViews(limits.MaxNodes, limits.MaxEdges),
	}
	if sink == nil {
		restored.sink = discard{}
	}
	if err := restored.restore(r, known); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	return restored, nil
}

func (t *Traffic) restore(r *checkpoint.Reader, known *Subjects) error {
	for _, counter := range t.counters.all() {
		*counter = r.Uint()
	}
	t.untimedAt = r.Time()
	t.book.read(r)
	t.views.read(r)
	t.book.check(r, known)
	t.views.check(r, known)
	arena, err := t.restoreState(r)
	if err != nil {
		return err
	}
	operations, err := t.restoreEngines(r, arena)
	if err != nil {
		return err
	}
	arena.Finish(r)
	operations.check(r, t.registry)
	t.checkOwnersOverLimit(r)
	return nil
}

func (t *Traffic) restoreState(r *checkpoint.Reader) (*sketch.ArenaRestore, error) {
	var err error
	if t.watermark, err = stream.RestoreWatermark(t.limits.Windows, r); err != nil {
		return nil, fmt.Errorf("watermark: %w", err)
	}
	arena, err := sketch.RestoreArena(t.limits.SketchEntries, r)
	if err != nil {
		return nil, fmt.Errorf("sketch arena: %w", err)
	}
	t.arena = arena.Arena
	perService := cardinality.Limits{
		MaxPerService: t.limits.MaxPerService,
	}
	if t.registry, err = cardinality.RestoreRegistry(perService, t.limits.MaxNodes, t.limits.MaxSeries, r); err != nil {
		return nil, fmt.Errorf("operation registry: %w", err)
	}
	return arena, nil
}

func (t *Traffic) restoreEngines(r *checkpoint.Reader, arena *sketch.ArenaRestore) (*operationCounts, error) {
	var err error
	cfg, subjects, keys := t.limits.Windows, t.limits.subjects(), subjectKeys{
		book: &t.book,
	}
	cells := subjectCells{
		subjectKeys: keys,
		restore:     arena,
	}
	if t.subjects, err = stream.Restore(cfg, stream.Options{
		Capacity: subjects,
		Early:    true,
	}, subjectSink{t}, r, cells); err != nil {
		return nil, fmt.Errorf("subject windows: %w", err)
	}
	operations := newOperationCounts(&t.book)
	if t.operations, err = stream.Restore(cfg, stream.Options{
		Capacity: t.limits.MaxSeries,
	}, operationSink{t}, r, operations); err != nil {
		return nil, fmt.Errorf("operation windows: %w", err)
	}
	if t.overflow, err = stream.Restore(cfg, stream.Options{
		Capacity: subjects,
	}, overflowSink{t}, r, subjectCounts{keys}); err != nil {
		return nil, fmt.Errorf("overflow windows: %w", err)
	}
	return operations, nil
}

func (t *Traffic) checkOwnersOverLimit(r *checkpoint.Reader) {
	for _, owner := range t.registry.OwnersOverLimit() {
		if int(owner) >= len(t.book.nodes) {
			r.Fail("an owner over its limit has no service")
			return
		}
	}
}

func (c *counters) all() []*uint64 {
	return []*uint64{
		&c.Untimed,
		&c.LatencyUnknown,
		&c.Heartbeats,
		&c.Finals,
	}
}

func (b *subjectBook) write(w *checkpoint.Writer) {
	w.Count(len(b.nodes))
	for _, service := range b.nodes {
		writeService(w, service)
	}
	w.Count(len(b.edges))
	for _, ends := range b.edges {
		writeService(w, ends.Caller)
		writeService(w, ends.Callee)
		w.Count(int(ends.CalleeNode))
	}
}

func (b *subjectBook) read(r *checkpoint.Reader) {
	b.nodes = b.nodes[:r.Count(cap(b.nodes))]
	for i := range b.nodes {
		b.nodes[i] = readService(r)
	}
	b.edges = b.edges[:r.Count(cap(b.edges))]
	for i := range b.edges {
		b.edges[i] = edgeEnds{
			Caller:     readService(r),
			Callee:     readService(r),
			CalleeNode: handle.Node(r.Int32(subjectLimit(len(b.nodes)))),
		}
	}
}

func (v *views) check(r *checkpoint.Reader, known *Subjects) {
	if len(v.nodes) > len(known.Nodes) || len(v.edges) > len(known.Edges) {
		r.Fail("views cover subjects the graph does not have")
	}
}

func (b *subjectBook) check(r *checkpoint.Reader, known *Subjects) {
	if len(b.nodes) > len(known.Nodes) || len(b.edges) > len(known.Edges) {
		r.Fail("the subject book names handles the graph does not have")
		return
	}
	if !b.nodesAgree(known) || !b.edgesAgree(known) {
		r.Fail("the subject book names a node or an edge other than the graph's")
	}
}

func (b *subjectBook) nodesAgree(known *Subjects) bool {
	for node, service := range b.nodes {
		if service != (resource.ServiceRef{}) && service != known.Nodes[node] {
			return false
		}
	}
	return true
}

func (b *subjectBook) edgesAgree(known *Subjects) bool {
	for edge := range b.edges {
		if !b.edgeAgrees(edge, known) {
			return false
		}
	}
	return true
}

func (b *subjectBook) edgeAgrees(edge int, known *Subjects) bool {
	ends, want := b.edges[edge], known.Edges[edge]
	return ends == edgeEnds{} ||
		ends.CalleeNode == want.Callee &&
			ends.Caller == known.Nodes[want.Caller] &&
			ends.Callee == known.Nodes[want.Callee]
}

func writeService(w *checkpoint.Writer, service resource.ServiceRef) {
	w.Bool(service != resource.ServiceRef{})
	if service != (resource.ServiceRef{}) {
		service.WriteCheckpoint(w)
	}
}

func readService(r *checkpoint.Reader) resource.ServiceRef {
	if !r.Bool() {
		return resource.ServiceRef{}
	}
	return resource.ReadServiceRef(r)
}

func (v *views) write(w *checkpoint.Writer) {
	for _, subjects := range [][]SubjectRED{v.nodes, v.edges} {
		w.Count(len(subjects))
		for i := range subjects {
			subjects[i].Live.WriteCheckpoint(w)
			subjects[i].Final.WriteCheckpoint(w)
		}
	}
	w.Bool(v.changed)
}

func (v *views) read(r *checkpoint.Reader) {
	for _, subjects := range []*[]SubjectRED{&v.nodes, &v.edges} {
		*subjects = (*subjects)[:r.Count(cap(*subjects))]
		for i := range *subjects {
			(*subjects)[i] = SubjectRED{
				Live:  red.ReadWindowStats(r),
				Final: red.ReadWindowStats(r),
			}
		}
	}
	v.changed = r.Bool()
}
