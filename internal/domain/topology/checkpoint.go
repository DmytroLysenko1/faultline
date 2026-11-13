package topology

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const CheckpointVersion uint32 = 1

var (
	ErrCheckpointIncompatible = errors.New("checkpoint is incompatible with the current limits")
	ErrCheckpointCorrupt      = checkpoint.ErrCorrupt
)

const (
	sectionLimits checkpoint.Tag = iota + 1
	sectionBuilder
	sectionMatcher
	sectionGraph
	sectionTraffic

	maxLimit = 1 << 30
)

func (b *Builder) WriteCheckpoint(out io.Writer) error {
	w := checkpoint.NewWriter(out, CheckpointVersion)
	section(w, sectionLimits, b.limits.write)
	section(w, sectionBuilder, b.published.write)
	section(w, sectionMatcher, b.Matcher.WriteCheckpoint)
	section(w, sectionGraph, b.Graph.WriteCheckpoint)
	section(w, sectionTraffic, b.Traffic.WriteCheckpoint)
	return w.Err()
}

func section(w *checkpoint.Writer, tag checkpoint.Tag, write func(*checkpoint.Writer)) {
	w.Begin(tag)
	write(w)
	w.End()
}

func RestoreBuilder(limits *Limits, in io.Reader, options ...Option) (*Builder, error) {
	r := checkpoint.NewReader(in)
	if err := checkCompatible(limits, r); err != nil {
		return nil, err
	}
	restored := &Builder{
		limits: *limits,
	}
	for _, option := range options {
		option(restored)
	}
	if err := restored.restore(r); err != nil {
		return nil, err
	}
	if err := r.Finish(); err != nil {
		return nil, err
	}
	return restored, nil
}

func checkCompatible(limits *Limits, r *checkpoint.Reader) error {
	if r.Err() == nil && r.Version() != CheckpointVersion {
		return fmt.Errorf("%w: version %d, want %d", ErrCheckpointIncompatible, r.Version(), CheckpointVersion)
	}
	r.Begin(sectionLimits)
	saved := new(readLimits(r))
	r.End()
	if err := r.Err(); err != nil {
		return err
	}
	return limits.accepts(saved)
}

func (b *Builder) restore(r *checkpoint.Reader) error {
	var err error
	r.Begin(sectionBuilder)
	b.published = readPublished(r)
	r.End()
	r.Begin(sectionMatcher)
	if b.Matcher, err = matcher.Restore(b.limits.matcherLimits(), r); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	r.End()
	r.Begin(sectionGraph)
	if b.Graph, err = graph.Restore(b.limits.graphLimits(), r); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	r.End()
	r.Begin(sectionTraffic)
	if b.Traffic, err = traffic.Restore(b.limits.trafficLimits(), b.sink, subjectsOf(b.Graph), r); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	r.End()
	return r.Err()
}

func subjectsOf(services *graph.Graph) *traffic.Subjects {
	known := &traffic.Subjects{
		Nodes: make([]resource.ServiceRef, len(services.Nodes)),
		Edges: make([]traffic.EdgeSubject, len(services.Edges)),
	}
	handles := make(map[string]handle.Node, len(services.Nodes))
	for _, node := range services.Nodes {
		known.Nodes[node.Handle] = node.Service
		handles[node.ID] = node.Handle
	}
	for _, edge := range services.Edges {
		known.Edges[edge.Handle] = traffic.EdgeSubject{
			Caller: handles[edge.Source],
			Callee: handles[edge.Target],
		}
	}
	return known
}

func (l *Limits) Hash() uint64 {
	hash := fnv.New64a()
	w := checkpoint.NewWriter(hash, CheckpointVersion)
	section(w, sectionLimits, l.write)
	return hash.Sum64()
}

func (l *Limits) write(w *checkpoint.Writer) {
	w.Duration(l.PairingTTL)
	w.Duration(l.TombstoneTTL)
	w.Duration(l.EarlyFirePeriod)
	w.Duration(l.Windows.Size)
	w.Duration(l.Windows.Delay)
	w.Duration(l.Windows.AllowedLateness)
	w.Count(l.Windows.SlackWindows)
	for _, limit := range l.named() {
		w.Count(*limit.capacity)
	}
}

func readLimits(r *checkpoint.Reader) Limits {
	saved := Limits{
		PairingTTL:      r.Duration(),
		TombstoneTTL:    r.Duration(),
		EarlyFirePeriod: r.Duration(),
		Windows: stream.Config{
			Size:            r.Duration(),
			Delay:           r.Duration(),
			AllowedLateness: r.Duration(),
			SlackWindows:    r.Count(maxLimit),
		},
	}
	for _, limit := range saved.named() {
		*limit.capacity = r.Count(maxLimit)
	}
	return saved
}

type capacityName struct {
	name     string
	capacity *int
}

func (l *Limits) named() []capacityName {
	return []capacityName{
		{"max pending", &l.MaxPending},
		{"max tombstones", &l.MaxTombstones},
		{"max nodes", &l.MaxNodes},
		{"max edges", &l.MaxEdges},
		{"max series", &l.MaxSeries},
		{"max operations per service", &l.MaxOperationsPerService},
		{"sketch entries", &l.SketchEntries},
	}
}

func (l *Limits) accepts(saved *Limits) error {
	if l.Windows != saved.Windows {
		return fmt.Errorf("%w: windows %+v, checkpoint has %+v", ErrCheckpointIncompatible, l.Windows, saved.Windows)
	}
	if l.PairingTTL < saved.PairingTTL || l.TombstoneTTL < saved.TombstoneTTL {
		return fmt.Errorf("%w: pairing ttl %s and tombstone ttl %s are below the checkpoint's %s and %s",
			ErrCheckpointIncompatible, l.PairingTTL, l.TombstoneTTL, saved.PairingTTL, saved.TombstoneTTL)
	}
	return l.holdsCapacities(saved)
}

func (l *Limits) holdsCapacities(saved *Limits) error {
	current, previous := l.named(), saved.named()
	for i := range current {
		if *current[i].capacity < *previous[i].capacity {
			return fmt.Errorf("%w: %s %d is below the checkpoint's %d", ErrCheckpointIncompatible, current[i].name,
				*current[i].capacity, *previous[i].capacity)
		}
	}
	return nil
}

func (p published) write(w *checkpoint.Writer) {
	w.Uint(p.revision)
	w.Time(p.generatedAt)
	w.Count(p.nodes)
	w.Count(p.edges)
}

func readPublished(r *checkpoint.Reader) published {
	return published{
		revision:    r.Uint(),
		generatedAt: r.Time(),
		nodes:       r.Count(maxLimit),
		edges:       r.Count(maxLimit),
	}
}
