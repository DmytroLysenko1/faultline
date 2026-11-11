package traffic

import (
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

var ErrInvalidLimits = errors.New("invalid traffic limits")

type Limits struct {
	Windows         stream.Config
	EarlyFirePeriod time.Duration
	MaxNodes        int
	MaxEdges        int
	MaxSeries       int
	MaxPerService   int
	SketchEntries   int
}

func (l *Limits) Validate() error {
	if err := l.Windows.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	if l.EarlyFirePeriod <= 0 || l.EarlyFirePeriod > l.Windows.Size {
		return fmt.Errorf("%w: early fire period %s must be in (0, %s]", ErrInvalidLimits, l.EarlyFirePeriod,
			l.Windows.Size)
	}
	return l.validateCapacities()
}

func (l *Limits) validateCapacities() error {
	if l.MaxNodes < 1 || l.MaxEdges < 1 || l.MaxSeries < 1 {
		return fmt.Errorf("%w: max nodes %d, edges %d and series %d must be positive", ErrInvalidLimits, l.MaxNodes,
			l.MaxEdges, l.MaxSeries)
	}
	if !l.fitEngines() {
		return fmt.Errorf("%w: nodes %d, edges %d (and their sum) and series %d must not pass %d", ErrInvalidLimits,
			l.MaxNodes, l.MaxEdges, l.MaxSeries, stream.MaxCapacity)
	}
	return nil
}

func (l *Limits) fitEngines() bool {
	return l.MaxNodes <= stream.MaxCapacity && l.MaxEdges <= stream.MaxCapacity &&
		l.MaxNodes+l.MaxEdges <= stream.MaxCapacity && l.MaxSeries <= stream.MaxCapacity
}

func (l *Limits) subjects() int {
	return l.MaxNodes + l.MaxEdges
}

type Traffic struct {
	limits     Limits
	watermark  *stream.Watermark
	arena      *sketch.Arena
	registry   *cardinality.Registry
	subjects   *stream.Windows[subjectKey, red.Cell]
	operations *stream.Windows[operationKey, red.Counts]
	overflow   *stream.Windows[subjectKey, red.Counts]
	sink       FinalSink
	book       subjectBook
	views      views
	untimedAt  time.Time
	counters   counters
	final      Final
}

func New(limits *Limits, sink FinalSink) (*Traffic, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	built := &Traffic{
		limits: *limits,
		sink:   sink,
		book:   newSubjectBook(limits.MaxNodes, limits.MaxEdges),
		views:  newViews(limits.MaxNodes, limits.MaxEdges),
	}
	if sink == nil {
		built.sink = discard{}
	}
	if err := built.buildState(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	if err := built.buildEngines(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLimits, err)
	}
	return built, nil
}

func (t *Traffic) buildState() error {
	var err error
	if t.watermark, err = stream.NewWatermark(t.limits.Windows); err != nil {
		return fmt.Errorf("watermark: %w", err)
	}
	if t.arena, err = sketch.NewArena(t.limits.SketchEntries); err != nil {
		return fmt.Errorf("sketch arena: %w", err)
	}
	perService := cardinality.Limits{
		MaxPerService: t.limits.MaxPerService,
	}
	if t.registry, err = cardinality.NewRegistry(perService, t.limits.MaxNodes); err != nil {
		return fmt.Errorf("operation registry: %w", err)
	}
	return nil
}

func (t *Traffic) buildEngines() error {
	var err error
	cfg, subjects := t.limits.Windows, t.limits.subjects()
	if t.subjects, err = stream.New(cfg, stream.Options{
		Capacity: subjects,
		Early:    true,
	}, subjectSink{t}); err != nil {
		return fmt.Errorf("subject windows: %w", err)
	}
	if t.operations, err = stream.New(cfg, stream.Options{
		Capacity: t.limits.MaxSeries,
	}, operationSink{t}); err != nil {
		return fmt.Errorf("operation windows: %w", err)
	}
	if t.overflow, err = stream.New(cfg, stream.Options{
		Capacity: subjects,
	}, overflowSink{t}); err != nil {
		return fmt.Errorf("overflow windows: %w", err)
	}
	return nil
}
