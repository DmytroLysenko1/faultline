package stream

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var ErrInvalidOptions = errors.New("invalid window options")

const (
	memberBytes  = 4
	freeBytes    = 4
	slotRefBytes = 8
	controlBytes = 1

	mapSlotsPerGroup  = 8
	mapEntriesInGroup = 7
	mapRoundUp        = 2
)

const maxCapacity = 1 << 24

type Sink[K comparable, C any] interface {
	Emit(key K, pane Pane, cell *C)
	Retire(key K, window Window, cell *C)
	Closed(key K)
}

type Options struct {
	Capacity int
	Early    bool
}

type seriesState[K comparable] struct {
	key  K
	live int32
}

type batchState struct {
	open      bool
	known     bool
	markNanos int64
	dropBelow int64
	openFrom  int64
	firstLive int64
	anchorID  int64
}

type Windows[K comparable, C any] struct {
	cfg      Config
	slotsPer int64
	early    bool
	sink     Sink[K, C]
	index    map[K]int32
	series   []seriesState[K]
	slots    []slot[C]
	free     []int32
	buckets  []bucket
	order    []int
	late     []slotRef
	earliest time.Time
	latest   time.Time
	batch    batchState
	stats    Stats
}

func New[K comparable, C any](cfg Config, options Options, sink Sink[K, C]) (*Windows[K, C], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if options.Capacity <= 0 || options.Capacity > maxCapacity || sink == nil {
		return nil, fmt.Errorf("%w: capacity %d must be in [1, %d] and a sink is required", ErrInvalidOptions,
			options.Capacity, maxCapacity)
	}
	slotsPer := cfg.Slots()
	return &Windows[K, C]{
		cfg:      cfg,
		slotsPer: int64(slotsPer),
		early:    options.Early,
		sink:     sink,
		index:    make(map[K]int32, options.Capacity),
		series:   make([]seriesState[K], options.Capacity),
		slots:    make([]slot[C], options.Capacity*slotsPer),
		free:     freeList(options.Capacity),
		buckets:  newBuckets(slotsPer, options.Capacity),
		order:    make([]int, 0, slotsPer),
		late:     make([]slotRef, 0, options.Capacity*cfg.lateWindows()),
		earliest: time.Unix(0, math.MinInt64),
		latest:   time.Unix(0, math.MaxInt64),
		stats: Stats{
			Capacity: options.Capacity,
		},
	}, nil
}

func (w *Windows[K, C]) Has(key K) bool {
	_, ok := w.index[key]
	return ok
}

func (w *Windows[K, C]) Len() int {
	return len(w.index)
}

func (w *Windows[K, C]) Capacity() int {
	return len(w.series)
}

func (w *Windows[K, C]) Stats() Stats {
	stats := w.stats
	stats.Series = len(w.index)
	return stats
}

func freeList(capacity int) []int32 {
	free := make([]int32, capacity)
	for i := range free {
		free[i] = seriesIndex(capacity - 1 - i)
	}
	return free
}

func newBuckets(count, capacity int) []bucket {
	buckets := make([]bucket, count)
	for i := range buckets {
		buckets[i].members = make([]int32, 0, capacity)
	}
	return buckets
}

type indexEntry[K comparable] struct {
	_ K
	_ int32
}

func WorstCaseHeapBytes[K comparable, C any](cfg Config, capacity int) int64 {
	slots := int64(cfg.Slots())
	indexBytes := (sizeOf[indexEntry[K]]() + controlBytes) * mapSlotsPerGroup * mapRoundUp / mapEntriesInGroup
	perSeries := slots*(sizeOf[slot[C]]()+memberBytes) + sizeOf[seriesState[K]]() + indexBytes + freeBytes +
		int64(cfg.lateWindows())*slotRefBytes
	return int64(capacity) * perSeries
}
