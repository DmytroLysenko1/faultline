package sketch

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"
)

var ErrInvalidBudget = errors.New("invalid sketch entry budget")

const (
	mappingBytes     = 16 << 10
	overheadBytes    = 4 << 10
	maxBudgetEntries = 1 << 28
)

type Percentiles struct {
	P50 int64
	P95 int64
	P99 int64
}

type ArenaStats struct {
	Budget            int
	InUse             int
	CollapsedAdds     uint64
	CollapsedSketches uint64
	DroppedSamples    uint64
}

type Arena struct {
	mapping     mapping
	blocks      buddy
	generations generations
	stats       ArenaStats
}

func NewArena(maxEntries int) (*Arena, error) {
	if err := validateBudget(maxEntries); err != nil {
		return nil, err
	}
	return &Arena{
		mapping:     newMapping(),
		blocks:      newBuddy(maxEntries),
		generations: newGenerations(maxEntries),
		stats: ArenaStats{
			Budget: maxEntries,
		},
	}, nil
}

func validateBudget(maxEntries int) error {
	switch {
	case maxEntries < MaxEntriesPerSketch:
		return fmt.Errorf("%w: %d entries is below one full sketch of %d", ErrInvalidBudget, maxEntries, MaxEntriesPerSketch)
	case maxEntries > maxBudgetEntries:
		return fmt.Errorf("%w: %d entries is above %d", ErrInvalidBudget, maxEntries, maxBudgetEntries)
	case bits.OnesCount(uint(maxEntries)) != 1:
		return fmt.Errorf("%w: %d entries is not a power of two", ErrInvalidBudget, maxEntries)
	}
	return nil
}

func WorstCaseHeapBytes(maxEntries int) int64 {
	return int64(maxEntries)*wordBytes +
		bitmapBytes(maxEntries) +
		generationsBytes(maxEntries) +
		mappingBytes +
		overheadBytes
}

func (a *Arena) Add(s *Sketch, nanos int64) {
	a.generations.check(s)
	if nanos <= 0 {
		a.addZero(s)
		return
	}
	a.addIndex(s, a.mapping.index(nanos))
}

func (a *Arena) addZero(s *Sketch) {
	if s.zero == maxZero {
		a.stats.DroppedSamples++
		return
	}
	s.zero++
	s.count++
}

func (a *Arena) Release(s *Sketch) {
	a.generations.check(s)
	a.releaseBlock(s)
	*s = Sketch{}
}

func (a *Arena) Stats() ArenaStats {
	return a.stats
}

func (a *Arena) Equal(x, y *Sketch) bool {
	a.generations.check(x)
	a.generations.check(y)
	return x.count == y.count &&
		x.zero == y.zero &&
		slices.Equal(a.entries(x), a.entries(y))
}

func (a *Arena) entries(s *Sketch) []entry {
	if !s.hasBlock() {
		return nil
	}
	return a.blocks.block(s.offset, s.order)[:s.length]
}
