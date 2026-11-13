package tombstone

import (
	"math"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func (s *Set) WriteCheckpoint(w *checkpoint.Writer) {
	w.Count(s.Size)
	for i := range s.Size {
		entry := s.Ring[(s.Head+i)%len(s.Ring)]
		w.Fixed(entry.Key.TraceID[:])
		w.Fixed(entry.Key.ID[:])
		w.Duration(entry.Deadline)
		w.Uint(uint64(s.ScopeByKey[entry.Key]))
	}
}

func Restore(limit int, r *checkpoint.Reader) (Set, error) {
	restored, err := New(limit)
	if err != nil {
		return Set{}, err
	}
	size, previous := r.Count(limit), time.Duration(math.MinInt64)
	for range size {
		previous = restored.readEntry(r, previous)
	}
	return restored, nil
}

func (s *Set) readEntry(r *checkpoint.Reader, previous time.Duration) time.Duration {
	var key ids.CallKey
	r.Fixed(key.TraceID[:])
	r.Fixed(key.ID[:])
	deadline := r.Duration()
	scope := Scope(r.Uint8(uint8(CallerOnly)))
	if _, repeated := s.ScopeByKey[key]; repeated || deadline < previous {
		r.Fail("a tombstone is repeated or out of deadline order")
		return previous
	}
	s.Add(key, scope, deadline)
	return deadline
}
