package tombstone

import (
	"errors"
	"fmt"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
)

const (
	minCapacity  = 16
	growthFactor = 2
)

var ErrInvalidLimit = errors.New("invalid tombstone limit")

type Scope uint8

const (
	BothSides Scope = iota
	CallerOnly
)

func (s Scope) Suppresses(side role.Role) bool {
	return s == BothSides || side == role.Caller
}

type Entry struct {
	Key      ids.CallKey
	Deadline time.Duration
}

type Set struct {
	ScopeByKey map[ids.CallKey]Scope
	Ring       []Entry
	Head       int
	Size       int
	Limit      int
}

func New(limit int) (Set, error) {
	if limit <= 0 {
		return Set{}, fmt.Errorf("%w: max tombstones %d must be positive", ErrInvalidLimit, limit)
	}
	return Set{
		ScopeByKey: make(map[ids.CallKey]Scope),
		Limit:      limit,
	}, nil
}

func (s *Set) Suppresses(key ids.CallKey, side role.Role) bool {
	scope, found := s.ScopeByKey[key]
	if !found {
		return false
	}
	return scope.Suppresses(side)
}

func (s *Set) Add(key ids.CallKey, scope Scope, deadline time.Duration) (evicted bool) {
	if _, found := s.ScopeByKey[key]; found {
		return false
	}
	if s.Size == s.Limit {
		s.removeOldest()
		evicted = true
	}
	s.grow()
	s.Ring[(s.Head+s.Size)%len(s.Ring)] = Entry{
		Key:      key,
		Deadline: deadline,
	}
	s.Size++
	s.ScopeByKey[key] = scope
	return evicted
}

func (s *Set) Expire(now time.Duration) {
	for s.Size > 0 && s.Ring[s.Head].Deadline <= now {
		s.removeOldest()
	}
}

func (s *Set) removeOldest() {
	delete(s.ScopeByKey, s.Ring[s.Head].Key)
	s.Ring[s.Head] = Entry{}
	s.Head = (s.Head + 1) % len(s.Ring)
	s.Size--
}

func (s *Set) grow() {
	if s.Size < len(s.Ring) {
		return
	}
	capacity := min(max(growthFactor*len(s.Ring), minCapacity), s.Limit)
	ring := make([]Entry, capacity)
	for i := range s.Size {
		ring[i] = s.Ring[(s.Head+i)%len(s.Ring)]
	}
	s.Ring = ring
	s.Head = 0
}
