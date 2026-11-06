package matcher

import "github.com/DmytroLysenko1/faultline/internal/domain/topology/role"

const SpansPerCall = 2

type Stats struct {
	Observed         uint64
	Matched          uint64
	Duplicate        uint64
	Unpairable       uint64
	Evicted          uint64
	Expired          uint64
	Pending          uint64
	ExpiredCaller    uint64
	EvictedCaller    uint64
	TombstoneEvicted uint64
}

func (s *Stats) Observe(side role.Role) (pairable bool) {
	if !side.Participates() {
		return false
	}
	s.Observed++
	if !side.Pairable() {
		s.Unpairable++
		return false
	}
	return true
}

func (s *Stats) RecordDuplicate() {
	s.Duplicate++
}

func (s *Stats) RecordWait() {
	s.Pending++
}

func (s *Stats) RecordMatch() {
	s.Pending--
	s.Matched++
}

func (s *Stats) RecordEviction(side role.Role) {
	s.Pending--
	s.Evicted++
	if side == role.Caller {
		s.EvictedCaller++
	}
}

func (s *Stats) RecordExpiry(side role.Role) {
	s.Pending--
	s.Expired++
	if side == role.Caller {
		s.ExpiredCaller++
	}
}

func (s *Stats) RecordTombstoneEviction() {
	s.TombstoneEvicted++
}

func (s *Stats) Balanced() bool {
	return s.Observed == SpansPerCall*s.Matched+s.Duplicate+s.Unpairable+s.Evicted+s.Expired+s.Pending
}
