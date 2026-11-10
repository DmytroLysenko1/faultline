package cardinality

import (
	"errors"
	"fmt"
	"math"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

var ErrInvalidLimits = errors.New("invalid cardinality limits")

type ID int32

type Limits struct {
	MaxPerService int
}

func (l Limits) fit(maxOwners int) bool {
	return l.MaxPerService >= 1 &&
		maxOwners >= 1 &&
		int64(maxOwners)*int64(l.MaxPerService) <= math.MaxInt32
}

type Stats struct {
	Operations      int
	OwnersOverLimit int
}

type key struct {
	owner     handle.Node
	operation operation.Operation
}

type entry struct {
	key        key
	references int
}

type Registry struct {
	index   map[key]ID
	entries []entry
	free    []ID
	next    ID
	owners  owners
}

func NewRegistry(limits Limits, maxOwners int) (*Registry, error) {
	if !limits.fit(maxOwners) {
		return nil, fmt.Errorf("%w: %+v for %d owners", ErrInvalidLimits, limits, maxOwners)
	}
	return &Registry{
		index:  make(map[key]ID),
		owners: newOwners(maxOwners, limits.MaxPerService),
	}, nil
}

func (r *Registry) Stats() Stats {
	return Stats{
		Operations:      len(r.index),
		OwnersOverLimit: r.owners.overLimitCount(),
	}
}

func (r *Registry) OwnersOverLimit() []handle.Node {
	return r.owners.overLimitList()
}
