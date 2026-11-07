package memgraph

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

var (
	ErrNilSnapshot = errors.New("nil snapshot")
	ErrNoSnapshot  = errors.New("no snapshot published")
)

const HeldSnapshots = 1

type Store struct {
	Current atomic.Pointer[topology.Snapshot]
}

func New(initial *topology.Snapshot) (*Store, error) {
	if initial == nil {
		return nil, ErrNilSnapshot
	}
	store := &Store{}
	store.Current.Store(initial)
	return store, nil
}

func (s *Store) Publish(ctx context.Context, snapshot *topology.Snapshot) error {
	if snapshot == nil {
		return ErrNilSnapshot
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish snapshot revision %d: %w", snapshot.Revision, err)
	}
	s.Current.Store(snapshot)
	return nil
}

func (s *Store) Latest(ctx context.Context) (*topology.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("latest snapshot: %w", err)
	}
	latest := s.Current.Load()
	if latest == nil {
		return nil, ErrNoSnapshot
	}
	return latest, nil
}
