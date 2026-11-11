package graphservice

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"golang.org/x/sync/semaphore"
)

var (
	ErrBusy          = errors.New("graph responses fill the response budget")
	ErrTooLarge      = errors.New("graph response is larger than the whole response budget")
	ErrNotAdmitted   = errors.New("graph handler runs without admission")
	ErrInvalidBudget = errors.New("invalid api response budget")
)

type Admission struct {
	Budget   *semaphore.Weighted
	Capacity int64
	Next     http.Handler
}

func NewAdmission(budget int64, next http.Handler) (*Admission, error) {
	if budget < 1 {
		return nil, fmt.Errorf("%w: %d bytes", ErrInvalidBudget, budget)
	}
	return &Admission{
		Budget:   semaphore.NewWeighted(budget),
		Capacity: budget,
		Next:     next,
	}, nil
}

func (a *Admission) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	held := &reservation{
		budget:   a.Budget,
		capacity: a.Capacity,
	}
	defer held.release()
	a.Next.ServeHTTP(&shrinkingWriter{
		ResponseWriter: response,
		held:           held,
	},
		request.WithContext(context.WithValue(
			request.Context(),
			reservationKey{},
			held,
		),
		),
	)
}

func reserve(ctx context.Context, nodes, edges int) error {
	held, admitted := ctx.Value(reservationKey{}).(*reservation)
	if !admitted {
		return ErrNotAdmitted
	}
	weight := ResponseWeight(nodes, edges)
	if weight > held.capacity {
		return fmt.Errorf("%w: %d bytes, budget %d", ErrTooLarge, weight, held.capacity)
	}
	if !held.budget.TryAcquire(weight) {
		return ErrBusy
	}
	held.held += weight
	held.nodes, held.edges = nodes, edges
	return nil
}

type reservationKey struct{}

type reservation struct {
	budget   *semaphore.Weighted
	capacity int64
	held     int64
	nodes    int
	edges    int
}

func (r *reservation) shrink(compressed bool) {
	written := WrittenWeight(r.nodes, r.edges)
	if compressed {
		written += ResponseBytes(r.nodes, r.edges)
	}
	if r.held <= written {
		return
	}
	r.budget.Release(r.held - written)
	r.held = written
}

func (r *reservation) release() {
	r.budget.Release(r.held)
	r.held = 0
}
