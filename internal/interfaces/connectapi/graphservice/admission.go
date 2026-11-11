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
	a.Next.ServeHTTP(response,
		request.WithContext(context.WithValue(
			request.Context(),
			reservationKey{},
			held,
		),
		),
	)
}

func reserve(ctx context.Context, weight int64) error {
	held, admitted := ctx.Value(reservationKey{}).(*reservation)
	if !admitted {
		return ErrNotAdmitted
	}
	if weight > held.capacity {
		return fmt.Errorf("%w: %d bytes, budget %d", ErrTooLarge, weight, held.capacity)
	}
	if !held.budget.TryAcquire(weight) {
		return ErrBusy
	}
	held.held += weight
	return nil
}

type reservationKey struct{}

type reservation struct {
	budget   *semaphore.Weighted
	capacity int64
	held     int64
}

func (r *reservation) release() {
	r.budget.Release(r.held)
	r.held = 0
}
