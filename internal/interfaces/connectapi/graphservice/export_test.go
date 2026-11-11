package graphservice

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// AdmittedContext is the context Admission gives the handler, for tests that call GetGraph directly.
func AdmittedContext(ctx context.Context, budget int64) (context.Context, func()) {
	held := &reservation{
		budget:   semaphore.NewWeighted(budget),
		capacity: budget,
	}
	return context.WithValue(ctx, reservationKey{}, held), held.release
}
