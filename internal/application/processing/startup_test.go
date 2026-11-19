package processing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

func TestOnlyAnUnusableCheckpointMayBeDiscarded(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "an unusable checkpoint", err: processing.ErrCheckpointUnusable, want: true},
		{name: "unusable and moved: a retry, not a loss", err: errors.Join(processing.ErrCheckpointUnusable,
			processing.ErrCheckpointMoved)},
		{name: "unusable and unavailable", err: errors.Join(processing.ErrCheckpointUnusable,
			processing.ErrCheckpointUnavailable)},
		{name: "unusable and refused", err: errors.Join(processing.ErrCheckpointUnusable,
			processing.ErrCheckpointRefused)},
		{name: "unusable after the lease was lost", err: errors.Join(processing.ErrCheckpointUnusable,
			processing.ErrLeaseLost)},
		{name: "unusable but canceled", err: errors.Join(processing.ErrCheckpointUnusable, context.Canceled)},
		{name: "unusable but out of time", err: errors.Join(processing.ErrCheckpointUnusable, context.DeadlineExceeded)},
		{name: "moved", err: processing.ErrCheckpointMoved},
		{name: "refused", err: processing.ErrCheckpointRefused},
		{name: "unavailable", err: processing.ErrCheckpointUnavailable},
		{name: "a refused discard", err: processing.ErrDiscardRefused},
		{name: "no checkpoint", err: processing.ErrNoCheckpoint},
		{name: "nothing failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processing.Discardable(tt.err); got != tt.want {
				t.Fatalf("Discardable(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
