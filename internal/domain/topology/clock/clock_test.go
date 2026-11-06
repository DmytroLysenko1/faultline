package clock_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
)

func TestMonotonicNeverGoesBack(t *testing.T) {
	type tick struct {
		Elapsed time.Duration
		Now     time.Time
	}
	tests := []struct {
		Name string
		Args []time.Duration
		Want []tick
	}{
		{
			Name: "first reading is the epoch",
			Args: []time.Duration{
				0,
			},
			Want: []tick{
				{
					Elapsed: 0,
					Now:     telemetrytest.Epoch(),
				},
			},
		},
		{
			Name: "time moves forward",
			Args: []time.Duration{
				0,
				time.Second,
				3 * time.Second,
			},
			Want: []tick{
				{
					Elapsed: 0,
					Now:     telemetrytest.Epoch(),
				},
				{
					Elapsed: time.Second,
					Now:     telemetrytest.Epoch().Add(time.Second),
				},
				{
					Elapsed: 3 * time.Second,
					Now:     telemetrytest.Epoch().Add(3 * time.Second),
				},
			},
		},
		{
			Name: "a reading from the past keeps the latest moment",
			Args: []time.Duration{
				0,
				5 * time.Second,
				2 * time.Second,
				-time.Second,
			},
			Want: []tick{
				{
					Elapsed: 0,
					Now:     telemetrytest.Epoch(),
				},
				{
					Elapsed: 5 * time.Second,
					Now:     telemetrytest.Epoch().Add(5 * time.Second),
				},
				{
					Elapsed: 5 * time.Second,
					Now:     telemetrytest.Epoch().Add(5 * time.Second),
				},
				{
					Elapsed: 5 * time.Second,
					Now:     telemetrytest.Epoch().Add(5 * time.Second),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var monotonic clock.Monotonic
			got := make([]tick, 0, len(tt.Args))
			for _, offset := range tt.Args {
				elapsed := monotonic.Advance(telemetrytest.Epoch().Add(offset))
				got = append(got, tick{
					Elapsed: elapsed,
					Now:     monotonic.Now(),
				})
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("clock mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
