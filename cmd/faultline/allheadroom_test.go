package main

import (
	"strconv"
	"testing"
	"time"
)

const (
	allHeadroomFloor      = 36 * mib
	pinnedAllHeadroom     = 42_433_143
	pinnedProcessorArena  = -8_684_937
	processorArenaEntries = 8 << 20
)

func allHeadroom(t *testing.T, values map[string]string) int64 {
	t.Helper()
	return defaultMemoryLimit/percent*budgetSharePercent - total(allContributions(t, values))
}

func TestTheAllHeadroomIsPinned(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want int64
	}{
		{
			name: "the defaults of all",
			args: map[string]string{},
			want: pinnedAllHeadroom,
		},
		{
			name: "the arena of the processor",
			args: map[string]string{
				envSketchEntries: strconv.Itoa(processorArenaEntries),
			},
			want: pinnedProcessorArena,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDiff(t, "headroom", tt.want, allHeadroom(t, tt.args))
		})
	}
	if headroom := allHeadroom(t, map[string]string{}); headroom < allHeadroomFloor {
		t.Errorf("all headroom %s at the defaults, want at least %s", inMiB(headroom), inMiB(allHeadroomFloor))
	}
}

func TestTheAllDefaultsYieldToTheEnvironment(t *testing.T) {
	type want struct {
		Entries   int
		Tombstone time.Duration
		Pending   int
	}
	tests := []struct {
		name string
		args map[string]string
		want want
	}{
		{
			name: "the defaults",
			args: map[string]string{},
			want: want{
				Entries:   2 << 20,
				Tombstone: 15 * time.Second,
				Pending:   defaultMaxPending,
			},
		},
		{
			name: "the environment",
			args: map[string]string{
				envSketchEntries: "4194304",
				envTombstoneTTL:  "2m",
			},
			want: want{
				Entries:   4 << 20,
				Tombstone: 2 * time.Minute,
				Pending:   defaultMaxPending,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup, err := loadAll(processOf(processorEnvironment(t, tt.args)))
			if err != nil {
				t.Fatalf("loadAll: %v", err)
			}
			limits := setup.Partition.Limits
			got := want{
				Entries:   limits.SketchEntries,
				Tombstone: limits.TombstoneTTL,
				Pending:   limits.MaxPending,
			}
			assertDiff(t, "all limits", tt.want, got)
		})
	}
}
