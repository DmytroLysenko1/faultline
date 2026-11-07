package main

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

const (
	defaultWorstCaseBytes = 1_143_522_800
	manifestMemoryLimit   = 1408 << 20
	formerMemoryLimit     = 1280 << 20
)

func totalOf(cfg *Config) int64 {
	var total int64
	for _, part := range contributions(cfg) {
		total += part.bytes
	}
	return total
}

func TestDefaultWorstCaseIsPinned(t *testing.T) {
	if got := totalOf(new(defaultConfig())); got != defaultWorstCaseBytes {
		t.Errorf("default worst case = %d bytes (%s), pinned %d: update the I5 table in the design doc",
			got, inMiB(got), defaultWorstCaseBytes)
	}
}

func TestBudgetAtTheMemoryLimit(t *testing.T) {
	type args struct {
		cfg   Config
		limit int64
	}
	tests := []struct {
		Name    string
		Args    args
		WantErr error
	}{
		{
			Name: "defaults fit the manifest limit",
			Args: args{
				cfg:   defaultConfig(),
				limit: manifestMemoryLimit,
			},
		},
		{
			Name: "defaults do not fit the former 1280 MiB limit",
			Args: args{
				cfg:   defaultConfig(),
				limit: formerMemoryLimit,
			},
			WantErr: errOverBudget,
		},
		{
			Name: "every variable in range but the sum over the limit",
			Args: args{
				cfg: configWith(func(cfg *Config) {
					cfg.Journal.MaxBytes = 512 << 20
					cfg.Topology.MaxTombstones = 5_000_000
					cfg.Topology.MaxPending = 1_000_000
				}),
				limit: manifestMemoryLimit,
			},
			WantErr: errOverBudget,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if err := checkBudget(&tt.Args.cfg, tt.Args.limit); !errors.Is(err, tt.WantErr) {
				t.Errorf("checkBudget = %v, want %v", err, tt.WantErr)
			}
		})
	}
}

func TestBreakdownListsContributionsLargestFirstInMiB(t *testing.T) {
	parts := []contribution{
		{
			name:  "small",
			bytes: 1 << 20,
		},
		{
			name:  "large",
			bytes: 3 << 19,
		},
		{
			name:  "largest",
			bytes: 5 << 20,
		},
	}
	want := "largest 5.0 MiB, large 1.5 MiB, small 1.0 MiB"
	if diff := cmp.Diff(want, breakdown(parts)); diff != "" {
		t.Errorf("breakdown (-want +got):\n%s", diff)
	}
}

func TestLargestAllowedGraphResponseFitsTheSendLimit(t *testing.T) {
	if got := connectapi.WorstResponseBytes(maxGraphMaxNodes, maxGraphMaxEdges); got > connectapi.SendMaxBytes {
		t.Errorf("worst response at the range limits = %s, SendMaxBytes %s", inMiB(got), inMiB(connectapi.SendMaxBytes))
	}
}
