package main

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	defaultWorstCaseBytes = 1_418_350_848
	manifestMemoryLimit   = 1600 << 20
	tightMemoryLimit      = defaultWorstCaseBytes/budgetSharePercent*percent - mib
	applyProbeSpans       = 148_470
)

func TestDefaultWorstCaseIsPinned(t *testing.T) {
	if got := total(contributions(new(defaultConfig()))); got != defaultWorstCaseBytes {
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
			Name: "defaults do not fit a limit one MiB below what they need",
			Args: args{
				cfg:   defaultConfig(),
				limit: tightMemoryLimit,
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

func TestApplyProbeUsesTheLargestRequest(t *testing.T) {
	if otlp.MaxSpansPerRequest != applyProbeSpans {
		t.Errorf("otlp.MaxSpansPerRequest = %d, the topology Apply probe still uses %d: update largestRequestSpans",
			otlp.MaxSpansPerRequest, applyProbeSpans)
	}
}

func TestSelfTracingAddsItsQueueOnlyWhenEnabled(t *testing.T) {
	tests := []struct {
		Name string
		Args string
		Want int64
	}{
		{
			Name: "disabled",
		},
		{
			Name: "enabled",
			Args: "collector:4317",
			Want: selftrace.WorstHeapBytes(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := configWith(func(cfg *Config) {
				cfg.SelfTracingEndpoint = tt.Args
			})
			if got := total(contributions(&cfg)) - defaultWorstCaseBytes; got != tt.Want {
				t.Errorf("self-tracing contribution = %d, want %d", got, tt.Want)
			}
		})
	}
}
