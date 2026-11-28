package main

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	manifestMemoryLimit = 1632 << 20
	applyProbeSpans     = 148_470
)

func allContributions(t *testing.T, values map[string]string) []contribution {
	t.Helper()
	setup, err := loadAll(processOf(processorEnvironment(t, values)))
	if err != nil {
		t.Fatalf("loadAll: %v", err)
	}
	logger := testLogger(&logBuffer{})
	return allStack(&setup, newLifecycle(logger), logger).contributions()
}

func TestBudgetAtTheMemoryLimit(t *testing.T) {
	type args struct {
		values map[string]string
		limit  func(worst int64) int64
	}
	manifest := func(int64) int64 { return manifestMemoryLimit }
	tests := []struct {
		Name    string
		Args    args
		WantErr error
	}{
		{
			Name: "defaults fit the manifest limit",
			Args: args{
				values: map[string]string{},
				limit:  manifest,
			},
		},
		{
			Name: "defaults do not fit a limit one MiB below what they need",
			Args: args{
				values: map[string]string{},
				limit:  func(worst int64) int64 { return worst/budgetSharePercent*percent - mib },
			},
			WantErr: errOverBudget,
		},
		{
			Name: "every variable in range but the sum over the limit",
			Args: args{
				values: map[string]string{
					envFeedMaxBytes:      "512MiB",
					envTombstoneMax:      "5000000",
					envPairingMaxPending: "1000000",
				},
				limit: manifest,
			},
			WantErr: errOverBudget,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			parts := allContributions(t, tt.Args.values)
			if err := checkParts(parts, tt.Args.limit(total(parts))); !errors.Is(err, tt.WantErr) {
				t.Errorf("checkParts = %v, want %v", err, tt.WantErr)
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
	if got := graphservice.ResponseBytes(maxGraphMaxNodes, maxGraphMaxEdges); got > connectapi.SendMaxBytes {
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
			cfg := Config{
				SelfTracingEndpoint: tt.Args,
			}
			traced, _ := tracingLayer(&cfg, nil)
			if got := total(traced.contributions); got != tt.Want {
				t.Errorf("self-tracing contribution = %d, want %d", got, tt.Want)
			}
		})
	}
}

const (
	pinnedDefaultResponseWeight = 257_762_560
	responseBudgetStep          = 16 * mib
)

func TestDefaultResponseBudgetHoldsTheWorstResponseAtDefaultLimits(t *testing.T) {
	weight := graphservice.ResponseWeight(defaultGraphMaxNodes, defaultGraphMaxEdges)
	if weight != pinnedDefaultResponseWeight {
		t.Errorf("worst response at the default limits = %d B (%s), pinned %d: update the I5 table", weight,
			inMiB(weight), pinnedDefaultResponseWeight)
	}
	roundedUp := (weight + responseBudgetStep - 1) / responseBudgetStep * responseBudgetStep
	if got := int64(connectapi.DefaultResponseBudget); got != roundedUp {
		t.Errorf("default response budget %s, want the worst response rounded up to 16 MiB: %s", inMiB(got),
			inMiB(roundedUp))
	}
}
