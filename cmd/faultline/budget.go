package main

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/selftrace"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	budgetSharePercent = 85
	percent            = 100
	mibFormat          = "%.1f MiB"
	contributionFormat = "%s %s"
	contributionSep    = ", "
)

var errOverBudget = errors.New("worst-case memory exceeds the budget")

type contribution struct {
	name  string
	bytes int64
}

func contributions(cfg *Config) []contribution {
	return append(coreContributions(cfg), selfTracingContribution(cfg)...)
}

func coreContributions(cfg *Config) []contribution {
	view := cfg.Topology.ViewHeapBytes()
	return []contribution{
		{name: "journal", bytes: cfg.Journal.WorstCaseHeapBytes()},
		{name: "pairing and graph", bytes: cfg.Topology.WorstCaseHeapBytes()},
		{name: "snapshots", bytes: (memgraph.HeldSnapshots + processing.HeldSnapshots) * view},
		{name: "batch apply", bytes: topology.ApplyHeapBytes(otlp.MaxSpansPerRequest)},
		{name: "api responses", bytes: connectapi.WorstHeapBytes(cfg.Topology.MaxNodes, cfg.Topology.MaxEdges, view)},
		{name: "otlp requests", bytes: otlp.WorstHeapBytes()},
	}
}

func selfTracingContribution(cfg *Config) []contribution {
	if cfg.SelfTracingEndpoint == "" {
		return nil
	}
	return []contribution{
		{
			name:  "self-tracing",
			bytes: selftrace.WorstHeapBytes(),
		},
	}
}

func checkBudget(cfg *Config, limit int64) error {
	parts := contributions(cfg)
	worst := total(parts)
	allowed := limit / percent * budgetSharePercent
	if worst <= allowed {
		return nil
	}
	return fmt.Errorf("%w: %s > %d%% of GOMEMLIMIT %s (%s)", errOverBudget, inMiB(worst), budgetSharePercent,
		inMiB(limit), breakdown(parts))
}

func total(parts []contribution) int64 {
	var sum int64
	for _, part := range parts {
		sum += part.bytes
	}
	return sum
}

func breakdown(parts []contribution) string {
	sorted := slices.SortedFunc(slices.Values(parts), func(a, b contribution) int {
		return cmp.Compare(b.bytes, a.bytes)
	})
	described := make([]string, 0, len(sorted))
	for _, part := range sorted {
		described = append(described, fmt.Sprintf(contributionFormat, part.name, inMiB(part.bytes)))
	}
	return strings.Join(described, contributionSep)
}

func inMiB(bytes int64) string {
	return fmt.Sprintf(mibFormat, float64(bytes)/mib)
}
