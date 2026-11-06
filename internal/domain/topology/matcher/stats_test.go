package matcher_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology/matcher"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
)

func TestStatsTransitionsKeepI1(t *testing.T) {
	waiting := matcher.Stats{
		Observed: 2,
		Pending:  2,
	}
	tests := []struct {
		Name    string
		Args    func(stats *matcher.Stats)
		Want    matcher.Stats
		WantErr error
	}{
		{
			Name: "ignored span is not observed",
			Args: func(stats *matcher.Stats) {
				stats.Observe(role.Ignored)
			},
			Want: waiting,
		},
		{
			Name: "unpairable span is observed and set aside",
			Args: func(stats *matcher.Stats) {
				stats.Observe(role.Unpairable)
			},
			Want: matcher.Stats{
				Observed:   3,
				Unpairable: 1,
				Pending:    2,
			},
		},
		{
			Name: "duplicate",
			Args: func(stats *matcher.Stats) {
				stats.Observe(role.Caller)
				stats.RecordDuplicate()
			},
			Want: matcher.Stats{
				Observed:  3,
				Duplicate: 1,
				Pending:   2,
			},
		},
		{
			Name: "new half waits",
			Args: func(stats *matcher.Stats) {
				stats.Observe(role.Callee)
				stats.RecordWait()
			},
			Want: matcher.Stats{
				Observed: 3,
				Pending:  3,
			},
		},
		{
			Name: "match takes the waiting half and the arrived one",
			Args: func(stats *matcher.Stats) {
				stats.Observe(role.Callee)
				stats.RecordMatch()
			},
			Want: matcher.Stats{
				Observed: 3,
				Matched:  1,
				Pending:  1,
			},
		},
		{
			Name: "evicted caller",
			Args: func(stats *matcher.Stats) {
				stats.RecordEviction(role.Caller)
			},
			Want: matcher.Stats{
				Observed:      2,
				Evicted:       1,
				EvictedCaller: 1,
				Pending:       1,
			},
		},
		{
			Name: "evicted callee",
			Args: func(stats *matcher.Stats) {
				stats.RecordEviction(role.Callee)
			},
			Want: matcher.Stats{
				Observed: 2,
				Evicted:  1,
				Pending:  1,
			},
		},
		{
			Name: "expired caller",
			Args: func(stats *matcher.Stats) {
				stats.RecordExpiry(role.Caller)
			},
			Want: matcher.Stats{
				Observed:      2,
				Expired:       1,
				ExpiredCaller: 1,
				Pending:       1,
			},
		},
		{
			Name: "expired callee",
			Args: func(stats *matcher.Stats) {
				stats.RecordExpiry(role.Callee)
			},
			Want: matcher.Stats{
				Observed: 2,
				Expired:  1,
				Pending:  1,
			},
		},
		{
			Name: "tombstone eviction is outside the I1 buckets",
			Args: func(stats *matcher.Stats) {
				stats.RecordTombstoneEviction()
			},
			Want: matcher.Stats{
				Observed:         2,
				Pending:          2,
				TombstoneEvicted: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := waiting
			tt.Args(&got)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("stats mismatch (-want +got):\n%s", diff)
			}
			if !got.Balanced() {
				t.Errorf("I1 broken after transition: %+v", got)
			}
		})
	}
}

func TestStatsBalanced(t *testing.T) {
	tests := []struct {
		Name    string
		Args    matcher.Stats
		Want    bool
		WantErr error
	}{
		{
			Name: "empty",
			Args: matcher.Stats{},
			Want: true,
		},
		{
			Name: "every bucket filled",
			Args: matcher.Stats{
				Observed:   7,
				Matched:    1,
				Duplicate:  1,
				Unpairable: 1,
				Evicted:    1,
				Expired:    1,
				Pending:    1,
			},
			Want: true,
		},
		{
			Name: "a matched pair counts two spans",
			Args: matcher.Stats{
				Observed: 1,
				Matched:  1,
			},
			Want: false,
		},
		{
			Name: "a lost span",
			Args: matcher.Stats{
				Observed: 3,
				Pending:  2,
			},
			Want: false,
		},
		{
			Name: "caller sub-counters are not buckets",
			Args: matcher.Stats{
				Observed:         1,
				Expired:          1,
				ExpiredCaller:    1,
				TombstoneEvicted: 5,
			},
			Want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, tt.Args.Balanced()); diff != "" {
				t.Errorf("Balanced mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
