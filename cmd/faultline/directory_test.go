package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
)

func TestTheDirectoryContributesOneMiB(t *testing.T) {
	assertDiff(t, "the snapshots pinned in natsjs/tokens and the header lists", int64(mib), int64(directoryHeap))
}

func TestTheDirectoryReportsNoAuthGroupBeforeItStarts(t *testing.T) {
	logger := testLogger(&logBuffer{})
	current, _ := directoryLayer(nil, defaultTenant, newLifecycle(logger), logger)

	sample := selfalert.Sample{}
	current.sample(t.Context(), &sample)

	assertDiff(t, "groups before the start", 0, len(current.statsGroups(t.Context())))
	assertDiff(t, "sample before the start", selfalert.Sample{}, sample)
}

func TestEveryTokenWaitNamesItsReason(t *testing.T) {
	tests := []struct {
		name string
		args error
		want string
	}{
		{
			name: "no bucket",
			args: fmt.Errorf("open: %w", tokens.ErrBucketMissing),
			want: reasonBucketMissing,
		},
		{
			name: "no leader",
			args: fmt.Errorf("open: %w", tokens.ErrNoLeader),
			want: reasonBucketLeaderless,
		},
		{
			name: "no quorum",
			args: fmt.Errorf("open: %w", tokens.ErrNoQuorum),
			want: reasonBucketQuorumless,
		},
		{
			name: "anything else",
			args: errTestSignal,
			want: reasonJetStreamUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDiff(t, "reason", tt.want, tokenWaitReason(tt.args))
		})
	}
}

func TestOnlyADriftedBucketHintsAtTheRecreateRunbook(t *testing.T) {
	tests := []struct {
		name string
		args error
		want string
	}{
		{
			name: "a bucket whose config injects grants",
			args: fmt.Errorf("open token directory: %w", tokens.ErrBucketDrift),
			want: hintBucketDrift,
		},
		{
			name: "a missing bucket",
			args: fmt.Errorf("open token directory: %w", tokens.ErrBucketMissing),
			want: "",
		},
		{
			name: "a refusal of another kind",
			args: errors.New("nats: permissions violation"),
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tokenStartHint(tt.args)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheDirectoryStatsFillTheAuthSample(t *testing.T) {
	type args struct {
		stats tokens.DirectoryStats
		fresh bool
	}
	tests := []struct {
		name string
		args args
		want selfalert.Sample
	}{
		{
			name: "a fresh directory without trouble",
			args: args{
				stats: tokens.DirectoryStats{
					Checks: 9,
				},
				fresh: true,
			},
			want: selfalert.Sample{},
		},
		{
			name: "a stale directory alone",
			args: args{},
			want: selfalert.Sample{
				TokensStale: true,
			},
		},
		{
			name: "a regressed bucket alone",
			args: args{
				stats: tokens.DirectoryStats{
					Regressed: true,
				},
				fresh: true,
			},
			want: selfalert.Sample{
				TokensRegressed: true,
			},
		},
		{
			name: "a bucket that blocks revocation alone",
			args: args{
				stats: tokens.DirectoryStats{
					RevokeBlocked: true,
				},
				fresh: true,
			},
			want: selfalert.Sample{
				RevokeBlocked: true,
			},
		},
		{
			name: "a drifted bucket alone",
			args: args{
				stats: tokens.DirectoryStats{
					ConfigDrift: true,
				},
				fresh: true,
			},
			want: selfalert.Sample{
				BucketDrift: true,
			},
		},
		{
			name: "epoch changes and mass revokes counted apart",
			args: args{
				stats: tokens.DirectoryStats{
					EpochChanges: 3,
					MassRevokes:  2,
				},
				fresh: true,
			},
			want: selfalert.Sample{
				EpochChanges: 3,
				MassRevokes:  2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selfalert.Sample{}
			fillAuthSample(&got, &tt.args.stats, tt.args.fresh)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
