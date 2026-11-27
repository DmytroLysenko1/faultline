package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const pinnedSnapshotBytes = 864 << 10

func TestTheDirectoryContributionCoversThePinnedSnapshotsAndTheHeaderLists(t *testing.T) {
	headerLists := int64(otlp.MaxConnections * otlp.MaxConcurrentStreams * otlp.MaxHeaderListBytes)
	if need := pinnedSnapshotBytes + headerLists; need > directoryHeap {
		t.Errorf("the snapshots pinned in natsjs/tokens and the header lists need %s, the contribution is %s",
			inMiB(need), inMiB(directoryHeap))
	}
}

func TestTheDirectoryReportsNoAuthGroupBeforeItStarts(t *testing.T) {
	logger := testLogger(&logBuffer{})
	current, _ := directoryLayer(nil, defaultTenant, newLifecycle(logger), logger)

	assertDiff(t, "groups before the start", 0, len(current.statsGroups(t.Context())))
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
