package main

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	heldSnapshots   = 2
	subjectTemplate = "$KV." + tokens.Bucket + ".%012d"
	idAlphabet      = "abcdefghijklmnopqrstuvwxyz234567"
)

func fullSnapshot(t *testing.T, seed int) credential.Grants {
	t.Helper()
	grants := credential.Grants{}
	tenant := strings.Repeat("t", record.MaxTenantBytes)
	token, err := credential.Parse(otlptest.Token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for index := range tokens.MaxGrants {
		id := credential.ID(idOf(seed*tokens.MaxGrants + index))
		grant, err := credential.NewGrant(id, tenant, token.Digest(), time.Now())
		if err != nil {
			t.Fatalf("NewGrant: %v", err)
		}
		grants[id] = grant
	}
	return grants
}

func idOf(number int) string {
	id := make([]byte, credential.IDLength)
	for position := range id {
		id[position] = idAlphabet[number%len(idAlphabet)]
		number /= len(idAlphabet)
	}
	return string(id)
}

func rereadSubjects() map[string]uint64 {
	subjects := make(map[string]uint64, tokens.MaxGrants)
	for index := range tokens.MaxGrants {
		subjects[fmt.Sprintf(subjectTemplate, index)] = 1
	}
	return subjects
}

func TestTheDirectoryContributionHoldsTwoFullSnapshotsAndTheHeaderLists(t *testing.T) {
	before := otlptest.HeapInUse()
	snapshots := make([]credential.Grants, 0, heldSnapshots)
	for seed := range heldSnapshots {
		snapshots = append(snapshots, fullSnapshot(t, seed))
	}
	subjects := rereadSubjects()
	held := otlptest.HeapInUse() - before
	runtime.KeepAlive(snapshots)
	runtime.KeepAlive(subjects)
	headerLists := int64(otlp.MaxConnections * otlp.MaxConcurrentStreams * otlp.MaxHeaderListBytes)
	if need := held + headerLists; need > directoryHeap {
		t.Errorf("two full snapshots, a reread and the header lists hold %s, the contribution is %s",
			inMiB(need), inMiB(directoryHeap))
	}
	t.Logf("held %s + header lists %s of %s", inMiB(held), inMiB(headerLists), inMiB(directoryHeap))
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
