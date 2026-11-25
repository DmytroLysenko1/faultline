package natsjs_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

func TestRepairedByEnsureIsFalseWithoutBlockingDrift(t *testing.T) {
	missing := natsjs.Drift{
		Stream: "SPANS_P0",
		Field:  "stream",
		Want:   "present",
		Got:    "missing",
	}
	shortWindow := natsjs.Drift{
		Stream: "SPANS_P0",
		Field:  "duplicate_window",
		Want:   (5 * time.Minute).String(),
		Got:    time.Minute.String(),
	}
	replicas := natsjs.Drift{
		Stream: "SPANS_P0",
		Field:  "replicas",
		Want:   "3",
		Got:    "1",
	}
	tests := []struct {
		name string
		args natsjs.Verification
		want bool
	}{
		{
			name: "nothing blocks, a note only",
			args: natsjs.Verification{
				Notes: []natsjs.Drift{shortWindow},
			},
		},
		{
			name: "the stream is missing",
			args: natsjs.Verification{
				Blocking: []natsjs.Drift{missing},
			},
			want: true,
		},
		{
			name: "the dedup window is shorter",
			args: natsjs.Verification{
				Blocking: []natsjs.Drift{shortWindow},
			},
			want: true,
		},
		{
			name: "an immutable field drifted",
			args: natsjs.Verification{
				Blocking: []natsjs.Drift{replicas},
			},
		},
		{
			name: "a missing stream next to an immutable drift",
			args: natsjs.Verification{
				Blocking: []natsjs.Drift{missing, replicas},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.args.RepairedByEnsure()); diff != "" {
				t.Errorf("RepairedByEnsure (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAMissingStreamIsRepairedByTheProcessorsEnsure(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	conn := dial(t, cluster)
	topology := topologyOf(onePartition, oneReplica)

	verified, err := conn.VerifyTopology(t.Context(), topology)

	if !verified.RepairedByEnsure() {
		t.Fatalf("VerifyTopology on an empty server = %+v, %v; want a drift the processor repairs", verified, err)
	}
	if err := conn.EnsureTopology(t.Context(), topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	if _, err := conn.VerifyTopology(t.Context(), topology); err != nil {
		t.Errorf("VerifyTopology after the processor ensured = %v, want nil", err)
	}
}

func TestAShortDedupWindowIsRepairedByTheProcessorsEnsure(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	conn := dial(t, cluster)
	short := topologyOf(onePartition, oneReplica)
	short.DuplicateWindow = time.Minute
	if err := conn.EnsureTopology(t.Context(), short, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology with a short window: %v", err)
	}
	topology := topologyOf(onePartition, oneReplica)

	verified, err := conn.VerifyTopology(t.Context(), topology)

	if !verified.RepairedByEnsure() {
		t.Fatalf("VerifyTopology on a short window = %+v, %v; want a drift the processor repairs", verified, err)
	}
	if err := conn.EnsureTopology(t.Context(), topology, natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	if _, err := conn.VerifyTopology(t.Context(), topology); err != nil {
		t.Errorf("VerifyTopology after the processor raised the window = %v, want nil", err)
	}
}
