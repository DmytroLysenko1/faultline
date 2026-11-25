package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestTheLeaseLineNamesALaterPartitionOnEveryPath(t *testing.T) {
	leaseExit := &unitExitError{
		name: "p7/lease",
		err:  errLeaseGone,
	}
	unitExit := &unitExitError{
		name: "a/run",
		err:  errA,
	}
	awaitLease := &stageError{
		stage: "journal/await_streams",
		err:   fmt.Errorf("await streams: %w", leaseExit),
	}
	tests := []struct {
		name string
		got  func() string
	}{
		{"rule 1", func() string {
			return partitionAttr(outcomeOf(&observed{
				stage: stageServing,
				cause: leaseExit,
				unit:  leaseExit,
			}))
		}},
		{"rule 2a", func() string {
			seen := &observed{
				stage: "journal",
				err:   awaitLease,
				cause: leaseExit,
				unit:  unitExit,
			}
			return partitionAttr(outcomeOf(seen))
		}},
		{"the late line", func() string {
			return leaseLossIn(keptLeaseExit{
				err: leaseExit,
			}, stageServing, nil).partition
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDiff(t, "partition", "p7", tt.got())
		})
	}
}

func TestTheLeaseBranchNamesThePartitionNotAnEarlierSource(t *testing.T) {
	unitExit := &unitExitError{
		name: "a/run",
		err:  errA,
	}
	err := errors.Join(&stageError{
		stage: "a/step",
		err:   errA,
	}, errStakeLease)
	seen := &observed{
		stage: "a",
		err:   err,
		cause: unitExit,
		unit:  unitExit,
	}
	assertDiff(t, "partition", "p0", partitionAttr(outcomeOf(seen)))
}

func TestALeaseExitBeforeTheSnapshotBeatsALeaseStopFailure(t *testing.T) {
	leaseExit := &unitExitError{
		name: "p0/lease",
		err:  errLeaseGone,
	}
	failures := []stopFailure{
		{
			layer: "p0",
			err:   errOrderStopLease,
		},
	}
	got := leaseLossIn(keptLeaseExit{
		err:            leaseExit,
		beforeSnapshot: true,
	}, "a", failures)
	want := &leaseLoss{
		stage:     "a",
		partition: "p0",
		err:       leaseExit,
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(leaseLoss{}), cmpopts.EquateErrors()); diff != "" {
		t.Errorf("loss (-want +got):\n%s", diff)
	}
}
