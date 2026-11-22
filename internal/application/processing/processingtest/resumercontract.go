package processingtest

import (
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	contractSequence = 42
	partialBytes     = 1
	foreignSuffix    = "-foreign"
)

type ResumerHarness struct {
	Store          CheckpointResumer
	Partition      uint16
	WithCheckpoint func(sequence uint64) (processing.DiscardTarget, error)
	Corrupt        func() (processing.DiscardTarget, error)
	BreakImage     func() error
	Unpoint        func() error
	MovePointer    func() error
}

type resumerRow struct {
	name string
	run  func(t *testing.T, harness *ResumerHarness)
}

func RunCheckpointResumerContract(t *testing.T, open func(t *testing.T) ResumerHarness) {
	t.Helper()
	for _, row := range []resumerRow{
		{name: "a checkpoint read to its end is staked where it ends", run: drainedIsStaked},
		{name: "a stake before the close of a drained reader passes", run: stakeBeforeClose},
		{name: "a checkpoint nobody read is not staked", run: unreadIsRefused},
		{name: "a checkpoint staked while its reader is still open and unread is refused", run: openUnreadIsRefused},
		{name: "an earlier reader read after the pointer emptied keeps the empty verdict", run: lateReadKeepsEmpty},
		{name: "an early close refuses the stake and keeps the discard", run: earlyCloseKeepsDiscard},
		{name: "a read cut by a moved pointer refuses the discard", run: movedReadRefusesDiscard},
		{name: "a late close of an earlier reader leaves the next verdict", run: lateCloseKeepsNext},
		{name: "a stake without a latest is refused", run: stakeWithoutLatest},
		{name: "a stake after a refused latest is refused", run: stakeAfterRefusedLatest},
		{name: "a refused latest is discarded at the incident start", run: refusedLatestDiscarded},
		{name: "a broken image is not staked but may be discarded", run: brokenImageDiscarded},
		{name: "the target of an early close is the one discard accepts", run: earlyCloseTargetAccepted},
		{name: "the target of a refused latest is the one discard accepts", run: refusedLatestTargetAccepted},
		{name: "an absent checkpoint names no target", run: noTargetWithoutRefusal},
		{name: "a discard while the reader is open and unread is refused", run: openUnreadDiscardRefused},
		{name: "a discard without a latest is refused", run: discardWithoutLatest},
		{name: "a discard naming another checkpoint is refused", run: foreignTargetRefused},
		{name: "a refused discard leaves the stake of the drained checkpoint", run: stakeAfterRefusedDiscard},
	} {
		t.Run(row.name, func(t *testing.T) {
			harness := open(t)
			row.run(t, &harness)
		})
	}
}

func withCheckpoint(t *testing.T, harness *ResumerHarness) processing.DiscardTarget {
	t.Helper()
	target, err := harness.WithCheckpoint(contractSequence)
	if err != nil {
		t.Fatalf("WithCheckpoint: %v", err)
	}
	return target
}

func latestReader(t *testing.T, harness *ResumerHarness) io.ReadCloser {
	t.Helper()
	_, reader, err := harness.Store.Latest(t.Context(), harness.Partition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	return reader
}

func drainReader(t *testing.T, reader io.Reader) {
	t.Helper()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("read the checkpoint to its end: %v", err)
	}
}

func closeReader(t *testing.T, reader io.Closer) {
	t.Helper()
	if err := reader.Close(); err != nil {
		t.Fatalf("close the checkpoint: %v", err)
	}
}

func step(t *testing.T, what string, run func() error) {
	t.Helper()
	if err := run(); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func emptyStake(harness *ResumerHarness) processing.Staked {
	return processing.Staked{
		After: processing.Position{
			Partition: harness.Partition,
		},
	}
}

func assertRefused(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Errorf("%s = %v, want %v", what, err, want)
	}
}

func assertStaked(t *testing.T, harness *ResumerHarness, want processing.Position) {
	t.Helper()
	staked, err := harness.Store.Stake(t.Context())
	if err != nil {
		t.Fatalf("Stake: %v", err)
	}
	if diff := cmp.Diff(want, staked.After); diff != "" {
		t.Errorf("staked after (-want +got):\n%s", diff)
	}
}

func checkpointEnd(harness *ResumerHarness) processing.Position {
	return processing.Position{
		Partition: harness.Partition,
		Sequence:  contractSequence,
	}
}

func drainedIsStaked(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	drainReader(t, reader)
	closeReader(t, reader)

	assertStaked(t, harness, checkpointEnd(harness))
}

func stakeBeforeClose(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	drainReader(t, reader)

	assertStaked(t, harness, checkpointEnd(harness))
	closeReader(t, reader)
}

func unreadIsRefused(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	closeReader(t, latestReader(t, harness))

	_, err := harness.Store.Stake(t.Context())

	assertRefused(t, "Stake of an unread checkpoint", err, processing.ErrCheckpointRefused)
}

func lateReadKeepsEmpty(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	first := latestReader(t, harness)
	step(t, "Unpoint", harness.Unpoint)
	if _, _, err := harness.Store.Latest(t.Context(), harness.Partition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("second Latest = %v, want %v", err, processing.ErrNoCheckpoint)
	}
	drainReader(t, first)
	closeReader(t, first)

	assertStaked(t, harness, emptyStake(harness).After)
}

func earlyCloseKeepsDiscard(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	if _, err := io.ReadFull(reader, make([]byte, partialBytes)); err != nil {
		t.Fatalf("read the first byte: %v", err)
	}
	closeReader(t, reader)

	_, stakeErr := harness.Store.Stake(t.Context())
	staked, discardErr := harness.Store.Discard(t.Context(), target)

	assertRefused(t, "Stake after an early close", stakeErr, processing.ErrCheckpointRefused)
	if discardErr != nil {
		t.Fatalf("Discard after an early close: %v", discardErr)
	}
	if diff := cmp.Diff(emptyStake(harness), staked); diff != "" {
		t.Errorf("discarded (-want +got):\n%s", diff)
	}
}

func movedReadRefusesDiscard(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	step(t, "MovePointer", harness.MovePointer)

	_, readErr := io.Copy(io.Discard, reader)
	closeReader(t, reader)
	_, discardErr := harness.Store.Discard(t.Context(), target)

	assertRefused(t, "read of a checkpoint the pointer left", readErr, processing.ErrCheckpointMoved)
	assertRefused(t, "Discard after a moved read", discardErr, processing.ErrCheckpointRefused)
}

func lateCloseKeepsNext(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	first := latestReader(t, harness)
	second := latestReader(t, harness)
	drainReader(t, second)
	closeReader(t, second)

	closeReader(t, first)

	assertStaked(t, harness, checkpointEnd(harness))
}

func stakeWithoutLatest(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)

	_, err := harness.Store.Stake(t.Context())

	assertRefused(t, "Stake without Latest", err, processing.ErrCheckpointRefused)
}

func corrupted(t *testing.T, harness *ResumerHarness) processing.DiscardTarget {
	t.Helper()
	withCheckpoint(t, harness)
	target, err := harness.Corrupt()
	if err != nil {
		t.Fatalf("Corrupt: %v", err)
	}
	if _, _, err := harness.Store.Latest(t.Context(), harness.Partition); !errors.Is(err,
		processing.ErrCheckpointUnusable) {
		t.Fatalf("Latest of a corrupt checkpoint = %v, want %v", err, processing.ErrCheckpointUnusable)
	}
	return target
}

func stakeAfterRefusedLatest(t *testing.T, harness *ResumerHarness) {
	corrupted(t, harness)

	_, err := harness.Store.Stake(t.Context())

	assertRefused(t, "Stake after a refused Latest", err, processing.ErrCheckpointRefused)
}

func refusedLatestDiscarded(t *testing.T, harness *ResumerHarness) {
	target := corrupted(t, harness)
	elsewhere := target
	elsewhere.FirstSeq++

	_, wrongStart := harness.Store.Discard(t.Context(), elsewhere)
	staked, err := harness.Store.Discard(t.Context(), target)

	assertRefused(t, "Discard naming another incident start", wrongStart, processing.ErrDiscardRefused)
	if err != nil {
		t.Fatalf("Discard at the incident start: %v", err)
	}
	if diff := cmp.Diff(emptyStake(harness), staked); diff != "" {
		t.Errorf("discarded (-want +got):\n%s", diff)
	}
}

func brokenImageDiscarded(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	step(t, "BreakImage", harness.BreakImage)

	_, readErr := io.Copy(io.Discard, reader)
	closeReader(t, reader)
	_, stakeErr := harness.Store.Stake(t.Context())
	_, discardErr := harness.Store.Discard(t.Context(), target)

	assertRefused(t, "read of a broken image", readErr, processing.ErrCheckpointUnusable)
	assertRefused(t, "Stake of a broken image", stakeErr, processing.ErrCheckpointRefused)
	if discardErr != nil {
		t.Errorf("Discard of a broken image: %v", discardErr)
	}
}

func discardWithoutLatest(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)

	_, err := harness.Store.Discard(t.Context(), target)

	assertRefused(t, "Discard without Latest", err, processing.ErrCheckpointRefused)
}

func foreignTargetRefused(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)
	closeReader(t, latestReader(t, harness))
	target.Checkpoint += foreignSuffix

	_, err := harness.Store.Discard(t.Context(), target)

	assertRefused(t, "Discard of another checkpoint", err, processing.ErrDiscardRefused)
}

func stakeAfterRefusedDiscard(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	drainReader(t, reader)
	closeReader(t, reader)
	target.Checkpoint += foreignSuffix
	if _, err := harness.Store.Discard(t.Context(), target); !errors.Is(err, processing.ErrDiscardRefused) {
		t.Fatalf("Discard of another checkpoint = %v, want %v", err, processing.ErrDiscardRefused)
	}

	assertStaked(t, harness, checkpointEnd(harness))
}

func openUnreadIsRefused(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	reader := latestReader(t, harness)

	_, err := harness.Store.Stake(t.Context())
	closeReader(t, reader)

	assertRefused(t, "Stake with an open unread reader", err, processing.ErrCheckpointRefused)
}

func refusedTarget(t *testing.T, harness *ResumerHarness) processing.DiscardTarget {
	t.Helper()
	target, refused := harness.Store.Refused(t.Context())
	if !refused {
		t.Fatal("Refused names no target after a refusal that may be discarded")
	}
	return target
}

func assertDiscards(t *testing.T, harness *ResumerHarness, target processing.DiscardTarget) {
	t.Helper()
	staked, err := harness.Store.Discard(t.Context(), target)
	if err != nil {
		t.Fatalf("Discard of the target Refused named: %v", err)
	}
	if diff := cmp.Diff(emptyStake(harness), staked); diff != "" {
		t.Errorf("discarded (-want +got):\n%s", diff)
	}
}

func earlyCloseTargetAccepted(t *testing.T, harness *ResumerHarness) {
	want := withCheckpoint(t, harness)
	reader := latestReader(t, harness)
	if _, err := io.ReadFull(reader, make([]byte, partialBytes)); err != nil {
		t.Fatalf("read the first byte: %v", err)
	}
	closeReader(t, reader)

	target := refusedTarget(t, harness)

	if diff := cmp.Diff(want, target); diff != "" {
		t.Errorf("refused target (-want +got):\n%s", diff)
	}
	assertDiscards(t, harness, target)
}

func refusedLatestTargetAccepted(t *testing.T, harness *ResumerHarness) {
	want := corrupted(t, harness)

	target := refusedTarget(t, harness)

	if diff := cmp.Diff(want, target); diff != "" {
		t.Errorf("refused target (-want +got):\n%s", diff)
	}
	assertDiscards(t, harness, target)
}

func noTargetWithoutRefusal(t *testing.T, harness *ResumerHarness) {
	withCheckpoint(t, harness)
	step(t, "Unpoint", harness.Unpoint)
	if _, _, err := harness.Store.Latest(t.Context(), harness.Partition); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest after Unpoint = %v, want %v", err, processing.ErrNoCheckpoint)
	}

	target, refused := harness.Store.Refused(t.Context())

	if refused {
		t.Errorf("Refused named %+v without a checkpoint", target)
	}
}

func openUnreadDiscardRefused(t *testing.T, harness *ResumerHarness) {
	target := withCheckpoint(t, harness)
	reader := latestReader(t, harness)

	_, err := harness.Store.Discard(t.Context(), target)
	closeReader(t, reader)

	assertRefused(t, "Discard while the reader is open and unread", err, processing.ErrCheckpointRefused)
}
