package feed

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	recordBytes     = 1 << 20
	storedRecords   = lastStreamRecord
	recordsPerPurge = maxPurgeBytes / recordBytes
	parallelCommits = 8
)

func stored() identity {
	return identity{Created: streamCreated(), First: firstStreamRecord, Last: lastStreamRecord,
		Bytes: storedRecords * 64, Msgs: storedRecords}
}

func through(sequence uint64) processing.Position {
	return processing.Position{Sequence: sequence}
}

func TestCommitPurgesExactlyThroughThePosition(t *testing.T) {
	stream := answering(answer{found: stored()})
	h := open(t, &Start{}, testOptions(), stream)

	err := h.feed.Commit(t.Context(), through(5))

	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if diff := cmp.Diff([]uint64{6}, stream.purged()); diff != "" {
		t.Errorf("purge bounds mismatch (-want +got):\n%s", diff)
	}
	if got := h.feed.Stats().PurgedThrough; got != 5 {
		t.Errorf("purged through %d, want 5", got)
	}
}

func TestCommitRefusesWhatItMustNotPurge(t *testing.T) {
	tests := []struct {
		name      string
		answer    answer
		prepare   func(h *harness)
		position  processing.Position
		wantErr   error
		wantInfos int
	}{
		{name: "a closed feed", prepare: func(h *harness) { h.feed.Close() }, position: through(5),
			wantErr: processing.ErrFeedClosed},
		{name: "a suspended lease", prepare: func(h *harness) { h.lease.suspended.Store(true) }, position: through(5),
			wantErr: ErrLeaseSuspended},
		{name: "a position of another partition", position: processing.Position{Partition: 1, Sequence: 5},
			wantErr: ErrForeignPosition},
		{name: "a recreated stream", answer: answer{found: identity{Created: recreated(), Last: lastStreamRecord}},
			position: through(5), wantErr: ErrStreamIdentity, wantInfos: 1},
		{name: "a deleted stream", answer: answer{err: jetstream.ErrStreamNotFound}, position: through(5),
			wantErr: ErrStreamIdentity, wantInfos: 1},
		{name: "a position past the stream's end", answer: answer{found: stored()}, position: through(lastStreamRecord + 1),
			wantErr: ErrPositionRange, wantInfos: 1},
		{name: "a feed closed while the stream answered", answer: answer{found: stored()},
			prepare: func(h *harness) { h.stream.onInfo = h.feed.Close }, position: through(5),
			wantErr: processing.ErrFeedClosed, wantInfos: 1},
		{name: "a lease suspended while the stream answered", answer: answer{found: stored()},
			prepare:  func(h *harness) { h.stream.onInfo = func() { h.lease.suspended.Store(true) } },
			position: through(5), wantErr: ErrLeaseSuspended, wantInfos: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := open(t, &Start{}, testOptions(), answering(tt.answer))
			if tt.prepare != nil {
				tt.prepare(h)
			}

			err := h.feed.Commit(t.Context(), tt.position)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Commit = %v, want %v", err, tt.wantErr)
			}
			if purges, infos := h.stream.purged(), h.stream.described(); len(purges) != 0 || infos != tt.wantInfos {
				t.Errorf("%d stream infos and purges %v, want %d infos and no purge", infos, purges, tt.wantInfos)
			}
		})
	}
}

func TestCommitAtOrBelowWhatIsPurgedAsksNothing(t *testing.T) {
	stream := answering(answer{found: stored()})
	h := open(t, &Start{}, testOptions(), stream)
	if err := h.feed.Commit(t.Context(), through(5)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for _, sequence := range []uint64{5, 3, 0} {
		if err := h.feed.Commit(t.Context(), through(sequence)); err != nil {
			t.Errorf("Commit(%d) = %v, want nil", sequence, err)
		}
	}

	if purges, infos := stream.purged(), stream.described(); len(purges) != 1 || infos != 1 {
		t.Errorf("%d infos and purges %v, want only the first Commit to reach the stream", infos, purges)
	}
}

func TestCommitBelowTheFirstRecordAtOpenAsksNothing(t *testing.T) {
	stream := answering(answer{found: stored()})
	stream.atOpen = identity{Created: streamCreated(), First: 500, Last: lastStreamRecord}
	h := open(t, &Start{}, testOptions(), stream)

	err := h.feed.Commit(t.Context(), through(499))

	if purges, infos := stream.purged(), stream.described(); err != nil || len(purges) != 0 || infos != 0 {
		t.Errorf("Commit = %v after %d infos and purges %v; want nil with no request", err, infos, purges)
	}
}

func TestPurgeIsChunked(t *testing.T) {
	stream := answering()
	stream.live = &identity{Created: streamCreated(), First: firstStreamRecord, Last: lastStreamRecord,
		Bytes: storedRecords * recordBytes, Msgs: storedRecords}
	h := open(t, &Start{}, testOptions(), stream)
	var progress []uint64

	for range 4 {
		if err := h.feed.Commit(t.Context(), through(lastStreamRecord)); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		progress = append(progress, h.feed.Stats().PurgedThrough)
	}

	want := []uint64{recordsPerPurge, 2 * recordsPerPurge, 3 * recordsPerPurge, lastStreamRecord}
	if diff := cmp.Diff(want, progress); diff != "" {
		t.Errorf("purged through after each Commit, about 256 MiB a step (-want +got):\n%s", diff)
	}
	if left, _ := stream.current(); left.Msgs != 0 {
		t.Errorf("%d records left after the steps, want none", left.Msgs)
	}
}

func TestAPurgeStepStartsAtTheStreamsFirstRecord(t *testing.T) {
	truncated := identity{Created: streamCreated(), First: 600, Last: lastStreamRecord,
		Bytes: (lastStreamRecord - 599) * recordBytes, Msgs: lastStreamRecord - 599}
	stream := answering(answer{found: truncated})
	h := open(t, &Start{}, testOptions(), stream)

	if err := h.feed.Commit(t.Context(), through(lastStreamRecord)); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if diff := cmp.Diff([]uint64{599 + recordsPerPurge + 1}, stream.purged()); diff != "" {
		t.Errorf("purge bounds mismatch, one step from the first record (-want +got):\n%s", diff)
	}
}

func TestAFailedPurgeKeepsWhatWasPurgedAndCounts(t *testing.T) {
	stream := answering(answer{found: stored()})
	stream.purgeErr = jetstream.ErrNoStreamResponse
	h := open(t, &Start{}, testOptions(), stream)

	err := h.feed.Commit(t.Context(), through(5))

	if !errors.Is(err, jetstream.ErrNoStreamResponse) {
		t.Fatalf("Commit = %v, want the purge's error", err)
	}
	stats := h.feed.Stats()
	if diff := cmp.Diff([]uint64{0, 1}, []uint64{stats.PurgedThrough, stats.PurgeFailures}); diff != "" {
		t.Errorf("purged through and failures mismatch (-want +got):\n%s", diff)
	}
}

func TestParallelCommitsKeepTheHighestPurge(t *testing.T) {
	stream := answering(answer{found: stored()})
	h := open(t, &Start{}, testOptions(), stream)
	var commits sync.WaitGroup

	for index := range uint64(parallelCommits) {
		commits.Go(func() {
			if err := h.feed.Commit(t.Context(), through(index+1)); err != nil {
				t.Errorf("Commit(%d): %v", index+1, err)
			}
		})
	}
	commits.Wait()

	if got := h.feed.Stats().PurgedThrough; got != parallelCommits {
		t.Errorf("purged through %d, want the highest %d", got, parallelCommits)
	}
}
