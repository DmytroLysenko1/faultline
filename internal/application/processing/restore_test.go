package processing_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

var (
	errLatestBroken = errors.New("pointer unreadable")
	errReaderBroken = errors.New("object stream broken")
)

func TestRestoreWithoutACheckpointStartsEmptyAtTheFirstPosition(t *testing.T) {
	store := processingtest.NewCheckpointStore(t, nil)
	restored, err := processing.Restore(t.Context(), store, partition, new(processingtest.Limits()), telemetrytest.Epoch())

	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Found || !restored.ReceivedAt.IsZero() {
		t.Errorf("restored = %+v, want nothing found", restored)
	}
	if got, want := restored.Through.Next(), (processing.Position{Partition: partition, Sequence: 1}); got != want {
		t.Errorf("feed opens at %+v, want %+v", got, want)
	}
	snapshot := restored.Builder.Snapshot()
	if len(snapshot.Nodes) != 0 || !snapshot.GeneratedAt.Equal(telemetrytest.Epoch()) {
		t.Errorf("a fresh builder has %d nodes generated at %s", len(snapshot.Nodes), snapshot.GeneratedAt)
	}
}

// TestRestoreContinuesFromTheCommittedCheckpoint: the builder comes back with what the checkpoint
// held, and the feed continues right after it with the same ReceivedAt.
func TestRestoreContinuesFromTheCommittedCheckpoint(t *testing.T) {
	p, store, _ := stepping(t, tenBatches, everyThird)
	p.run(t)
	restored, err := processing.Restore(t.Context(), store, partition, new(processingtest.Limits()), telemetrytest.Epoch())

	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	committed := store.Committed()
	last := committed[len(committed)-1]
	want := processing.Restored{
		Builder: restored.Builder, Through: last.Position, ReceivedAt: last.ReceivedAt, Found: true,
	}
	if diff := gocmp.Diff(want, restored, gocmp.Comparer(sameBuilder)); diff != "" {
		t.Errorf("restored (-want +got):\n%s", diff)
	}
	var rewritten bytes.Buffer
	if err := restored.Builder.WriteCheckpoint(&rewritten); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	if !bytes.Equal(rewritten.Bytes(), store.LatestBytes(partition)) {
		t.Error("the restored builder is not the state that was checkpointed")
	}
	if got := processingtest.TotalCalls(restored.Builder.Snapshot()); got != tenBatches*callsPerBatch {
		t.Errorf("restored %d calls, want %d", got, tenBatches*callsPerBatch)
	}
	if got := store.OpenReaders(); got != 0 {
		t.Errorf("%d checkpoint readers left open", got)
	}
}

func sameBuilder(a, b *topology.Builder) bool {
	return a == b
}

func committedStore(t *testing.T) *processingtest.CheckpointStore {
	t.Helper()
	p, store, _ := stepping(t, tenBatches, everyThird)
	p.run(t)
	return store
}

func rewrite(t *testing.T, store *processingtest.CheckpointStore,
	change func(*processing.CheckpointMeta, []byte) []byte,
) {
	t.Helper()
	committed := store.Committed()
	meta := committed[len(committed)-1]
	data := change(&meta, store.LatestBytes(partition))
	store.Put(partition, &meta, data)
}

// TestAnIncompatibleCheckpointFailsClosed: a checkpoint the processor cannot continue from is an
// error, never an empty start; cmd turns it into exit code 1 unless the operator discards it.
func TestAnIncompatibleCheckpointFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		args    func(t *testing.T, store *processingtest.CheckpointStore, limits *topology.Limits)
		wantErr error
	}{
		{
			name: "a damaged checkpoint",
			args: func(t *testing.T, store *processingtest.CheckpointStore, _ *topology.Limits) {
				rewrite(t, store, func(_ *processing.CheckpointMeta, data []byte) []byte {
					data[len(data)/2] ^= 0xff
					return data
				})
			},
			wantErr: topology.ErrCheckpointCorrupt,
		},
		{
			name: "a cut checkpoint",
			args: func(t *testing.T, store *processingtest.CheckpointStore, _ *topology.Limits) {
				rewrite(t, store, func(_ *processing.CheckpointMeta, data []byte) []byte { return data[:len(data)/2] })
			},
			wantErr: topology.ErrCheckpointCorrupt,
		},
		{
			name: "fewer nodes than the checkpoint holds",
			args: func(_ *testing.T, _ *processingtest.CheckpointStore, limits *topology.Limits) {
				limits.MaxNodes = 1
			},
			wantErr: topology.ErrCheckpointIncompatible,
		},
		{
			name: "the checkpoint of another partition",
			args: func(t *testing.T, store *processingtest.CheckpointStore, _ *topology.Limits) {
				rewrite(t, store, func(meta *processing.CheckpointMeta, data []byte) []byte {
					meta.Position.Partition = partition + 1
					return data
				})
			},
			wantErr: processing.ErrForeignCheckpoint,
		},
		{
			name: "an unreadable pointer",
			args: func(_ *testing.T, store *processingtest.CheckpointStore, _ *topology.Limits) {
				store.FailLatest(errLatestBroken)
			},
			wantErr: errLatestBroken,
		},
		{
			name: "a stream that breaks on close",
			args: func(_ *testing.T, store *processingtest.CheckpointStore, _ *topology.Limits) {
				store.FailReaderClose(errReaderBroken)
			},
			wantErr: errReaderBroken,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := committedStore(t)
			limits := processingtest.Limits()
			tt.args(t, store, &limits)

			restored, err := processing.Restore(t.Context(), store, partition, &limits, telemetrytest.Epoch())

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Restore = %v, want %v", err, tt.wantErr)
			}
			if restored.Builder != nil || restored.Found {
				t.Errorf("a refused checkpoint still gave %+v", restored)
			}
			if got := store.OpenReaders(); got != 0 {
				t.Errorf("%d checkpoint readers left open", got)
			}
		})
	}
}

func TestRestoreRefusesInvalidLimitsWithoutACheckpoint(t *testing.T) {
	limits := processingtest.Limits()
	limits.MaxNodes = 0

	_, err := processing.Restore(t.Context(), processingtest.NewCheckpointStore(t, nil), partition, &limits,
		telemetrytest.Epoch())

	if !errors.Is(err, topology.ErrInvalidLimits) {
		t.Errorf("Restore = %v, want ErrInvalidLimits", err)
	}
}

// strayReader is a store answer the contract does not promise: data with an error, or no data
// without one.
type strayReader struct {
	reader io.ReadCloser
	err    error
}

func (s strayReader) Latest(context.Context, uint16) (processing.CheckpointMeta, io.ReadCloser, error) {
	return processing.CheckpointMeta{}, s.reader, s.err
}

type countingCloser struct {
	io.Reader
	closed int
}

func (c *countingCloser) Close() error {
	c.closed++
	return nil
}

func TestRestoreSurvivesAStoreThatBreaksItsContract(t *testing.T) {
	type want struct {
		found  bool
		closed int
		err    error
	}
	tests := []struct {
		name string
		args func(*countingCloser) strayReader
		want want
	}{
		{
			name: "no checkpoint, yet a reader",
			args: func(r *countingCloser) strayReader { return strayReader{reader: r, err: processing.ErrNoCheckpoint} },
			want: want{closed: 1},
		},
		{
			name: "an error, yet a reader",
			args: func(r *countingCloser) strayReader { return strayReader{reader: r, err: errLatestBroken} },
			want: want{closed: 1, err: errLatestBroken},
		},
		{
			name: "a checkpoint without a reader",
			args: func(*countingCloser) strayReader { return strayReader{} },
			want: want{err: topology.ErrCheckpointCorrupt},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &countingCloser{Reader: bytes.NewReader(nil)}
			restored, err := processing.Restore(t.Context(), tt.args(reader), partition, new(processingtest.Limits()),
				telemetrytest.Epoch())

			if !errors.Is(err, tt.want.err) {
				t.Fatalf("Restore = %v, want %v", err, tt.want.err)
			}
			if restored.Found != tt.want.found || reader.closed != tt.want.closed {
				t.Errorf("found %t, reader closed %d times; want %t and %d", restored.Found, reader.closed,
					tt.want.found, tt.want.closed)
			}
		})
	}
}
