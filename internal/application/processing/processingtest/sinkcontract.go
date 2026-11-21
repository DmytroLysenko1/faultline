package processingtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

type CheckpointSink interface {
	Begin(ctx context.Context, meta *processing.CheckpointMeta) (processing.CheckpointWriter, error)
	BeginOnStop(ctx context.Context, meta *processing.CheckpointMeta) (processing.CheckpointWriter, error)
}

type SinkHarness struct {
	Sink      CheckpointSink
	Partition uint16
	Hold      func() (release func())
	Settled   func(ctx context.Context) error
	Stop      func(ctx context.Context) error
	Latest    func(ctx context.Context) (processing.CheckpointMeta, io.ReadCloser, error)
	Retained  func() (processing.Position, bool)
}

type contractRow struct {
	name string
	run  func(t *testing.T, harness *SinkHarness)
}

func RunCheckpointSinkContract(t *testing.T, open func(t *testing.T) SinkHarness) {
	t.Helper()
	for _, row := range []contractRow{
		{name: "a closed checkpoint becomes the latest with its image", run: closedBecomesLatest},
		{name: "an aborted checkpoint leaves nothing and frees the slot", run: abortedLeavesNothing},
		{name: "one final is let in while another is in flight and moves no retention", run: finalPastBusy},
		{name: "every begin after the stop is refused", run: refusedAfterStop},
		{name: "a begin with a canceled context takes no slot and no final", run: canceledTakesNothing},
	} {
		t.Run(row.name, func(t *testing.T) {
			harness := open(t)
			row.run(t, &harness)
		})
	}
}

func closedBecomesLatest(t *testing.T, harness *SinkHarness) {
	if err := latestErr(harness.Latest(t.Context())); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest before any checkpoint = %v, want ErrNoCheckpoint", err)
	}
	image := spoolImage(t, harness, harness.Sink.Begin, firstPosition)
	settle(t, harness)
	assertLatest(t, harness, firstPosition, image)
	assertRetained(t, harness, firstPosition, true)
}

func abortedLeavesNothing(t *testing.T, harness *SinkHarness) {
	writer, err := harness.Sink.Begin(t.Context(), harness.metaAt(firstPosition))
	if err != nil {
		t.Fatalf("Begin = %v", err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatalf("Abort = %v", err)
	}
	settle(t, harness)
	if err := latestErr(harness.Latest(t.Context())); !errors.Is(err, processing.ErrNoCheckpoint) {
		t.Fatalf("Latest after an abort = %v, want ErrNoCheckpoint", err)
	}
	image := spoolImage(t, harness, harness.Sink.Begin, secondPosition)
	settle(t, harness)
	assertLatest(t, harness, secondPosition, image)
}

func finalPastBusy(t *testing.T, harness *SinkHarness) {
	release := harness.Hold()
	spoolImage(t, harness, harness.Sink.Begin, firstPosition)
	assertBusy(t, harness.Sink.Begin, harness.metaAt(secondPosition))
	image := spoolImage(t, harness, harness.Sink.BeginOnStop, secondPosition)
	assertBusy(t, harness.Sink.BeginOnStop, harness.metaAt(thirdPosition))
	assertBusy(t, harness.Sink.Begin, harness.metaAt(thirdPosition))
	release()
	settle(t, harness)
	assertLatest(t, harness, secondPosition, image)
	assertRetained(t, harness, firstPosition, true)
}

func refusedAfterStop(t *testing.T, harness *SinkHarness) {
	if err := harness.Stop(t.Context()); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	for name, begin := range map[string]func(context.Context, *processing.CheckpointMeta) (
		processing.CheckpointWriter, error){"Begin": harness.Sink.Begin, "BeginFinal": harness.Sink.BeginOnStop} {
		if _, err := begin(t.Context(), harness.metaAt(firstPosition)); !errors.Is(err, processing.ErrCheckpointRefused) {
			t.Errorf("%s after the stop = %v, want ErrCheckpointRefused", name, err)
		}
	}
}

func canceledTakesNothing(t *testing.T, harness *SinkHarness) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for name, begin := range map[string]func(context.Context, *processing.CheckpointMeta) (
		processing.CheckpointWriter, error){"Begin": harness.Sink.Begin, "BeginOnStop": harness.Sink.BeginOnStop} {
		writer, err := begin(canceled, harness.metaAt(firstPosition))
		if !errors.Is(err, context.Canceled) || writer != nil {
			t.Errorf("%s with a canceled context = %v and a writer %t, want the cancel and none", name, err,
				writer != nil)
		}
	}
	image := spoolImage(t, harness, harness.Sink.BeginOnStop, firstPosition)
	settle(t, harness)
	assertLatest(t, harness, firstPosition, image)
	assertRetained(t, harness, 0, false)
}

const (
	firstPosition  = 10
	secondPosition = 20
	thirdPosition  = 30
)

func (h *SinkHarness) metaAt(sequence uint64) *processing.CheckpointMeta {
	return &processing.CheckpointMeta{
		Position: processing.Position{
			Partition: h.Partition,
			Sequence:  sequence,
		},
	}
}

func spoolImage(
	t *testing.T,
	harness *SinkHarness,
	begin func(context.Context, *processing.CheckpointMeta) (processing.CheckpointWriter, error),
	sequence uint64,
) []byte {
	t.Helper()
	var image bytes.Buffer
	if err := NewBuilder(t).WriteCheckpoint(&image); err != nil {
		t.Fatalf("WriteCheckpoint = %v", err)
	}
	writer, err := begin(t.Context(), harness.metaAt(sequence))
	if err != nil {
		t.Fatalf("begin at %d = %v", sequence, err)
	}
	if _, err := writer.Write(image.Bytes()); err != nil {
		t.Fatalf("Write at %d = %v", sequence, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close at %d = %v", sequence, err)
	}
	return image.Bytes()
}

func assertBusy(
	t *testing.T,
	begin func(context.Context, *processing.CheckpointMeta) (processing.CheckpointWriter, error),
	meta *processing.CheckpointMeta,
) {
	t.Helper()
	writer, err := begin(t.Context(), meta)
	if !errors.Is(err, processing.ErrCheckpointBusy) {
		t.Errorf("begin at %d while full = %v, want ErrCheckpointBusy", meta.Position.Sequence, err)
	}
	if writer != nil {
		t.Errorf("begin at %d while full gave a writer", meta.Position.Sequence)
	}
}

func settle(t *testing.T, harness *SinkHarness) {
	t.Helper()
	if err := harness.Settled(t.Context()); err != nil {
		t.Fatalf("Settled = %v", err)
	}
}

func assertLatest(t *testing.T, harness *SinkHarness, sequence uint64, image []byte) {
	t.Helper()
	meta, reader, err := harness.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest = %v", err)
	}
	read, err := io.ReadAll(reader)
	if err = errors.Join(err, reader.Close()); err != nil {
		t.Fatalf("read the latest = %v", err)
	}
	if meta.Position.Sequence != sequence {
		t.Errorf("latest position = %d, want %d", meta.Position.Sequence, sequence)
	}
	if !bytes.Equal(read, image) {
		t.Errorf("latest image differs from the one written: %d bytes, want %d", len(read), len(image))
	}
}

func assertRetained(t *testing.T, harness *SinkHarness, sequence uint64, moved bool) {
	t.Helper()
	position, retained := harness.Retained()
	if diff := cmp.Diff(struct {
		Sequence uint64
		Moved    bool
	}{sequence, moved}, struct {
		Sequence uint64
		Moved    bool
	}{position.Sequence, retained}); diff != "" {
		t.Errorf("retention (-want +got):\n%s", diff)
	}
}

func latestErr(_ processing.CheckpointMeta, reader io.ReadCloser, err error) error {
	if reader == nil {
		return err
	}
	return errors.Join(err, reader.Close())
}
