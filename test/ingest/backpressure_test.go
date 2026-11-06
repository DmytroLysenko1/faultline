package ingest_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

func TestExhaustedCreditIsRetryableAndChangesNothing(t *testing.T) {
	first := requestOf(smallRequestSpans, 0)
	p := startPipeline(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, first),
		MaxBatches: roomyBatches,
	})
	if _, err := p.Client.Export(t.Context(), first); err != nil {
		t.Fatalf("first Export: %v", err)
	}
	held := p.Journal.Stats()
	_, err := p.Client.Export(t.Context(), requestOf(smallRequestSpans, smallRequestSpans))
	want := rejection{
		Code:      codes.ResourceExhausted,
		RetryInfo: true,
	}
	if diff := cmp.Diff(want, rejectionOf(err)); diff != "" {
		t.Errorf("rejection (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(held, p.Journal.Stats()); diff != "" {
		t.Errorf("a rejected request changed the journal (-want +got):\n%s", diff)
	}
}

func TestRequestLargerThanTheRemainingCreditIsRejectedWhole(t *testing.T) {
	small := requestOf(smallRequestSpans, 0)
	large := requestOf(largeRequestSpans, smallRequestSpans)
	p := startPipeline(t, memjournal.Limits{
		MaxBytes:   footprintOf(t, small) + footprintOf(t, large) - 1,
		MaxBatches: roomyBatches,
	})
	if _, err := p.Client.Export(t.Context(), small); err != nil {
		t.Fatalf("small Export: %v", err)
	}
	_, err := p.Client.Export(t.Context(), large)
	if diff := cmp.Diff(codes.ResourceExhausted, rejectionOf(err).Code); diff != "" {
		t.Errorf("code (-want +got):\n%s", diff)
	}
	if got := p.Journal.Stats().Spans; got != smallRequestSpans {
		t.Errorf("journal holds %d spans, want only the %d of the accepted request", got, smallRequestSpans)
	}
}

func TestRequestLargerThanTheJournalIsPermanent(t *testing.T) {
	p := startPipeline(t, memjournal.Limits{
		MaxBytes:   tinyJournalBytes,
		MaxBatches: roomyBatches,
	})
	_, err := p.Client.Export(t.Context(), requestOf(largeRequestSpans, 0))
	want := rejection{
		Code: codes.InvalidArgument,
	}
	if diff := cmp.Diff(want, rejectionOf(err)); diff != "" {
		t.Errorf("rejection (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(memjournal.Stats{}, p.Journal.Stats()); diff != "" {
		t.Errorf("journal after a permanent rejection (-want +got):\n%s", diff)
	}
}

func TestClosedJournalAnswersUnavailable(t *testing.T) {
	p := startPipeline(t, memjournal.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
	})
	p.Journal.Close()
	_, err := p.Client.Export(t.Context(), requestOf(smallRequestSpans, 0))
	want := rejection{
		Code: codes.Unavailable,
	}
	if diff := cmp.Diff(want, rejectionOf(err)); diff != "" {
		t.Errorf("rejection (-want +got):\n%s", diff)
	}
}

func TestLargestRequestFitsTheJournalLowerBound(t *testing.T) {
	p := startPipeline(t, memjournal.Limits{
		MaxBytes:   journalLowerBound,
		MaxBatches: journalBatchesUsed,
	})
	traces := otlptest.MinimalSpans(t)
	if _, err := p.Client.Export(t.Context(), ptraceotlpRequest(traces)); err != nil {
		t.Fatalf("largest request of minimal spans rejected by a %d-byte journal: %v", journalLowerBound, err)
	}
	stats := p.Journal.Stats()
	if stats.Spans != traces.SpanCount() || stats.Bytes > journalLowerBound {
		t.Errorf("journal holds %d spans in %d bytes, want %d spans within %d bytes",
			stats.Spans, stats.Bytes, traces.SpanCount(), journalLowerBound)
	}
}

func TestQueuedBatchDoesNotPinTheRequest(t *testing.T) {
	p := startPipeline(t, memjournal.Limits{
		MaxBytes:   roomyBytes,
		MaxBatches: roomyBatches,
	})
	request := ptraceotlpRequest(otlptest.DemoSpans(t))
	before := otlptest.HeapInUse()
	if _, err := p.Client.Export(t.Context(), request); err != nil {
		t.Fatalf("Export: %v", err)
	}
	retained := otlptest.HeapInUse() - before
	if limit := p.Journal.Stats().Bytes + retentionSlack; retained > limit {
		t.Errorf("after Export the heap grew by %d bytes, want at most the batch footprint plus slack: %d",
			retained, limit)
	}
}
