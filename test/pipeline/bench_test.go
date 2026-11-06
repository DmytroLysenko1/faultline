package pipeline_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memgraph"
)

const batchesPerSecond = "batches/s"

func iterationAfterPrefill(b *testing.B, first uint64) []processing.Delivery {
	b.Helper()
	iteration := make([]processing.Delivery, 0, processing.MaxBatchesPerIteration)
	for _, created := range processingtest.SinglePairsAfterPrefill(b, first, processing.MaxBatchesPerIteration) {
		iteration = append(iteration, processingtest.NewDelivery(created))
	}
	return iteration
}

func BenchmarkIteration50kEdges(b *testing.B) {
	builder := processingtest.PrefilledBuilder(b)
	store, err := memgraph.New(builder.Snapshot())
	if err != nil {
		b.Fatalf("memgraph.New: %v", err)
	}
	var batches uint64
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		feed := processingtest.NewIterationFeed(iterationAfterPrefill(b, batches))
		b.StartTimer()
		if err := processing.NewPipeline(feed, builder, store).Run(b.Context()); err != nil {
			b.Fatalf("Run: %v", err)
		}
		batches += processing.MaxBatchesPerIteration
	}
	b.ReportMetric(float64(batches)/b.Elapsed().Seconds(), batchesPerSecond)
}
