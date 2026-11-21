package journal_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	benchStreamBytes = 1 << 30
	eightPartitions  = 8
	smallRequest     = 512
	largeRequest     = 2048
)

func BenchmarkAppend(b *testing.B) {
	for _, partitions := range []int{onePartition, eightPartitions} {
		for _, spans := range []int{smallRequest, largeRequest} {
			b.Run(fmt.Sprintf("partitions=%d/spans=%d", partitions, spans), func(b *testing.B) {
				benchmarkAppend(b, partitions, spans)
			})
		}
	}
}

func benchmarkAppend(b *testing.B, partitions, count int) {
	topology := liveTopology(partitions)
	topology.MaxBytesPerPartition = benchStreamBytes
	live := startJournal(b, topology)
	request := spansAcross(count, 1)
	b.ReportAllocs()
	var iteration uint64
	for b.Loop() {
		iteration++
		renumber(request, iteration)
		if err := live.journal.Append(b.Context(), request); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
	b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "spans/s")
	if failed := live.journal.Stats().Failed; failed != [natsjs.ReasonCount]uint64{} {
		b.Errorf("failed publishes by reason: %v", failed)
	}
}

func renumber(request []span.Span, iteration uint64) {
	for index := range request {
		binary.BigEndian.PutUint64(request[index].SpanID[:], iteration<<32|uint64(index))
	}
}
