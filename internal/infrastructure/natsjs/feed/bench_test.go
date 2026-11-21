package feed_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	benchRecords     = 64
	benchFootprint   = 16 << 20
	benchCredit      = 64 << 20
	spanRecordBytes  = 30
	smallRecordBytes = 64 << 10
	largeRecordBytes = 1 << 20
)

func BenchmarkFeedReceive(b *testing.B) {
	for _, nodes := range []int{singleNode, threeNodes} {
		for _, size := range []int{smallRecordBytes, largeRecordBytes} {
			b.Run(fmt.Sprintf("R%d/%dKiB", nodes, size>>10), func(b *testing.B) {
				benchmarkReceive(b, nodes, size)
			})
		}
	}
}

func benchmarkReceive(b *testing.B, nodes, size int) {
	l := startLive(b, nodes, nodes)
	spans := spansOfSize(b, size)
	l.publishBench(b, spans)
	options := optionsOf(l.topology)
	options.MaxFootprint, options.MaxBytes = benchFootprint, benchCredit
	b.SetBytes(int64(benchRecords * encodedSize(b, spans)))
	b.ReportAllocs()
	for b.Loop() {
		readAll(b, l, options)
	}
}

func readAll(b *testing.B, l *live, options *feed.Options) {
	opened, err := feed.Open(b.Context(), l.conn, newHeldLease(), feed.Start{}, options)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	for read := 0; read < benchRecords; {
		deliveries, err := opened.Receive(b.Context(), receiveLimit, receiveWait)
		if err != nil {
			b.Fatalf("Receive after %d: %v", read, err)
		}
		read += len(released(deliveries))
	}
	shut(b, opened)
}

func spansOfSize(b *testing.B, size int) []span.Span {
	b.Helper()
	base := spansOf(b, 0)
	spans := make([]span.Span, 0, size/spanRecordBytes)
	for len(spans) < cap(spans) {
		spans = append(spans, base...)
	}
	return spans
}

func (l *live) publishBench(b *testing.B, spans []span.Span) {
	b.Helper()
	for index := range uint64(benchRecords) {
		spans[0] = spansOf(b, (index+1)*traceStride)[0]
		l.publish(b, spans)
	}
}

func encodedSize(b *testing.B, spans []span.Span) int {
	b.Helper()
	var size int
	err := new(record.Encoder).Encode(tenant, 0, spans, time.Now(), 0, func(data []byte, _ string) error {
		size = len(data)
		return nil
	})
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	return size
}
