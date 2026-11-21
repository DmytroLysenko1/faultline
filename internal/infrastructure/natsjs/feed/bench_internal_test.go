package feed

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	benchSpanBytes = 30
	benchFootprint = 16 << 20
	benchCredit    = 64 << 20
	smallRecord    = 64 << 10
	largeRecord    = 1 << 20
)

type endlessReader struct {
	data     []byte
	sequence atomic.Uint64
	stopped  chan struct{}
}

func (r *endlessReader) Next(time.Duration) (message, error) {
	select {
	case <-r.stopped:
		return message{}, jetstream.ErrMsgIteratorClosed
	default:
	}
	return message{data: r.data, sequence: r.sequence.Add(1)}, nil
}

func (r *endlessReader) Consumer() string {
	return firstConsumer
}

func (r *endlessReader) Stop() {
	close(r.stopped)
}

func BenchmarkDelivery(b *testing.B) {
	for _, size := range []int{smallRecord, largeRecord} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			data := encodeBench(b, size)
			reader := &endlessReader{data: data, stopped: make(chan struct{})}
			options := testOptions()
			options.MaxFootprint, options.MaxBytes = benchFootprint, benchCredit
			found := &source{reader: reader, stream: answering(), letters: newFakeLetters()}
			feed := newFeed(b.Context(), newFakeLease(), found, &Start{},
				options)
			var before, after runtime.MemStats
			records := 0
			runtime.ReadMemStats(&before)
			for b.Loop() {
				records += receiveBench(b, feed)
			}
			runtime.ReadMemStats(&after)
			b.SetBytes(int64(len(data) * records / max(b.N, 1)))
			b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(records), "allocs/record")
			feed.Close()
			if err := feed.Wait(context.WithoutCancel(b.Context())); err != nil {
				b.Fatalf("Wait: %v", err)
			}
		})
	}
}

func receiveBench(b *testing.B, feed *Feed) int {
	deliveries, err := feed.Receive(b.Context(), receiveLimit, longWait)
	if err != nil || len(deliveries) == 0 {
		b.Fatalf("Receive = %d, %v", len(deliveries), err)
	}
	for _, delivery := range deliveries {
		delivery.Release()
	}
	return len(deliveries)
}

func encodeBench(b *testing.B, size int) []byte {
	b.Helper()
	base := spansOf(b, 0)
	spans := make([]span.Span, 0, size/benchSpanBytes)
	for len(spans) < cap(spans) {
		spans = append(spans, base...)
	}
	var data []byte
	err := new(record.Encoder).Encode(testTenant, 0, spans, at(0), 0, func(made []byte, _ string) error {
		data = slices.Clone(made)
		return nil
	})
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	return data
}
