package stream_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

func newBenchWindows(b *testing.B, series int, early bool) (*stream.Windows[int, countCell], *stream.Watermark) {
	b.Helper()
	watermark, err := stream.NewWatermark(config)
	if err != nil {
		b.Fatal(err)
	}
	return newCountingWindows(b, series, early), watermark
}

func BenchmarkLocateRecordRandomSeries(b *testing.B) {
	const series = 100_000
	windows, watermark := newBenchWindows(b, series, true)
	now := at(time.Hour)
	windows.Begin(watermark.Advance(now, stream.Mark{Time: now, Known: true}), now)
	keys := rand.New(rand.NewPCG(1, 2)).Perm(series)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		c, _, _ := windows.Locate(keys[i%series], now)
		c.requests++
		i++
	}
	windows.End()
}

func BenchmarkWindowClose160k(b *testing.B) {
	const series = 160_000
	for b.Loop() {
		b.StopTimer()
		windows, watermark := newBenchWindows(b, series, false)
		now := at(time.Hour)
		windows.Begin(watermark.Advance(now, stream.Mark{Time: now, Known: true}), now)
		for key := range series {
			windows.Locate(key, now)
		}
		windows.End()
		later := now.Add(delay + lateness + 2*windowSize)
		b.StartTimer()
		windows.Begin(watermark.Advance(later, stream.Mark{Time: later, Known: true}), later)
		windows.End()
	}
}

func BenchmarkFireEarly60k(b *testing.B) {
	const series = 60_000
	windows, watermark := newBenchWindows(b, series, true)
	now := at(time.Hour)
	for b.Loop() {
		b.StopTimer()
		windows.Begin(watermark.Advance(now, stream.Mark{Time: now, Known: true}), now)
		for key := range series {
			windows.Locate(key, now)
		}
		windows.End()
		b.StartTimer()
		windows.FireEarly()
	}
}
