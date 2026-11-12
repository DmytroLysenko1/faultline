package record_test

import (
	"hash/fnv"
	"math/rand/v2"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

func TestPartitionOfIsFNV1aOfTheTraceIDModP(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 10_000 {
		var trace ids.TraceID
		for i := range trace {
			trace[i] = byte(rng.Uint32())
		}
		partitions := 1 + rng.IntN(1024)
		hasher := fnv.New64a()
		hasher.Write(trace[:])
		want := uint16(hasher.Sum64() % uint64(partitions))

		got := record.PartitionOf(trace, partitions)

		if got != want {
			t.Fatalf("PartitionOf(%x, %d) = %d, want %d", trace, partitions, got, want)
		}
	}
}

func TestPartitionOfWithOnePartitionOrLessIsZero(t *testing.T) {
	for _, partitions := range []int{-1, 0, 1} {
		if got := record.PartitionOf(traceOf(42), partitions); got != 0 {
			t.Errorf("PartitionOf(_, %d) = %d, want 0", partitions, got)
		}
	}
}

// TestPartitionsShareTheLoadEvenly: random trace ids land within 5 % of the mean on every one of 8 partitions.
func TestPartitionsShareTheLoadEvenly(t *testing.T) {
	const partitions, traces = 8, 160_000
	rng := rand.New(rand.NewPCG(3, 4))
	counts := make([]int, partitions)
	for range traces {
		var trace ids.TraceID
		for i := range trace {
			trace[i] = byte(rng.Uint32())
		}
		counts[record.PartitionOf(trace, partitions)]++
	}
	mean := traces / partitions
	for partition, count := range counts {
		if count < mean*95/100 || count > mean*105/100 {
			t.Errorf("partition %d got %d of %d traces, want within 5 %% of %d", partition, count, traces, mean)
		}
	}
}
