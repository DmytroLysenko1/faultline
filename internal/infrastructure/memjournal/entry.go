package memjournal

import "github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"

type Stats struct {
	Bytes   int64
	Batches int
	Spans   int
}

func (s Stats) with(added entry) Stats {
	return Stats{
		Bytes:   s.Bytes + added.size,
		Batches: s.Batches + 1,
		Spans:   s.Spans + len(added.batch.Spans),
	}
}

func (s Stats) without(removed entry) Stats {
	return Stats{
		Bytes:   s.Bytes - removed.size,
		Batches: s.Batches - 1,
		Spans:   s.Spans - len(removed.batch.Spans),
	}
}

type entry struct {
	batch batch.Batch
	size  int64
}

func newEntry(received batch.Batch) entry {
	return entry{
		batch: received,
		size:  received.Footprint(),
	}
}
