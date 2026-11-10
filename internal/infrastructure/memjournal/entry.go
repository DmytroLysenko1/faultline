package memjournal

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

type Stats struct {
	Bytes   int64
	Batches int
	Spans   int
}

func (s Stats) with(added *entry) Stats {
	if added.batch.Heartbeat {
		return s
	}
	return Stats{
		Bytes:   s.Bytes + added.size,
		Batches: s.Batches + 1,
		Spans:   s.Spans + len(added.batch.Spans),
	}
}

func (s Stats) without(removed *entry) Stats {
	if removed.batch.Heartbeat {
		return s
	}
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

func heartbeatAt(receivedAt time.Time) entry {
	return entry{
		batch: batch.Heartbeat(receivedAt),
	}
}
