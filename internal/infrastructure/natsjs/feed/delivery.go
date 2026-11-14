package feed

import (
	"sync/atomic"

	"golang.org/x/sync/semaphore"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

type delivery struct {
	batch    batch.Batch
	bytes    int64
	position processing.Position
	credit   *semaphore.Weighted
	counters *counters
	released atomic.Bool
}

func (d *delivery) Batch() batch.Batch {
	return d.batch
}

func (d *delivery) Bytes() int64 {
	return d.bytes
}

func (d *delivery) Position() processing.Position {
	return d.position
}

func (d *delivery) Release() {
	if !d.released.CompareAndSwap(false, true) {
		return
	}
	d.credit.Release(d.bytes)
	d.counters.released.Add(1)
}
