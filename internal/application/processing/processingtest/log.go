package processingtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

var (
	ErrRetentionPassed = errors.New("records before the position were already dropped")
	ErrBeyondTheLog    = errors.New("position is past the end of the log")
)

// RetainedLog is a partition of a journal that keeps every record until retention drops it, so a
// restarted processor can read it again from any retained position.
type RetainedLog struct {
	partition     uint16
	batches       []batch.Batch
	mu            sync.Mutex
	firstRetained uint64
}

func NewRetainedLog(partition uint16, batches []batch.Batch) *RetainedLog {
	return &RetainedLog{partition: partition, batches: batches, firstRetained: 1}
}

func (l *RetainedLog) Commit(_ context.Context, through processing.Position) error {
	if through.Partition != l.partition || through.Sequence > uint64(len(l.batches)) {
		return fmt.Errorf("commit through %d of partition %d: %w", through.Sequence, through.Partition, ErrBeyondTheLog)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.firstRetained = max(l.firstRetained, through.Sequence+1)
	return nil
}

func (l *RetainedLog) FirstRetained() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstRetained
}

func (l *RetainedLog) Open(from processing.Position) (*LogReader, error) {
	if first := l.FirstRetained(); from.Partition != l.partition || from.Sequence < first {
		return nil, fmt.Errorf("open partition %d at %d, first retained %d: %w", from.Partition, from.Sequence, first,
			ErrRetentionPassed)
	}
	return &LogReader{log: l, next: from.Sequence}, nil
}

// LogReader feeds a pipeline from its position. PerReceive caps a Receive below the pipeline's
// limit, Clock advances once per Receive, and Kill — called at the KillAfter-th Receive — cancels the
// pipeline's context the way SIGKILL ends a processor: no drain, no final checkpoint.
type LogReader struct {
	log        *RetainedLog
	next       uint64
	PerReceive int
	Clock      *SteppingClock
	KillAfter  int
	Kill       context.CancelFunc
	receives   int
	handedOut  []*Delivery
}

func (r *LogReader) Receive(ctx context.Context, limit int, _ time.Duration) ([]processing.Delivery, error) {
	r.step()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("receive: %w", err)
	}
	if r.next > uint64(len(r.log.batches)) {
		return nil, processing.ErrFeedClosed
	}
	return r.take(limit), nil
}

func (r *LogReader) step() {
	r.receives++
	if r.KillAfter > 0 && r.receives >= r.KillAfter {
		r.Kill()
	}
	if r.Clock != nil {
		r.Clock.Advance()
	}
}

func (r *LogReader) take(limit int) []processing.Delivery {
	if r.PerReceive > 0 {
		limit = min(limit, r.PerReceive)
	}
	var deliveries []processing.Delivery
	for ; len(deliveries) < limit && r.next <= uint64(len(r.log.batches)); r.next++ {
		position := processing.Position{Partition: r.log.partition, Sequence: r.next}
		delivered := NewDeliveryAt(r.log.batches[r.next-1], position)
		r.handedOut = append(r.handedOut, delivered)
		deliveries = append(deliveries, delivered)
	}
	return deliveries
}

func (r *LogReader) HandedOut() []*Delivery {
	return r.handedOut
}
