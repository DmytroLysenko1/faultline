package processing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

var (
	ErrNoCheckpoint   = errors.New("no checkpoint for partition")
	ErrCheckpointBusy = errors.New("previous checkpoint is still uploading")
	errNoWriter       = errors.New("checkpoint store began no writer")
)

type CheckpointMeta struct {
	Position          Position
	ReceivedAt        time.Time
	StreamCreated     time.Time
	Epoch             uint64
	CheckpointVersion uint32
	RecordVersion     uint8
	LimitsHash        uint64
	RulesHash         uint32
}

type CheckpointWriter interface {
	io.Writer
	Close() error
	Abort() error
}

type checkpointSink interface {
	Begin(ctx context.Context, meta *CheckpointMeta) (CheckpointWriter, error)
}

type CheckpointStats struct {
	Spooled        uint64
	Skipped        uint64
	Failed         uint64
	SpooledThrough Position
	LastFailure    error
}

func (s CheckpointStats) after(through Position, err error) CheckpointStats {
	switch {
	case err == nil:
		s.Spooled++
		s.SpooledThrough = through
	case errors.Is(err, ErrCheckpointBusy):
		s.Skipped++
	default:
		s.Failed++
		s.LastFailure = err
	}
	return s
}

type checkpointCounters struct {
	mu    sync.Mutex
	stats CheckpointStats
}

func (c *checkpointCounters) record(through Position, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = c.stats.after(through, err)
}

func (c *checkpointCounters) read() CheckpointStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

type checkpointer struct {
	sink       checkpointSink
	interval   time.Duration
	next       time.Time
	through    Position
	receivedAt time.Time
	unwritten  bool
	counters   checkpointCounters
}

func (c *checkpointer) start(now time.Time) {
	c.next = now.Add(c.interval)
}

func (c *checkpointer) applied(delivery Delivery) {
	c.through, c.receivedAt, c.unwritten = delivery.Position(), delivery.Batch().ReceivedAt, true
}

func (c *checkpointer) due(now time.Time) bool {
	return c.interval > 0 && c.unwritten && reached(now, c.next, c.interval)
}

func (c *checkpointer) pendingAtStop() bool {
	return c.interval > 0 && c.unwritten
}

func (c *checkpointer) write(ctx context.Context, builder *topology.Builder, now time.Time) error {
	c.next = now.Add(c.interval)
	err := c.save(ctx, builder)
	if canceledByCaller(ctx, err) {
		return err
	}
	c.counters.record(c.through, err)
	if err == nil {
		c.unwritten = false
	}
	return err
}

func (c *checkpointer) save(ctx context.Context, builder *topology.Builder) error {
	writer, err := c.begin(ctx)
	if err != nil {
		return err
	}
	if err := builder.WriteCheckpoint(writer); err != nil {
		return errors.Join(fmt.Errorf("write checkpoint through %d: %w", c.through.Sequence, err), writer.Abort())
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close checkpoint through %d: %w", c.through.Sequence, err)
	}
	return nil
}

func (c *checkpointer) begin(ctx context.Context) (CheckpointWriter, error) {
	writer, err := c.sink.Begin(ctx, &CheckpointMeta{
		Position:   c.through,
		ReceivedAt: c.receivedAt,
	})
	if err := strayWriter(writer, err); err != nil {
		return nil, fmt.Errorf("begin checkpoint through %d: %w", c.through.Sequence, err)
	}
	return writer, nil
}

func strayWriter(writer CheckpointWriter, err error) error {
	switch {
	case err != nil && writer != nil:
		return errors.Join(err, writer.Abort())
	case err == nil && writer == nil:
		return errNoWriter
	}
	return err
}

func reached(now, deadline time.Time, period time.Duration) bool {
	return !now.Before(deadline) || deadline.Sub(now) > period
}
