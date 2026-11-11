package processingtest

import (
	"context"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

const (
	testFirePeriod   = time.Second
	testMaxHeldBytes = 64 << 20
)

type Feed interface {
	Receive(ctx context.Context, limit int, maxWait time.Duration) ([]processing.Delivery, error)
}

type Publisher interface {
	Publish(ctx context.Context, snapshot *topology.Snapshot) error
}

type Clock interface {
	Now() time.Time
}

type StoppedClock struct{}

func (StoppedClock) Now() time.Time {
	return telemetrytest.Epoch()
}

type SteppingClock struct {
	At   time.Time
	Step time.Duration
}

func (c *SteppingClock) Now() time.Time {
	return c.At
}

func (c *SteppingClock) Advance() {
	c.At = c.At.Add(c.Step)
}

func Cadence() processing.Cadence {
	return processing.Cadence{
		FirePeriod:   testFirePeriod,
		MaxHeldBytes: testMaxHeldBytes,
	}
}

func NewPipeline(tb testing.TB, feed Feed, builder *topology.Builder, publisher Publisher, clock Clock) *processing.Pipeline {
	tb.Helper()
	pipeline, err := processing.NewPipeline(feed, builder, publisher, clock, Cadence())
	if err != nil {
		tb.Fatalf("NewPipeline: %v", err)
	}
	return pipeline
}
