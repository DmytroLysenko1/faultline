//go:build !race

package topology_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const (
	retainedPairs    = 2_000
	retainedNameSize = 256
	retainedPolls    = 50
	retainedPause    = 10 * time.Millisecond
)

// TestApplyDoesNotKeepAReleasedBatchAlive: once the windows of a batch have closed, nothing the
// builder reuses still points at the batch's operation names, so the journal's release frees them.
func TestApplyDoesNotKeepAReleasedBatchAlive(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	start := telemetrytest.Epoch()
	var freed atomic.Int64
	spans := make([]span.Span, 0, 2*retainedPairs)
	for trace := range uint64(retainedPairs) {
		name := fmt.Sprintf("%0*d", retainedNameSize, trace)
		runtime.AddCleanup(unsafe.StringData(name), func(*atomic.Int64) { freed.Add(1) }, &freed)
		pair := timedPair(t, trace+1, "frontend", "cart", start.Add(time.Second), false)
		for i := range pair {
			pair[i].Operation = operation.Operation{Protocol: operation.ProtocolHTTP, Name: name}
		}
		spans = append(spans, pair...)
	}
	builder.Apply(receivedAt(t, start.Add(time.Second), spans...))
	builder.Apply(receivedAt(t, start.Add(2*time.Second), timedPair(t, 0, "frontend", "cart", start, false)...))
	builder.Apply(batch.Heartbeat(start.Add(redFlushAhead)))
	for range retainedPolls {
		runtime.GC()
		if freed.Load() == retainedPairs {
			break
		}
		time.Sleep(retainedPause)
	}
	runtime.KeepAlive(builder)
	if got := freed.Load(); got != retainedPairs {
		t.Errorf("%d of %d operation names were freed after their windows closed", got, retainedPairs)
	}
}
