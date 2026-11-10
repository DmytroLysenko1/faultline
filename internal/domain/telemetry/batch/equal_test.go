package batch_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

func TestBatchEqualDistinguishesHeartbeatReceiveTimeAndSpans(t *testing.T) {
	at := time.Unix(5, 0)
	data := func(t *testing.T) batch.Batch { return mustBatch(t, at, spanEndingAt(t, 1, 2*time.Second)) }
	tests := []struct {
		Name string
		Args func(t *testing.T) (batch.Batch, batch.Batch)
		Want bool
	}{
		{Name: "same data batch", Args: func(t *testing.T) (batch.Batch, batch.Batch) { return data(t), data(t) },
			Want: true},
		{Name: "heartbeat versus an empty data batch", Args: func(*testing.T) (batch.Batch, batch.Batch) {
			return batch.Heartbeat(at), batch.Batch{}.Stamped(at)
		}},
		{Name: "another receive time", Args: func(t *testing.T) (batch.Batch, batch.Batch) {
			return data(t), mustBatch(t, at.Add(time.Second), spanEndingAt(t, 1, 2*time.Second))
		}},
		{Name: "another span", Args: func(t *testing.T) (batch.Batch, batch.Batch) {
			return data(t), mustBatch(t, at, spanEndingAt(t, 2, 2*time.Second))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			left, right := tt.Args(t)
			got := [2]bool{left.Equal(right), cmp.Diff(left, right) == ""}
			if diff := cmp.Diff([2]bool{tt.Want, tt.Want}, got); diff != "" {
				t.Errorf("[Equal, cmp.Diff empty] mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
