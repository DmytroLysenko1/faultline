package clock_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/clock"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const clockSection = checkpoint.Tag(1)

func restoreClock(write func(w *checkpoint.Writer)) (clock.Monotonic, error) {
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(clockSection)
	write(w)
	w.End()
	r := checkpoint.NewReader(&out)
	r.Begin(clockSection)
	restored := clock.Restore(r)
	r.End()
	return restored, r.Finish()
}

// TestARestoredClockKeepsItsEpochAndNeverGoesBack: a reading from before the restore still keeps the
// latest moment, exactly as in the clock that never stopped.
func TestARestoredClockKeepsItsEpochAndNeverGoesBack(t *testing.T) {
	tests := []struct {
		name     string
		readings []time.Duration
	}{
		{name: "never started", readings: nil},
		{name: "started and moved", readings: []time.Duration{0, 5 * time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var original clock.Monotonic
			for _, offset := range tt.readings {
				original.Advance(telemetrytest.Epoch().Add(offset))
			}

			restored, err := restoreClock(original.WriteCheckpoint)

			if err != nil {
				t.Fatalf("restore: %v", err)
			}
			back := telemetrytest.Epoch().Add(time.Second)
			want, got := original.Advance(back), restored.Advance(back)
			if want != got || !original.Now().Equal(restored.Now()) {
				t.Errorf("after a reading from the past: original %s at %v, restored %s at %v",
					want, original.Now(), got, restored.Now())
			}
			if diff := cmp.Diff(original, restored); diff != "" {
				t.Errorf("clock mismatch (-original +restored):\n%s", diff)
			}
		})
	}
}

func TestAClockCheckpointRefusesAClockThatWentBack(t *testing.T) {
	tests := []struct {
		name  string
		write func(w *checkpoint.Writer)
	}{
		{name: "negative elapsed", write: func(w *checkpoint.Writer) {
			w.Bool(true)
			w.Time(telemetrytest.Epoch())
			w.Duration(-time.Second)
		}},
		{name: "not started but with an epoch", write: func(w *checkpoint.Writer) {
			w.Bool(false)
			w.Time(telemetrytest.Epoch())
			w.Duration(0)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := restoreClock(tt.write)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}
