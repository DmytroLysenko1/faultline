package processingtest_test

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/application/processing/processingtest"
)

type lastRetained struct {
	mu       sync.Mutex
	through  processing.Position
	retained bool
}

func (r *lastRetained) Commit(_ context.Context, through processing.Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.through, r.retained = through, true
	return nil
}

func (r *lastRetained) last() (processing.Position, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.through, r.retained
}

func TestTheFakeStoreKeepsTheSinkContract(t *testing.T) {
	processingtest.RunCheckpointSinkContract(t, func(t *testing.T) processingtest.SinkHarness {
		retention := &lastRetained{}
		store := processingtest.NewCheckpointStore(t, retention)
		return processingtest.SinkHarness{
			Sink: store,
			Hold: store.Hold,
			Settled: func(context.Context) error {
				return nil
			},
			Stop: func(context.Context) error {
				store.Close()
				return nil
			},
			Latest: func(ctx context.Context) (processing.CheckpointMeta, io.ReadCloser, error) {
				return store.Latest(ctx, 0)
			},
			Retained: retention.last,
		}
	})
}

func TestTheFakesSecondFinalWithNothingInFlightMovesRetention(t *testing.T) {
	retention := &lastRetained{}
	store := processingtest.NewCheckpointStore(t, retention)
	for _, sequence := range []uint64{firstFinal, secondFinal} {
		writer, err := store.BeginOnStop(t.Context(), &processing.CheckpointMeta{
			Position: processing.Position{Sequence: sequence},
		})
		if err != nil {
			t.Fatalf("BeginFinal at %d = %v", sequence, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close at %d = %v", sequence, err)
		}
	}
	if through, retained := retention.last(); !retained || through.Sequence != secondFinal {
		t.Errorf("retained through %d (%t), want %d: the second final is an ordinary checkpoint", through.Sequence,
			retained, secondFinal)
	}
}

const (
	firstFinal  = 10
	secondFinal = 20
)
