package batch_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

func spanEndingAt(t *testing.T, id byte, end time.Duration) span.Span {
	t.Helper()
	return telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
		f.SpanID = ids.SpanID{id}
		f.Start = uint64(time.Second)
		f.End = uint64(end)
	})))
}

func TestCandidateIsTheLatestEndNotLaterThanTheReceiveTime(t *testing.T) {
	receivedAt := time.Unix(100, 0)
	type want struct {
		Candidate time.Time
		Known     bool
	}
	tests := []struct {
		Name string
		Args func(t *testing.T) batch.Batch
		Want want
	}{
		{Name: "heartbeat is its receive time", Args: func(*testing.T) batch.Batch { return batch.Heartbeat(receivedAt) },
			Want: want{Candidate: receivedAt, Known: true}},
		{Name: "latest end of timed spans", Args: func(t *testing.T) batch.Batch {
			return mustBatch(t, receivedAt, spanEndingAt(t, 1, 50*time.Second), spanEndingAt(t, 2, 80*time.Second))
		}, Want: want{Candidate: time.Unix(80, 0), Known: true}},
		{Name: "end after the receive time is capped", Args: func(t *testing.T) batch.Batch {
			return mustBatch(t, receivedAt, spanEndingAt(t, 1, 300*time.Second))
		}, Want: want{Candidate: receivedAt, Known: true}},
		{Name: "untimed spans give no candidate", Args: func(t *testing.T) batch.Batch {
			return mustBatch(t, receivedAt, spanEndingAt(t, 1, 0))
		}, Want: want{}},
		{Name: "untimed spans do not lower the latest end", Args: func(t *testing.T) batch.Batch {
			return mustBatch(t, receivedAt, spanEndingAt(t, 1, 0), spanEndingAt(t, 2, 70*time.Second))
		}, Want: want{Candidate: time.Unix(70, 0), Known: true}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			candidate, known := tt.Args(t).Candidate()
			if diff := cmp.Diff(tt.Want, want{Candidate: candidate, Known: known}); diff != "" {
				t.Errorf("candidate mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func mustBatch(t *testing.T, receivedAt time.Time, spans ...span.Span) batch.Batch {
	t.Helper()
	received, err := batch.New(spans, receivedAt)
	if err != nil {
		t.Fatal(err)
	}
	return received
}

func TestHeartbeatIsABatchWithoutSpans(t *testing.T) {
	pulse := batch.Heartbeat(time.Unix(5, 0))
	data := mustBatch(t, time.Unix(5, 0), spanEndingAt(t, 1, 2*time.Second))

	type observed struct {
		PulseIsHeartbeat, DataIsHeartbeat bool
		PulseSpans, PulseFootprint        int64
	}
	want := observed{PulseIsHeartbeat: true}
	got := observed{PulseIsHeartbeat: pulse.IsHeartbeat(), DataIsHeartbeat: data.IsHeartbeat(),
		PulseSpans: int64(len(pulse.Spans())), PulseFootprint: pulse.Footprint()}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("heartbeat mismatch (-want +got):\n%s", diff)
	}
}

func TestFootprintCountsEachDistinctOperationNameOnce(t *testing.T) {
	named := func(id byte, name string) span.Span {
		return telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
			f.SpanID = ids.SpanID{id}
			if name != "" {
				f.Operation = operation.Operation{Protocol: operation.ProtocolHTTP, Name: name}
			}
		})))
	}
	unnamed := mustBatch(t, time.Unix(1, 0), named(1, ""), named(2, ""), named(3, "")).Footprint()

	got := mustBatch(t, time.Unix(1, 0), named(1, "GET /a"), named(2, "GET /a"), named(3, "GET /bb")).Footprint()

	want := unnamed + int64(len("GET /a")+len("GET /bb"))
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("footprint mismatch (-want +got):\n%s", diff)
	}
}
