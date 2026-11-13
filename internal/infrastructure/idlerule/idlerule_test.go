package idlerule_test

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/idlerule"
)

const after = 35 * time.Second

var start = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)

func at(offset time.Duration) time.Time { return start.Add(offset) }

// data is a batch received at receivedAt whose span ended at end; a zero end means the end is unknown.
func data(t *testing.T, receivedAt, end time.Time) batch.Batch {
	t.Helper()
	observed := telemetrytest.NumberedSpan(t, kind.Server, 1, "checkout")
	if !end.IsZero() {
		observed.Interval = eventtime.FromUnixNanos(0, uint64(end.UnixNano()))
	}
	received, err := batch.New([]span.Span{observed}, receivedAt)
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	return received
}

// step is one event in the journal's life: a recorded batch, or a question to the rule.
type step struct {
	record  *batch.Batch
	dueAt   time.Time
	before  *batch.Batch
	wantDue bool
}

func TestTheRuleAnswersWhenAHeartbeatIsDue(t *testing.T) {
	tests := []struct {
		name  string
		steps func(t *testing.T) []step
	}{
		{name: "nothing recorded: never due", steps: func(*testing.T) []step {
			return []step{{dueAt: at(time.Hour), wantDue: false}}
		}},
		{name: "a heartbeat alone does not start the rule", steps: func(t *testing.T) []step {
			return []step{
				{record: new(batch.Heartbeat(at(0)))},
				{dueAt: at(time.Hour), wantDue: false},
				{before: new(data(t, at(time.Hour), at(-time.Minute))), wantDue: false},
			}
		}},
		{name: "a heartbeat before any data leaves the rule untouched", steps: func(t *testing.T) []step {
			return []step{
				{record: new(batch.Heartbeat(at(time.Hour)))},
				{record: new(data(t, at(0), at(-time.Second)))},
				{record: new(data(t, at(20*time.Second), at(19*time.Second)))},
				{dueAt: at(20*time.Second + after - time.Nanosecond), wantDue: false},
				{dueAt: at(20*time.Second + after), wantDue: true},
			}
		}},
		{name: "due exactly after the idle period that follows the first data", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{dueAt: at(after - time.Nanosecond), wantDue: false},
				{dueAt: at(after), wantDue: true},
			}
		}},
		{name: "data with a newer candidate restarts the idle period", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{record: new(data(t, at(20*time.Second), at(19*time.Second)))},
				{dueAt: at(after), wantDue: false},
				{dueAt: at(20*time.Second + after), wantDue: true},
			}
		}},
		{name: "late data that does not move the candidate keeps the idle period running", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{record: new(data(t, at(20*time.Second), at(-time.Minute)))},
				{dueAt: at(after), wantDue: true},
			}
		}},
		{name: "data without a known end does not move the candidate", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{record: new(data(t, at(20*time.Second), time.Time{}))},
				{dueAt: at(after), wantDue: true},
			}
		}},
		{name: "a heartbeat after data restarts the idle period: one pulse per period", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{record: new(batch.Heartbeat(at(after)))},
				{dueAt: at(after + time.Second), wantDue: false},
				{dueAt: at(2 * after), wantDue: true},
			}
		}},
		{name: "a pulse is due before data that does not move the candidate", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{before: new(data(t, at(after), at(-time.Minute))), wantDue: true},
			}
		}},
		{name: "no pulse before data that moves the candidate itself", steps: func(t *testing.T) []step {
			return []step{
				{record: new(data(t, at(0), at(-time.Second)))},
				{before: new(data(t, at(after), at(after-time.Second))), wantDue: false},
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := idlerule.Rule{
				After: after,
			}
			for i, s := range tt.steps(t) {
				switch {
				case s.record != nil:
					rule.Record(*s.record)
				case s.before != nil:
					if got := rule.DueBefore(*s.before); got != s.wantDue {
						t.Errorf("step %d: DueBefore(batch at %v) = %t, want %t", i, s.before.ReceivedAt, got, s.wantDue)
					}
				default:
					if got := rule.Due(s.dueAt); got != s.wantDue {
						t.Errorf("step %d: Due(%v) = %t, want %t", i, s.dueAt, got, s.wantDue)
					}
				}
			}
		})
	}
}
