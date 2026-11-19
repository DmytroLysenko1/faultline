package batch_test

import (
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const idleAfter = 35 * time.Second

var idleStart = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)

func idleAt(offset time.Duration) time.Time { return idleStart.Add(offset) }

// idleBatch is a batch received at receivedAt whose span ended at end; a zero end means the end is unknown.
func idleBatch(t *testing.T, receivedAt, end time.Time) batch.Batch {
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

// idleStep is one event in the journal's life: a recorded batch, or a question to the rule.
type idleStep struct {
	record  *batch.Batch
	dueAt   time.Time
	before  *batch.Batch
	wantDue bool
}

func TestTheRuleAnswersWhenAHeartbeatIsDue(t *testing.T) {
	tests := []struct {
		name  string
		steps func(t *testing.T) []idleStep
	}{
		{name: "nothing recorded: never due", steps: func(*testing.T) []idleStep {
			return []idleStep{{dueAt: idleAt(time.Hour), wantDue: false}}
		}},
		{name: "a heartbeat alone does not start the rule", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(batch.Heartbeat(idleAt(0)))},
				{dueAt: idleAt(time.Hour), wantDue: false},
				{before: new(idleBatch(t, idleAt(time.Hour), idleAt(-time.Minute))), wantDue: false},
			}
		}},
		{name: "a heartbeat before any data leaves the rule untouched", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(batch.Heartbeat(idleAt(time.Hour)))},
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{record: new(idleBatch(t, idleAt(20*time.Second), idleAt(19*time.Second)))},
				{dueAt: idleAt(20*time.Second + idleAfter - time.Nanosecond), wantDue: false},
				{dueAt: idleAt(20*time.Second + idleAfter), wantDue: true},
			}
		}},
		{name: "due exactly after the idle period that follows the first data", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{dueAt: idleAt(idleAfter - time.Nanosecond), wantDue: false},
				{dueAt: idleAt(idleAfter), wantDue: true},
			}
		}},
		{name: "data with a newer candidate restarts the idle period", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{record: new(idleBatch(t, idleAt(20*time.Second), idleAt(19*time.Second)))},
				{dueAt: idleAt(idleAfter), wantDue: false},
				{dueAt: idleAt(20*time.Second + idleAfter), wantDue: true},
			}
		}},
		{name: "late data that does not move the candidate keeps the idle period running",
			steps: func(t *testing.T) []idleStep {
				return []idleStep{
					{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
					{record: new(idleBatch(t, idleAt(20*time.Second), idleAt(-time.Minute)))},
					{dueAt: idleAt(idleAfter), wantDue: true},
				}
			}},
		{name: "data without a known end does not move the candidate", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{record: new(idleBatch(t, idleAt(20*time.Second), time.Time{}))},
				{dueAt: idleAt(idleAfter), wantDue: true},
			}
		}},
		{name: "a heartbeat after data restarts the idle period: one pulse per period", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{record: new(batch.Heartbeat(idleAt(idleAfter)))},
				{dueAt: idleAt(idleAfter + time.Second), wantDue: false},
				{dueAt: idleAt(2 * idleAfter), wantDue: true},
			}
		}},
		{name: "a pulse is due before data that does not move the candidate", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{before: new(idleBatch(t, idleAt(idleAfter), idleAt(-time.Minute))), wantDue: true},
			}
		}},
		{name: "no pulse before data that moves the candidate itself", steps: func(t *testing.T) []idleStep {
			return []idleStep{
				{record: new(idleBatch(t, idleAt(0), idleAt(-time.Second)))},
				{before: new(idleBatch(t, idleAt(idleAfter), idleAt(idleAfter-time.Second))), wantDue: false},
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := batch.IdleRule{
				After: idleAfter,
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

// TestAResumedRuleIsDueOneIdlePeriodAfterTheRestoredReceiveTime: a feed that restarts behind a
// checkpoint must not wait for fresh data before its first heartbeat.
func TestAResumedRuleIsDueOneIdlePeriodAfterTheRestoredReceiveTime(t *testing.T) {
	rule := batch.ResumedIdleRule(idleAfter, idleAt(0))

	if rule.Due(idleAt(idleAfter - time.Nanosecond)) {
		t.Errorf("a resumed rule is due before %s passed", idleAfter)
	}
	if !rule.Due(idleAt(idleAfter)) {
		t.Errorf("a resumed rule is not due %s after the restored receive time", idleAfter)
	}
}
