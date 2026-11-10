package memjournal

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

type idleRule struct {
	after     time.Duration
	started   bool
	candidate time.Time
	since     time.Time
}

func (r *idleRule) due(now time.Time) bool {
	return r.started && now.Sub(r.since) >= r.after
}

func (r *idleRule) dueBefore(next batch.Batch) bool {
	_, grows := r.growth(next)
	return r.due(next.ReceivedAt) && !grows
}

func (r *idleRule) record(recorded batch.Batch) {
	if !r.started {
		r.started = true
		r.since = recorded.ReceivedAt
	}
	if candidate, grows := r.growth(recorded); grows {
		r.candidate = candidate
		r.since = recorded.ReceivedAt
	}
}

func (r *idleRule) growth(next batch.Batch) (time.Time, bool) {
	candidate, known := next.Candidate()
	return candidate, known && candidate.After(r.candidate)
}
