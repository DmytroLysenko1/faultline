package idlerule

import (
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
)

type Rule struct {
	After     time.Duration
	Started   bool
	Candidate time.Time
	Since     time.Time
}

func (r *Rule) Due(now time.Time) bool {
	return r.Started && now.Sub(r.Since) >= r.After
}

func (r *Rule) DueBefore(next batch.Batch) bool {
	_, grows := r.growth(next)
	return r.Due(next.ReceivedAt) && !grows
}

func (r *Rule) Record(recorded batch.Batch) {
	if !r.Started && recorded.Heartbeat {
		return
	}
	if !r.Started {
		r.Started = true
		r.Since = recorded.ReceivedAt
	}
	if candidate, grows := r.growth(recorded); grows {
		r.Candidate = candidate
		r.Since = recorded.ReceivedAt
	}
}

func (r *Rule) growth(next batch.Batch) (time.Time, bool) {
	candidate, known := next.Candidate()
	return candidate, known && candidate.After(r.Candidate)
}
