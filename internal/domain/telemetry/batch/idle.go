package batch

import "time"

type IdleRule struct {
	After     time.Duration
	Started   bool
	Candidate time.Time
	Since     time.Time
}

func ResumedIdleRule(after time.Duration, since time.Time) IdleRule {
	return IdleRule{
		After:   after,
		Started: true,
		Since:   since,
	}
}

func (r *IdleRule) Due(now time.Time) bool {
	return r.Started && now.Sub(r.Since) >= r.After
}

func (r *IdleRule) DueBefore(next Batch) bool {
	_, grows := r.growth(next)
	return r.Due(next.ReceivedAt) && !grows
}

func (r *IdleRule) Record(recorded Batch) {
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

func (r *IdleRule) growth(next Batch) (time.Time, bool) {
	candidate, known := next.Candidate()
	return candidate, known && candidate.After(r.Candidate)
}
