package stream

import "time"

type Mark struct {
	Time  time.Time
	Known bool
}

type Watermark struct {
	delay         time.Duration
	lateness      time.Duration
	current       Mark
	maxCandidate  Mark
	maxReceivedAt Mark
	coverage      Mark
}

func NewWatermark(cfg Config) (*Watermark, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Watermark{
		delay:    cfg.Delay,
		lateness: cfg.AllowedLateness,
	}, nil
}

func (w *Watermark) Advance(receivedAt time.Time, candidate Mark) Mark {
	w.maxReceivedAt = later(w.maxReceivedAt, receivedAt)
	if !candidate.Known {
		return w.current
	}
	candidate = notAfter(candidate, receivedAt)
	w.maxCandidate = later(w.maxCandidate, candidate.Time)
	w.current = later(w.current, candidate.Time.Add(-w.delay))
	if !w.coverage.Known {
		w.coverage = Mark{
			Time:  w.current.Time.Add(-w.lateness),
			Known: true,
		}
	}
	return w.current
}

func (w *Watermark) Anchor(batchCandidate Mark, receivedAt time.Time) time.Time {
	switch {
	case batchCandidate.Known:
		return notAfter(batchCandidate, receivedAt).Time
	case w.maxCandidate.Known:
		return w.maxCandidate.Time
	}
	return receivedAt
}

func (w *Watermark) Current() Mark {
	return w.current
}

func (w *Watermark) MaxReceivedAt() Mark {
	return w.maxReceivedAt
}

func (w *Watermark) CoverageStart() Mark {
	return w.coverage
}

func notAfter(candidate Mark, receivedAt time.Time) Mark {
	if candidate.Time.After(receivedAt) {
		candidate.Time = receivedAt
	}
	return candidate
}

func later(mark Mark, t time.Time) Mark {
	if mark.Known && !t.After(mark.Time) {
		return mark
	}
	return Mark{
		Time:  t,
		Known: true,
	}
}
