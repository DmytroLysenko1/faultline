package stream

import "time"

type Mark struct {
	Time  time.Time
	Known bool
}

type Watermark struct {
	delay         time.Duration
	lateness      time.Duration
	Current       Mark
	MaxReceivedAt Mark
	CoverageStart Mark
	maxCandidate  Mark
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
	w.MaxReceivedAt = later(w.MaxReceivedAt, receivedAt)
	if !candidate.Known {
		return w.Current
	}
	candidate = notAfter(candidate, receivedAt)
	w.maxCandidate = later(w.maxCandidate, candidate.Time)
	w.Current = later(w.Current, candidate.Time.Add(-w.delay))
	if !w.CoverageStart.Known {
		w.CoverageStart = Mark{
			Time:  w.Current.Time.Add(-w.lateness),
			Known: true,
		}
	}
	return w.Current
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
