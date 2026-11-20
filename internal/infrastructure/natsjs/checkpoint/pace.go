package checkpoint

import "time"

type pace struct {
	rate   float64
	burst  float64
	tokens float64
	at     time.Time
}

func newPace(rate, burst int64, now time.Time) *pace {
	return &pace{
		rate:   float64(rate),
		burst:  float64(burst),
		tokens: float64(burst),
		at:     now,
	}
}

func (p *pace) reserve(now time.Time, bytes int64) time.Duration {
	if elapsed := now.Sub(p.at); elapsed > 0 {
		p.tokens = min(p.burst, p.tokens+p.rate*elapsed.Seconds())
		p.at = now
	}
	p.tokens -= float64(bytes)
	if p.tokens >= 0 {
		return 0
	}
	return time.Duration(-p.tokens / p.rate * float64(time.Second))
}
