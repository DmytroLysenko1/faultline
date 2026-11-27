package admission

import (
	"strings"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const pruneInterval = time.Second

type bucket struct {
	tokens float64
	last   time.Time
}

func (b *bucket) refill(now time.Time, rate, burst float64) {
	b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	b.last = now
}

type entry struct {
	id           credential.ID
	held         int
	seen         time.Time
	waitingUntil time.Time
	recent       bool
	bucket       bucket
}

func (e *entry) quiet(now time.Time, window time.Duration) bool {
	return e.held == 0 && now.Sub(e.seen) > window
}

type book struct {
	inUse    int
	entries  map[credential.ID]*entry
	holders  map[*entry]credential.ID
	recent   int
	pruned   time.Time
	instance bucket
}

func (b *book) prune(now time.Time, window time.Duration) {
	if now.Sub(b.pruned) < pruneInterval {
		return
	}
	b.pruned = now
	for _, held := range b.entries {
		b.pruneEntry(held, now, window)
	}
}

func (b *book) pruneEntry(held *entry, now time.Time, window time.Duration) {
	if !held.quiet(now, window) {
		return
	}
	b.forgetRecent(held)
	if !now.Before(held.waitingUntil) {
		delete(b.entries, held.id)
	}
}

func (b *book) forgetRecent(held *entry) {
	if held.recent {
		held.recent = false
		b.recent--
	}
}

func (b *book) touch(id credential.ID, now time.Time, rate float64) *entry {
	held, known := b.entries[id]
	if !known {
		held = &entry{
			id: credential.ID(strings.Clone(string(id))),
			bucket: bucket{
				tokens: tokenBurst(rate, b.recent+1),
				last:   now,
			},
		}
		b.entries[held.id] = held
	}
	if !held.recent {
		held.recent = true
		b.recent++
	}
	held.seen = now
	return held
}

func tokenBurst(rate float64, active int) float64 {
	return max(1, rate/float64(active))
}

func (b *book) allowBoth(held *entry, now time.Time, rate float64) bool {
	b.instance.refill(now, rate, rate)
	held.bucket.refill(now, rate/float64(b.recent), tokenBurst(rate, b.recent))
	if held.bucket.tokens < 1 || b.instance.tokens < 1 {
		return false
	}
	held.bucket.tokens--
	b.instance.tokens--
	return true
}

func (b *book) take(held *entry) {
	held.held++
	held.waitingUntil = time.Time{}
	if held.held == 1 {
		b.holders[held] = held.id
	}
	b.inUse++
}

func (b *book) give(held *entry, now time.Time) {
	held.held--
	if held.held == 0 {
		delete(b.holders, held)
	}
	b.inUse--
	held.seen = now
}

func (b *book) topHolder() (credential.ID, int) {
	var top credential.ID
	most := 0
	for held, id := range b.holders {
		if held.held > most || (held.held == most && id < top) {
			top, most = id, held.held
		}
	}
	return top, most
}
