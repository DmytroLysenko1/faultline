package batch

import (
	"errors"
	"slices"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

var ErrEmpty = errors.New("empty batch")

const spanFixedBytes = 256

type Batch struct {
	Spans      []span.Span
	ReceivedAt time.Time
	Latest     eventtime.Interval
	Heartbeat  bool
}

func New(spans []span.Span, receivedAt time.Time) (Batch, error) {
	unstamped, err := NewOwning(slices.Clone(spans))
	if err != nil {
		return Batch{}, err
	}
	unstamped.ReceivedAt = receivedAt
	return unstamped, nil
}

func NewOwning(spans []span.Span) (Batch, error) {
	if len(spans) == 0 {
		return Batch{}, ErrEmpty
	}
	return Batch{
		Spans:  spans,
		Latest: latestOf(spans),
	}, nil
}

func Heartbeat(receivedAt time.Time) Batch {
	return Batch{
		ReceivedAt: receivedAt,
		Heartbeat:  true,
	}
}

func (b Batch) Candidate() (time.Time, bool) {
	if b.Heartbeat {
		return b.ReceivedAt, true
	}
	latest, timed := b.Latest.End()
	if !timed {
		return time.Time{}, false
	}
	if latest.After(b.ReceivedAt) {
		return b.ReceivedAt, true
	}
	return latest, true
}

func MaxFootprint(maxDecoded, maxSpans, minSpanWeight int64) int64 {
	return maxDecoded + maxSpans*max(spanFixedBytes-minSpanWeight, 0)
}

func (b Batch) Footprint() int64 {
	total := int64(len(b.Spans)) * spanFixedBytes
	distinct := make(map[string]struct{})
	for i := range b.Spans {
		total += unseenBytes(distinct, b.Spans[i].IdentityStrings())
	}
	return total
}

func (b Batch) Equal(other Batch) bool {
	return b.Heartbeat == other.Heartbeat &&
		b.ReceivedAt.Equal(other.ReceivedAt) &&
		slices.Equal(b.Spans, other.Spans)
}

func latestOf(spans []span.Span) eventtime.Interval {
	var latest eventtime.Interval
	for i := range spans {
		if spans[i].Interval.EndsAfter(latest) {
			latest = spans[i].Interval
		}
	}
	return latest
}

func unseenBytes(distinct map[string]struct{}, identities [span.IdentityStringCount]string) (added int64) {
	for _, identity := range identities {
		if _, seen := distinct[identity]; seen {
			continue
		}
		distinct[identity] = struct{}{}
		added += int64(len(identity))
	}
	return added
}
