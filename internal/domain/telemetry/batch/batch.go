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
	spans      []span.Span
	receivedAt time.Time
	latest     eventtime.Interval
	heartbeat  bool
}

func New(spans []span.Span, receivedAt time.Time) (Batch, error) {
	unstamped, err := NewOwning(slices.Clone(spans))
	if err != nil {
		return Batch{}, err
	}
	return unstamped.Stamped(receivedAt), nil
}

func NewOwning(spans []span.Span) (Batch, error) {
	if len(spans) == 0 {
		return Batch{}, ErrEmpty
	}
	return Batch{
		spans:  spans,
		latest: latestOf(spans),
	}, nil
}

func Heartbeat(receivedAt time.Time) Batch {
	return Batch{
		receivedAt: receivedAt,
		heartbeat:  true,
	}
}

func (b Batch) Stamped(receivedAt time.Time) Batch {
	b.receivedAt = receivedAt
	return b
}

func (b Batch) Spans() []span.Span {
	return b.spans
}

func (b Batch) ReceivedAt() time.Time {
	return b.receivedAt
}

func (b Batch) IsHeartbeat() bool {
	return b.heartbeat
}

func (b Batch) Candidate() (time.Time, bool) {
	if b.heartbeat {
		return b.receivedAt, true
	}
	latest, timed := b.latest.End()
	if !timed {
		return time.Time{}, false
	}
	if latest.After(b.receivedAt) {
		return b.receivedAt, true
	}
	return latest, true
}

func (b Batch) Footprint() int64 {
	total := int64(len(b.spans)) * spanFixedBytes
	distinct := make(map[string]struct{})
	for i := range b.spans {
		total += unseenBytes(distinct, b.spans[i].IdentityStrings())
	}
	return total
}

func (b Batch) Equal(other Batch) bool {
	return b.heartbeat == other.heartbeat &&
		b.receivedAt.Equal(other.receivedAt) &&
		slices.Equal(b.spans, other.spans)
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
