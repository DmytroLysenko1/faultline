package batch

import (
	"errors"
	"slices"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

var ErrEmpty = errors.New("empty batch")

const spanFixedBytes = 256

type Batch struct {
	Spans      []span.Span
	ReceivedAt time.Time
	heartbeat  bool
}

func Heartbeat(receivedAt time.Time) Batch {
	return Batch{ReceivedAt: receivedAt, heartbeat: true}
}

func (b Batch) IsHeartbeat() bool {
	return b.heartbeat
}

func (b Batch) Candidate() (time.Time, bool) {
	if b.heartbeat {
		return b.ReceivedAt, true
	}
	latest, known := b.latestEnd()
	if !known {
		return time.Time{}, false
	}
	if latest.After(b.ReceivedAt) {
		return b.ReceivedAt, true
	}
	return latest, true
}

func (b Batch) latestEnd() (time.Time, bool) {
	var latest time.Time
	known := false
	for i := range b.Spans {
		end, timed := b.Spans[i].Interval.End()
		if timed && (!known || end.After(latest)) {
			latest, known = end, true
		}
	}
	return latest, known
}

func New(spans []span.Span, receivedAt time.Time) (Batch, error) {
	return NewOwning(slices.Clone(spans), receivedAt)
}

func NewOwning(spans []span.Span, receivedAt time.Time) (Batch, error) {
	if len(spans) == 0 {
		return Batch{}, ErrEmpty
	}
	return Batch{
		Spans:      spans,
		ReceivedAt: receivedAt,
	}, nil
}

func (b Batch) Footprint() int64 {
	total := int64(len(b.Spans)) * spanFixedBytes
	distinct := make(map[string]struct{})
	for i := range b.Spans {
		total += unseenBytes(distinct, b.Spans[i].IdentityStrings())
	}
	return total
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

func (b Batch) Equal(other Batch) bool {
	return b.heartbeat == other.heartbeat &&
		b.ReceivedAt.Equal(other.ReceivedAt) &&
		slices.Equal(b.Spans, other.Spans)
}
