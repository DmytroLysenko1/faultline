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
}

func New(spans []span.Span, receivedAt time.Time) (Batch, error) {
	if len(spans) == 0 {
		return Batch{}, ErrEmpty
	}
	return Batch{
		Spans:      slices.Clone(spans),
		ReceivedAt: receivedAt,
	}, nil
}

func (b Batch) Footprint() int64 {
	total := int64(len(b.Spans)) * spanFixedBytes
	distinct := make(map[string]struct{})
	for _, observed := range b.Spans {
		total += unseenBytes(distinct, observed.IdentityStrings())
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
