package checkpoint

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	headerPosition          = "Checkpoint-Position"
	headerReceivedAt        = "Checkpoint-Received-At"
	headerStreamCreated     = "Checkpoint-Stream-Created"
	headerEpoch             = "Checkpoint-Epoch"
	headerCheckpointVersion = "Checkpoint-Format-Version"
	headerRecordVersion     = "Checkpoint-Record-Version"
	headerLimitsHash        = "Checkpoint-Limits-Hash"
	headerRulesHash         = "Checkpoint-Rules-Hash"
	uint32Bits              = 32
	uint8Bits               = 8
)

var errBadHeader = errors.New("checkpoint meta header does not parse")

type headerField struct {
	name  string
	bits  int
	store func(meta *processing.CheckpointMeta, value uint64)
}

var headerFields = []headerField{
	{name: headerPosition, bits: uintBits, store: func(m *processing.CheckpointMeta, v uint64) { m.Position.Sequence = v }},
	{name: headerReceivedAt, bits: uintBits, store: func(m *processing.CheckpointMeta, v uint64) {
		m.ReceivedAt = unixTime(v)
	}},
	{name: headerStreamCreated, bits: uintBits, store: func(m *processing.CheckpointMeta, v uint64) {
		m.StreamCreated = unixTime(v)
	}},
	{name: headerEpoch, bits: uintBits, store: func(m *processing.CheckpointMeta, v uint64) { m.Epoch = v }},
	{name: headerCheckpointVersion, bits: uint32Bits, store: func(m *processing.CheckpointMeta, v uint64) {
		m.CheckpointVersion = uint32(v)
	}},
	{name: headerRecordVersion, bits: uint8Bits, store: func(m *processing.CheckpointMeta, v uint64) {
		m.RecordVersion = uint8(v)
	}},
	{name: headerLimitsHash, bits: uintBits, store: func(m *processing.CheckpointMeta, v uint64) { m.LimitsHash = v }},
	{name: headerRulesHash, bits: uint32Bits, store: func(m *processing.CheckpointMeta, v uint64) { m.RulesHash = uint32(v) }},
}

func decodeMeta(headers nats.Header, partition uint16) (processing.CheckpointMeta, error) {
	meta := processing.CheckpointMeta{
		Position: processing.Position{
			Partition: partition,
		},
	}
	for _, field := range headerFields {
		value, err := strconv.ParseUint(headers.Get(field.name), decimal, field.bits)
		if err != nil {
			return processing.CheckpointMeta{}, fmt.Errorf("%w: %w: %s: %w", errCorrupt, errBadHeader,
				field.name, err)
		}
		field.store(&meta, value)
	}
	return meta, nil
}

func unixTime(nanos uint64) time.Time {
	if nanos == 0 || nanos > math.MaxInt64 {
		return time.Time{}
	}
	return time.Unix(0, int64(nanos)).UTC()
}
