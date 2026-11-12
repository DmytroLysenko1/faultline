package record

import (
	"errors"
	"time"
)

type Kind uint8

const (
	KindBatch     Kind = 0
	KindHeartbeat Kind = 1
)

const (
	Version         uint8 = 1
	HeaderBytes           = 32
	MaxRecordBytes        = 6 << 20
	MaxTenantBytes        = 63
	MsgIDLength           = 32
	pooledBufferMax       = 1 << 20
)

const (
	magicFirst      byte = 'F'
	magicSecond     byte = 'J'
	offsetVersion        = 2
	offsetKind           = 3
	offsetFlags          = 4
	offsetTenantLen      = 5
	offsetPartition      = 6
	offsetWeight         = 8
	offsetWrittenAt      = 12
	offsetRules          = 20
	offsetCRC            = 24
	offsetReserved       = 28
	crcBytes             = 4
)

type spanFlag uint8

const (
	flagParent     spanFlag = 1
	flagEnd        spanFlag = 2
	flagLatency    spanFlag = 4
	flagFailed     spanFlag = 8
	flagExternal   spanFlag = 16
	knownSpanFlags          = flagParent | flagEnd | flagLatency | flagFailed | flagExternal
)

var (
	ErrCorrupt        = errors.New("journal record corrupt")
	ErrUnknownVersion = errors.New("journal record version unknown")
	ErrTooLarge       = errors.New("journal record too large")
	ErrInvalidTenant  = errors.New("journal record tenant invalid")
	ErrEmpty          = errors.New("journal record has no spans")
	errZeroID         = errors.New("zero trace or span id")
	errZeroParent     = errors.New("parent flag with a zero parent id")
	errUnknownKind    = errors.New("unknown span kind")
)

type Header struct {
	Version      uint8
	Kind         Kind
	Tenant       string
	Partition    uint16
	Weight       int64
	WrittenAt    time.Time
	RulesVersion uint32
}

type Publish func(data []byte, msgID string) error
