package otlp

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
)

const (
	MaxRecvMsgBytes       = 4 << 20
	MaxHeaderListBytes    = 16 << 10
	MaxConcurrentStreams  = 10
	MaxConnections        = 1
	ExportHeapRatio       = 11
	MaxDecodedBytes       = 32 << 20
	MaxNestingDepth       = 32
	KeepaliveMinTime      = 10 * time.Second
	KeepaliveTime         = 30 * time.Second
	KeepaliveTimeout      = 10 * time.Second
	MaxConnectionIdle     = time.Minute
	MaxConnectionAge      = 5 * time.Minute
	MaxConnectionAgeGrace = 10 * time.Second
	ConnectionTimeout     = 3 * time.Second
	RetryDelay            = time.Second
	LogInterval           = 10 * time.Second
	MaxErrorMessageBytes  = 1 << 10
	MaxSpansPerRequest    = MaxDecodedBytes / decodelimit.MinSpanWeight

	RequestSlots             = 10
	MinTokenShare            = 2
	ActiveTokenWindow        = 10 * time.Second
	SlotHoldLimit            = 15 * time.Second
	ClientRetryGapMax        = 45 * time.Second
	WaitingTokenWindow       = SlotHoldLimit + ClientRetryGapMax
	DefaultRequestsPerSecond = 2000
	MaxRequestsPerSecond     = 100_000
	StreamWindowBytes        = 64 << 10
	ConnWindowBytes          = MaxConcurrentStreams * MaxRecvMsgBytes
)

func MaxBatchFootprint() int64 {
	return batch.MaxFootprint(MaxDecodedBytes, MaxSpansPerRequest, decodelimit.MinSpanWeight)
}

type clock interface {
	Now() time.Time
}

type Keepalive struct {
	Time                  time.Duration
	Timeout               time.Duration
	MaxConnectionIdle     time.Duration
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
}

type Options struct {
	Keepalive     Keepalive
	SlotHoldLimit time.Duration
	Clock         clock
	Logger        *slog.Logger
}

func (o Options) validate() error {
	if o.SlotHoldLimit <= 0 {
		return fmt.Errorf("slot hold limit %s: %w", o.SlotHoldLimit, errInvalidOptions)
	}
	if o.Clock == nil || o.Logger == nil {
		return fmt.Errorf("clock or logger missing: %w", errInvalidOptions)
	}
	return nil
}

func DefaultOptions(logger *slog.Logger, clock clock) Options {
	return Options{
		Keepalive: Keepalive{
			Time:                  KeepaliveTime,
			Timeout:               KeepaliveTimeout,
			MaxConnectionIdle:     MaxConnectionIdle,
			MaxConnectionAge:      MaxConnectionAge,
			MaxConnectionAgeGrace: MaxConnectionAgeGrace,
		},
		SlotHoldLimit: SlotHoldLimit,
		Clock:         clock,
		Logger:        logger,
	}
}

func AdmissionLimits(requestsPerSecond int) admission.Limits {
	return admission.Limits{
		Slots:             RequestSlots,
		MinShare:          MinTokenShare,
		ActiveWindow:      ActiveTokenWindow,
		WaitingWindow:     WaitingTokenWindow,
		RequestsPerSecond: requestsPerSecond,
	}
}

func WorstHeapBytes() int64 {
	return RequestSlots * MaxRecvMsgBytes * ExportHeapRatio
}
