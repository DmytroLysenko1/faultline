package otlp

import (
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
)

const (
	MaxRecvMsgBytes       = 4 << 20
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
	ConnectionTimeout     = 10 * time.Second
	RetryDelay            = time.Second
	LogInterval           = 10 * time.Second
	MaxErrorMessageBytes  = 1 << 10
	MaxSpansPerRequest    = MaxDecodedBytes / decodelimit.MinSpanWeight
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
	Keepalive Keepalive
	Clock     clock
	Logger    *slog.Logger
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
		Clock:  clock,
		Logger: logger,
	}
}

func WorstHeapBytes() int64 {
	return MaxConnections * MaxConcurrentStreams * MaxRecvMsgBytes * ExportHeapRatio
}
