package otlp

import (
	"context"
	"errors"
	"log/slog"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/throttle"
)

const (
	reasonTooHeavy  = "too_heavy"
	reasonTooDeep   = "too_deep"
	reasonMalformed = "malformed"
)

type events struct {
	logger     *slog.Logger
	rejected   *throttle.Throttle
	overloads  *throttle.Throttle
	rejections *throttle.Throttle
}

func newEvents(options Options) *events {
	return &events{
		logger:     options.Logger,
		rejected:   throttle.New(LogInterval, options.Clock),
		overloads:  throttle.New(LogInterval, options.Clock),
		rejections: throttle.New(LogInterval, options.Clock),
	}
}

func (e *events) accepted(ctx context.Context, result ingest.AcceptResult) {
	if result.Rejected == 0 {
		return
	}
	suppressed, admitted := e.rejected.Admit(int64(result.Rejected))
	if !admitted {
		return
	}
	e.logger.LogAttrs(ctx, slog.LevelWarn, "otlp.export.rejected", withTrace(ctx,
		slog.Int("rejected_spans", result.Rejected),
		slog.String("reason", firstReason(result.Reasons)),
		slog.Int64("suppressed_requests", suppressed.Events),
		slog.Int64("suppressed_spans", suppressed.Weight),
	)...)
}

func (e *events) overloaded(ctx context.Context, _ error) {
	suppressed, admitted := e.overloads.Admit(0)
	if !admitted {
		return
	}
	e.logger.LogAttrs(ctx, slog.LevelWarn, "otlp.export.overloaded", withTrace(ctx,
		slog.Int64("suppressed_requests", suppressed.Events),
	)...)
}

func (e *events) tooLarge(ctx context.Context, err error) {
	e.logger.LogAttrs(ctx, slog.LevelError, "otlp.export.too_large", withTrace(ctx, slog.Any("error", err))...)
}

func (e *events) internal(ctx context.Context, err error) {
	e.logger.LogAttrs(ctx, slog.LevelError, "otlp.export.failed", withTrace(ctx, slog.Any("error", err))...)
}

func (e *events) interrupted(ctx context.Context, err error) {
	if !e.logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	e.logger.LogAttrs(ctx, slog.LevelDebug, "otlp.export.interrupted", withTrace(ctx, slog.Any("error", err))...)
}

func (e *events) decodeRejected(err error, weight int64) {
	suppressed, admitted := e.rejections.Admit(0)
	if !admitted {
		return
	}
	attributes := []any{
		slog.String("reason", decodeReason(err)),
		slog.Int64("suppressed_requests", suppressed.Events),
	}
	if errors.Is(err, decodelimit.ErrTooHeavy) {
		attributes = append(attributes, slog.Int64("weight", weight), slog.Int64("limit", MaxDecodedBytes))
	}
	e.logger.Warn("otlp.export.decode_rejected", attributes...)
}

func decodeReason(err error) string {
	switch {
	case errors.Is(err, decodelimit.ErrTooHeavy):
		return reasonTooHeavy
	case errors.Is(err, decodelimit.ErrTooDeep):
		return reasonTooDeep
	}
	return reasonMalformed
}

func firstReason(reasons []string) string {
	if len(reasons) == 0 {
		return ""
	}
	return reasons[0]
}

func withTrace(ctx context.Context, attributes ...slog.Attr) []slog.Attr {
	if traceID, found := traceIDFrom(ctx); found {
		return append(attributes, slog.String("trace_id", traceID))
	}
	return attributes
}
