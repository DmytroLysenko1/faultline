package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
)

var (
	ErrOverloaded    = errors.New("span journal overloaded")
	ErrBatchTooLarge = errors.New("batch exceeds span journal capacity")
	ErrUnavailable   = errors.New("span journal unavailable")
)

const MaxReasons = 16

type spanJournal interface {
	Append(ctx context.Context, received batch.Batch) error
}

type clock interface {
	Now() time.Time
}

type SpanSource interface {
	Len() int
	All() iter.Seq2[resource.Resource, *Record]
}

type Record struct {
	Fields span.Fields
	Facts  operation.Facts
}

type AcceptResult struct {
	Accepted int
	Rejected int
	Reasons  []string
}

func (r *AcceptResult) reject(err error) {
	r.Rejected++
	if len(r.Reasons) >= MaxReasons {
		return
	}
	if reason := err.Error(); !slices.Contains(r.Reasons, reason) {
		r.Reasons = append(r.Reasons, reason)
	}
}

type Stats struct {
	AcceptedSpans       int64
	RejectedSpans       int64
	OverloadedResponses int64
	TooLargeResponses   int64
}

type AcceptBatch struct {
	journal    spanJournal
	clock      clock
	normalizer *operation.Normalizer
	accepted   atomic.Int64
	rejected   atomic.Int64
	overloaded atomic.Int64
	tooLarge   atomic.Int64
}

func NewAcceptBatch(journal spanJournal, clock clock, normalizer *operation.Normalizer) *AcceptBatch {
	if normalizer == nil {
		panic("ingest: NewAcceptBatch needs an operation normalizer")
	}
	return &AcceptBatch{
		journal:    journal,
		clock:      clock,
		normalizer: normalizer,
	}
}

func (a *AcceptBatch) Execute(ctx context.Context, spans SpanSource) (AcceptResult, error) {
	valid, result := a.partition(spans)
	if err := a.enqueue(ctx, valid); err != nil {
		return AcceptResult{}, err
	}
	a.accepted.Add(int64(result.Accepted))
	a.rejected.Add(int64(result.Rejected))
	return result, nil
}

func (a *AcceptBatch) enqueue(ctx context.Context, valid []span.Span) error {
	received, err := batch.NewOwning(fitted(valid), a.clock.Now())
	if errors.Is(err, batch.ErrEmpty) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("accept batch: %w", err)
	}
	if err := a.journal.Append(ctx, received); err != nil {
		a.countFailure(err)
		return fmt.Errorf("accept batch of %d spans: %w", len(valid), err)
	}
	return nil
}

func (a *AcceptBatch) countFailure(err error) {
	switch {
	case errors.Is(err, ErrOverloaded):
		a.overloaded.Add(1)
	case errors.Is(err, ErrBatchTooLarge):
		a.tooLarge.Add(1)
	}
}

func (a *AcceptBatch) Stats() Stats {
	return Stats{
		AcceptedSpans:       a.accepted.Load(),
		RejectedSpans:       a.rejected.Load(),
		OverloadedResponses: a.overloaded.Load(),
		TooLargeResponses:   a.tooLarge.Load(),
	}
}

func (a *AcceptBatch) partition(spans SpanSource) ([]span.Span, AcceptResult) {
	valid := make([]span.Span, 0, max(spans.Len(), 0))
	names := make(operation.Names)
	var result AcceptResult
	for source, record := range spans.All() {
		a.nameOperation(source, record, names)
		accepted, err := span.New(source, &record.Fields)
		if err != nil {
			result.reject(err)
			continue
		}
		valid = append(valid, accepted)
	}
	result.Accepted = len(valid)
	return valid, result
}

func (a *AcceptBatch) nameOperation(source resource.Resource, record *Record, names operation.Names) {
	if record.Fields.Kind != kind.Client && record.Fields.Kind != kind.Server {
		return
	}
	record.Fields.Operation = a.normalizer.NormalizeShared(source.ServiceRef(), &record.Facts, names)
}

func fitted(valid []span.Span) []span.Span {
	if len(valid) > cap(valid)/2 {
		return valid
	}
	return slices.Clone(valid)
}
