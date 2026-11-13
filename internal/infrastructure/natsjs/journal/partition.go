package journal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

type partitionSpans struct {
	partition uint16
	spans     []span.Span
}

func groupByPartition(spans []span.Span, partitions int) []partitionSpans {
	if len(spans) == 0 {
		return nil
	}
	partitionOf := make([]uint16, len(spans))
	var counts partitionCounts
	for index := range spans {
		partitionOf[index] = record.PartitionOf(spans[index].TraceID, partitions)
		counts[partitionOf[index]]++
	}
	touched := lo.CountBy(counts[:], func(count int) bool {
		return count > 0
	})
	if touched == 1 {
		return []partitionSpans{{
			partition: partitionOf[0],
			spans:     spans,
		}}
	}
	return scatter(spans, partitionOf, &counts, touched)
}

type partitionCounts [natsjs.MaxPartitions]int

func scatter(spans []span.Span, partitionOf []uint16, counts *partitionCounts, touched int) []partitionSpans {
	sorted := make([]span.Span, len(spans))
	groups := make([]partitionSpans, 0, touched)
	var next partitionCounts
	offset := 0
	for partition := range uint16(natsjs.MaxPartitions) {
		next[partition] = offset
		if counts[partition] > 0 {
			groups = append(groups, partitionSpans{
				partition: partition,
				spans:     sorted[offset : offset+counts[partition]],
			})
		}
		offset += counts[partition]
	}
	for index := range spans {
		sorted[next[partitionOf[index]]] = spans[index]
		next[partitionOf[index]]++
	}
	return groups
}

func (j *Journal) publishAll(ctx context.Context, groups []partitionSpans) []error {
	errs := make([]error, len(groups))
	if len(groups) == 1 {
		errs[0] = j.publishPartition(ctx, &groups[0])
		return errs
	}
	var publishes sync.WaitGroup
	for index := range groups {
		publishes.Go(func() {
			errs[index] = j.publishPartition(ctx, &groups[index])
		})
	}
	publishes.Wait()
	return errs
}

func (j *Journal) publishPartition(ctx context.Context, group *partitionSpans) error {
	reached := false
	err := j.encoder.Encode(j.options.Tenant, group.partition, group.spans, j.options.Clock.Now(),
		j.options.RulesVersion, func(data []byte, msgID string) error {
			reached = true
			return j.publishRecord(ctx, group.partition, data, msgID)
		})
	if err == nil {
		return nil
	}
	if reached {
		return fmt.Errorf("publish partition %d: %w", group.partition, err)
	}
	return j.encodeFailure(group.partition, err)
}

func (j *Journal) publishRecord(ctx context.Context, partition uint16, data []byte, msgID string) error {
	started := j.options.Clock.Now()
	duplicate, err := j.publisher.Publish(ctx, partition, data, msgID)
	j.counters.record(j.options.Clock.Now().Sub(started), duplicate, err)
	if err == nil || hasIngestMeaning(err) {
		return err
	}
	return errors.Join(ingest.ErrUnavailable, err)
}

func hasIngestMeaning(err error) bool {
	return lo.SomeBy([]error{ingest.ErrBatchTooLarge, ingest.ErrOverloaded, ingest.ErrUnavailable},
		func(sentinel error) bool {
			return errors.Is(err, sentinel)
		})
}

func (j *Journal) encodeFailure(partition uint16, err error) error {
	sentinel := ingest.ErrUnavailable
	if errors.Is(err, record.ErrTooLarge) {
		j.counters.encodeTooLarge.Add(1)
		sentinel = ingest.ErrBatchTooLarge
	}
	return fmt.Errorf("encode partition %d: %w", partition, errors.Join(sentinel, err))
}
