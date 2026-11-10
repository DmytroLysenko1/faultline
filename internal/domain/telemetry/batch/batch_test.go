package batch_test

import (
	"errors"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const spanFixedBudget = 256

func TestSpanFixedSizeFitsBudget(t *testing.T) {
	if got := unsafe.Sizeof(span.Span{}); got > spanFixedBudget {
		t.Fatalf(
			"unsafe.Sizeof(Span{}) = %d B, budget %d B: raise batch spanFixedBytes and the memory budget",
			got,
			spanFixedBudget,
		)
	}
}

func TestNewBatchRejectsEmpty(t *testing.T) {
	_, err := batch.New(nil, time.Unix(1, 0))
	if !errors.Is(err, batch.ErrEmpty) {
		t.Fatalf("err = %v, want %v", err, batch.ErrEmpty)
	}
}

func TestBatchFootprint(t *testing.T) {
	checkout := telemetrytest.ValidResource()
	payment := telemetrytest.ResourceWith(func(r *resource.Resource) {
		r.ServiceName = "payment"
	})
	tests := []struct {
		Name    string
		Args    []resource.Resource
		Want    int64
		WantErr error
	}{
		{
			Name: "one span",
			Args: []resource.Resource{
				checkout,
			},
			Want: spanFixedBudget + int64(len("otel-demo")+len("checkout")),
		},
		{
			Name: "shared resource strings are counted once",
			Args: []resource.Resource{
				checkout,
				checkout,
				checkout,
				payment,
			},
			Want: 4*spanFixedBudget + int64(len("otel-demo")+len("checkout")+len("payment")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			spans := lo.Map(tt.Args, func(resource resource.Resource, i int) span.Span {
				return telemetrytest.MustSpan(t, resource, new(telemetrytest.FieldsWith(func(f *span.Fields) {
					f.SpanID = ids.SpanID{
						byte(i + 1),
					}
				})))
			})
			received, err := batch.New(spans, time.Unix(1, 0))
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, received.Footprint()); diff != "" {
				t.Errorf("Footprint() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewBatchCopiesSpans(t *testing.T) {
	receivedAt := time.Unix(42, 0)
	spans := []span.Span{
		telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
			f.SpanID = ids.SpanID{
				1,
			}
		}))),
		telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
			f.SpanID = ids.SpanID{
				2,
			}
		}))),
	}
	received, err := batch.New(spans, receivedAt)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}

	spans[0] = telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.FieldsWith(func(f *span.Fields) {
		f.SpanID = ids.SpanID{
			7,
		}
	})))

	got := lo.Map(received.Spans, func(span span.Span, _ int) ids.SpanID {
		return span.SpanID
	})
	if diff := cmp.Diff([]ids.SpanID{
		{
			1,
		},
		{
			2,
		},
	}, got); diff != "" {
		t.Errorf("batch spans changed with the caller's slice (-want +got):\n%s", diff)
	}
	if !received.ReceivedAt.Equal(receivedAt) {
		t.Errorf("ReceivedAt = %v, want %v", received.ReceivedAt, receivedAt)
	}
}

func TestNewOwningRejectsEmpty(t *testing.T) {
	for _, spans := range [][]span.Span{
		nil,
		{},
	} {
		if _, err := batch.NewOwning(spans, time.Unix(1, 0)); !errors.Is(err, batch.ErrEmpty) {
			t.Errorf("NewOwning(%d spans) err = %v, want %v", len(spans), err, batch.ErrEmpty)
		}
	}
}

func TestNewOwningTakesSpansWithoutCopy(t *testing.T) {
	const runs = 100
	spans := []span.Span{
		telemetrytest.MustSpan(t, telemetrytest.ValidResource(), new(telemetrytest.ValidFields())),
	}
	received, err := batch.NewOwning(spans, time.Unix(1, 0))
	if err != nil {
		t.Fatalf("NewOwning: %v", err)
	}
	if &received.Spans[0] != &spans[0] {
		t.Errorf("NewOwning copied the spans, want the batch to own the caller's slice")
	}
	allocations := testing.AllocsPerRun(runs, func() {
		if _, err := batch.NewOwning(spans, time.Unix(1, 0)); err != nil {
			t.Errorf("NewOwning: %v", err)
		}
	})
	if allocations != 0 {
		t.Errorf("NewOwning allocates %.0f times, want 0 on the ingest hot path", allocations)
	}
}
