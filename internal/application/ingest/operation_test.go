package ingest_test

import (
	"iter"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

type recordSource []ingest.Record

func (r recordSource) Len() int {
	return len(r)
}

func (r recordSource) All() iter.Seq2[resource.Resource, *ingest.Record] {
	return func(yield func(resource.Resource, *ingest.Record) bool) {
		var record ingest.Record
		for i := range r {
			record = r[i]
			if !yield(telemetrytest.ValidResource(), &record) {
				return
			}
		}
	}
}

func cartRecord(id byte, spanKind kind.Kind) ingest.Record {
	fields := telemetrytest.ValidFields()
	fields.SpanID = ids.SpanID{id}
	fields.Kind = spanKind
	return ingest.Record{Fields: fields, Facts: operation.Facts{HTTPMethod: "GET", Path: "/cart/42"}}
}

func acceptedOperations(t *testing.T, records ...ingest.Record) []operation.Operation {
	t.Helper()
	h := newHarness(nil)
	if _, err := h.Accept.Execute(t.Context(), recordSource(records)); err != nil {
		t.Fatal(err)
	}
	var operations []operation.Operation
	for i := range h.Journal.Batches[0].Spans {
		operations = append(operations, h.Journal.Batches[0].Spans[i].Operation)
	}
	return operations
}

func TestAcceptBatchNamesOnlyClientAndServerOperations(t *testing.T) {
	cart := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart/{id}"}
	tests := []struct {
		Name string
		Args kind.Kind
		Want operation.Operation
	}{
		{Name: "client", Args: kind.Client, Want: cart},
		{Name: "server", Args: kind.Server, Want: cart},
		{Name: "internal", Args: kind.Internal, Want: operation.Operation{}},
		{Name: "producer", Args: kind.Producer, Want: operation.Operation{}},
		{Name: "consumer", Args: kind.Consumer, Want: operation.Operation{}},
		{Name: "unspecified", Args: kind.Unspecified, Want: operation.Operation{}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := acceptedOperations(t, cartRecord(1, tt.Args))
			if diff := cmp.Diff([]operation.Operation{tt.Want}, got); diff != "" {
				t.Errorf("operation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAcceptBatchSharesOneStringPerOperationNameInARequest(t *testing.T) {
	got := acceptedOperations(t, cartRecord(1, kind.Server), cartRecord(2, kind.Client))

	if unsafe.StringData(got[0].Name) != unsafe.StringData(got[1].Name) {
		t.Fatalf("two spans of %q hold two copies of the name, want one shared string", got[0].Name)
	}
}

func TestNewAcceptBatchWithoutANormalizerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewAcceptBatch without a normalizer did not panic")
		}
	}()
	ingest.NewAcceptBatch(&fakeJournal{}, fixedClock{}, nil)
}

func TestAcceptBatchAllocatesOneNamePerDistinctOperation(t *testing.T) {
	records := make(recordSource, 0, 64)
	for i := range cap(records) {
		records = append(records, cartRecord(byte(i+1), kind.Server))
	}
	h := newHarness(nil)
	baseline := testing.AllocsPerRun(20, func() { executeOrFail(t, h.Accept, records[:1]) })

	perRequest := testing.AllocsPerRun(20, func() { executeOrFail(t, h.Accept, records) })

	if perRequest > baseline+allowedGrowth {
		t.Fatalf("64 spans of one operation took %.0f allocations, one span %.0f: names are allocated per span",
			perRequest, baseline)
	}
}

const allowedGrowth = 8

func executeOrFail(t *testing.T, accept *ingest.AcceptBatch, records recordSource) {
	t.Helper()
	if _, err := accept.Execute(t.Context(), records); err != nil {
		t.Fatal(err)
	}
}

func TestJournalDoesNotKeepTheCapacityOfRejectedSpans(t *testing.T) {
	records := make(recordSource, 0, 100)
	records = append(records, cartRecord(1, kind.Server))
	for range 99 {
		rejected := cartRecord(2, kind.Server)
		rejected.Fields.TraceID = ids.TraceID{}
		records = append(records, rejected)
	}
	h := newHarness(nil)

	if _, err := h.Accept.Execute(t.Context(), records); err != nil {
		t.Fatal(err)
	}

	held := h.Journal.Batches[0].Spans
	if diff := cmp.Diff([2]int{1, 1}, [2]int{len(held), cap(held)}); diff != "" {
		t.Errorf("[len, cap] of the journaled spans when 99 of 100 were rejected (-want +got):\n%s", diff)
	}
}
