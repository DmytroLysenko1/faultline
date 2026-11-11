package ingest_test

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	uniqueNameRequestSlack = 64 << 10
	uniqueNameCheckEvery   = 100
	uniqueNameDigits       = 8
)

// uniqueNamesRequest gives every span its own resource with a service name of the longest
// accepted length, so the journal charges a distinct string per span.
func uniqueNamesRequest() ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()
	sizer := ptrace.ProtoMarshaler{}
	for index := 0; index%uniqueNameCheckEvery != 0 ||
		sizer.TracesSize(traces) < otlptest.MaxRequestBytes-uniqueNameRequestSlack; index++ {
		resourceSpans := traces.ResourceSpans().AppendEmpty()
		name := fmt.Sprintf("%0*d", resource.MaxServiceNameBytes, index)[:resource.MaxServiceNameBytes-uniqueNameDigits] +
			fmt.Sprintf("%0*d", uniqueNameDigits, index)
		resourceSpans.Resource().Attributes().PutStr("service.name", name)
		otlptest.AppendNumberedSpan(resourceSpans.ScopeSpans().AppendEmpty().Spans(), index)
	}
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

// TestTheLargestRequestsFitTheBatchFootprintBound: the pipeline holds at most
// journal_max_bytes − MaxBatchFootprint, so the journal can always take one more request; the
// bound must cover both the most spans and the most distinct strings a request can carry.
func TestTheLargestRequestsFitTheBatchFootprintBound(t *testing.T) {
	tests := []struct {
		Name string
		Args ptraceotlp.ExportRequest
	}{
		{Name: "most spans", Args: ptraceotlpRequest(otlptest.MinimalSpans(t))},
		{Name: "a distinct longest service name per span", Args: uniqueNamesRequest()},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := footprintOf(t, tt.Args)
			t.Logf("footprint %.1f MiB, bound %.1f MiB", float64(got)/(1<<20), float64(otlp.MaxBatchFootprint())/(1<<20))
			if got > otlp.MaxBatchFootprint() {
				t.Errorf("request footprint %d B above MaxBatchFootprint %d B", got, otlp.MaxBatchFootprint())
			}
		})
	}
}

func TestTheSmallestJournalLeavesRoomToHold(t *testing.T) {
	if held := int64(journalLowerBound) - otlp.MaxBatchFootprint(); held <= 0 {
		t.Errorf("the smallest journal %d B leaves %d B to hold after the largest request", journalLowerBound, held)
	}
}
