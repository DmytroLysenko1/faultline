package span_test

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

func TestNewCarriesTheIntervalAndTheOperation(t *testing.T) {
	cart := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart"}
	fields := telemetrytest.FieldsWith(func(f *span.Fields) {
		f.Start = uint64(time.Second)
		f.End = uint64(3 * time.Second)
		f.Operation = cart
	})

	created, err := span.New(telemetrytest.ValidResource(), &fields)
	if err != nil {
		t.Fatal(err)
	}

	end, timed := created.Interval.End()
	latency, measured := created.Interval.Latency()
	type observed struct {
		End       time.Time
		Timed     bool
		Latency   time.Duration
		Measured  bool
		Operation operation.Operation
	}
	want := observed{End: time.Unix(3, 0), Timed: true, Latency: 2 * time.Second, Measured: true, Operation: cart}
	got := observed{End: end, Timed: timed, Latency: latency, Measured: measured, Operation: created.Operation}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("span mismatch (-want +got):\n%s", diff)
	}
}

func TestNewRejectsAnInvalidOperation(t *testing.T) {
	tests := []struct {
		Name    string
		Args    operation.Operation
		WantErr error
	}{
		{Name: "reserved overflow name", Args: operation.Operation{Protocol: operation.ProtocolHTTP,
			Name: operation.OverflowName}, WantErr: operation.ErrInvalidOperation},
		{Name: "name too long", Args: operation.Operation{Protocol: operation.ProtocolHTTP,
			Name: strings.Repeat("a", operation.MaxNameBytes+1)}, WantErr: operation.ErrInvalidOperation},
		{Name: "name without a protocol", Args: operation.Operation{Name: "x"}, WantErr: operation.ErrInvalidOperation},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fields := telemetrytest.FieldsWith(func(f *span.Fields) { f.Operation = tt.Args })
			_, err := span.New(telemetrytest.ValidResource(), &fields)
			if !errors.Is(err, tt.WantErr) || !errors.Is(err, span.ErrInvalid) {
				t.Fatalf("New() = %v, want %v wrapped in span.ErrInvalid", err, tt.WantErr)
			}
		})
	}
}

func TestSpanFitsTheJournalSlotAndNamesItsOperation(t *testing.T) {
	cart := telemetrytest.FieldsWith(func(f *span.Fields) {
		f.Operation = operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart"}
	})
	created := telemetrytest.MustSpan(t, telemetrytest.ValidResource(), &cart)

	type layout struct {
		FitsIn128    bool
		LastIdentity string
	}
	got := layout{FitsIn128: unsafe.Sizeof(created) <= 128, LastIdentity: created.IdentityStrings()[2]}
	if diff := cmp.Diff(layout{FitsIn128: true, LastIdentity: "GET /cart"}, got); diff != "" {
		t.Errorf("span layout mismatch (-want +got):\n%s", diff)
	}
}
