package ids_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
)

func TestIsZero(t *testing.T) {
	type want struct {
		Trace bool
		Span  bool
	}
	tests := []struct {
		Name    string
		Args    ids.CallKey
		Want    want
		WantErr error
	}{
		{
			Name: "zero ids",
			Args: ids.CallKey{},
			Want: want{
				Trace: true,
				Span:  true,
			},
		},
		{
			Name: "last byte set",
			Args: ids.NewCallKey(ids.TraceID{
				15: 1,
			}, ids.SpanID{
				7: 1,
			}),
			Want: want{},
		},
		{
			Name: "first byte set",
			Args: ids.NewCallKey(ids.TraceID{
				1,
			}, ids.SpanID{
				1,
			}),
			Want: want{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := want{
				Trace: tt.Args.TraceID.IsZero(),
				Span:  tt.Args.ID.IsZero(),
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("IsZero mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewCallKey(t *testing.T) {
	got := ids.NewCallKey(ids.TraceID{
		1,
	}, ids.SpanID{
		2,
	})
	if diff := cmp.Diff(ids.CallKey{
		TraceID: ids.TraceID{
			1,
		},
		ID: ids.SpanID{
			2,
		},
	}, got); diff != "" {
		t.Errorf("call key mismatch (-want +got):\n%s", diff)
	}
}
