package query_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/query"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
)

var errReaderBroken = errors.New("reader broken")

type fakeReader struct {
	Snapshot *topology.Snapshot
	Err      error
	Calls    []context.Context
}

func (f *fakeReader) Latest(ctx context.Context) (*topology.Snapshot, error) {
	f.Calls = append(f.Calls, ctx)
	return f.Snapshot, f.Err
}

func TestGetGraphReturnsLatestSnapshot(t *testing.T) {
	latest := &topology.Snapshot{
		Revision: 7,
	}
	reader := &fakeReader{
		Snapshot: latest,
	}
	got, err := query.NewGetGraph(reader).Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got != latest {
		t.Errorf("Execute returned %p, want the reader's snapshot %p", got, latest)
	}
	if diff := cmp.Diff(1, len(reader.Calls)); diff != "" {
		t.Errorf("reader calls (-want +got):\n%s", diff)
	}
}

func TestGetGraphPassesTheCallerContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(t.Context(), ctxKey{}, "request")
	reader := &fakeReader{
		Snapshot: &topology.Snapshot{},
	}
	if _, err := query.NewGetGraph(reader).Execute(ctx); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if reader.Calls[0].Value(ctxKey{}) != "request" {
		t.Errorf("reader did not receive the caller context")
	}
}

func TestGetGraphWrapsReaderError(t *testing.T) {
	tests := []struct {
		Name    string
		Args    error
		Want    string
		WantErr error
	}{
		{
			Name:    "reader failure",
			Args:    errReaderBroken,
			Want:    "get graph: reader broken",
			WantErr: errReaderBroken,
		},
		{
			Name:    "deadline stays recognizable",
			Args:    context.DeadlineExceeded,
			Want:    "get graph: context deadline exceeded",
			WantErr: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := query.NewGetGraph(&fakeReader{
				Err: tt.Args,
			}).Execute(t.Context())
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message mismatch (-want +got):\n%s", diff)
			}
			if got != nil {
				t.Errorf("snapshot = %v, want nil on error", got)
			}
		})
	}
}
