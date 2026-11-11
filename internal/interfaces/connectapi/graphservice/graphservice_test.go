package graphservice_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphmap"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
)

var errQueryBroken = errors.New("query broken")

const roomyBudget = 1 << 30

type fakeQuery struct {
	Snapshot *topology.Snapshot
	Err      error
}

func (f *fakeQuery) Execute(_ context.Context) (*topology.Snapshot, error) {
	return f.Snapshot, f.Err
}

func getGraph(t *testing.T, handler *graphservice.Handler) (*faultlinev1.Graph, error) {
	t.Helper()
	ctx, release := graphservice.AdmittedContext(t.Context(), roomyBudget)
	defer release()
	response, err := handler.GetGraph(ctx, connect.NewRequest(&faultlinev1.GetGraphRequest{}))
	if err != nil {
		return nil, err
	}
	return response.Msg.GetGraph(), nil
}

func TestGetGraphMapsTheSnapshot(t *testing.T) {
	snapshot := &topology.Snapshot{
		Revision:    2,
		GeneratedAt: telemetrytest.Epoch(),
		Nodes: []topology.NodeView{
			{Node: graph.Node{
				ID:      graph.ServiceNodeID(telemetrytest.Service("cart")),
				Service: telemetrytest.Service("cart"),
			}},
		},
	}
	want, err := graphmap.Graph(snapshot)
	if err != nil {
		t.Fatalf("graphmap.Graph: %v", err)
	}
	handler := graphservice.NewHandler(&fakeQuery{
		Snapshot: snapshot,
	}, silentLogger())
	got, err := getGraph(t, handler)
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("graph mismatch (-want +got):\n%s", diff)
	}
}

func TestGetGraphPassesContextErrors(t *testing.T) {
	tests := []struct {
		Name    string
		Args    error
		Want    connect.Code
		WantErr error
	}{
		{
			Name:    "canceled request",
			Args:    fmt.Errorf("get graph: latest snapshot: %w", context.Canceled),
			Want:    connect.CodeCanceled,
			WantErr: context.Canceled,
		},
		{
			Name:    "deadline exceeded",
			Args:    fmt.Errorf("get graph: latest snapshot: %w", context.DeadlineExceeded),
			Want:    connect.CodeDeadlineExceeded,
			WantErr: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var logs bytes.Buffer
			_, err := getGraph(t, graphservice.NewHandler(&fakeQuery{
				Err: tt.Args,
			}, slog.New(slog.NewTextHandler(&logs, nil))))
			if diff := cmp.Diff(tt.Want, connect.CodeOf(err)); diff != "" {
				t.Errorf("code mismatch (-want +got):\n%s", diff)
			}
			if !errors.Is(err, tt.WantErr) {
				t.Errorf("err = %v, want it to wrap %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(connect.NewError(tt.Want, tt.WantErr).Error(), err.Error()); diff != "" {
				t.Errorf("the client got more than the bare context error (-want +got):\n%s", diff)
			}
			if logs.Len() != 0 {
				t.Errorf("a client-side cancellation was logged as a failure: %s", logs.String())
			}
		})
	}
}

func TestGetGraphHidesInternalFailures(t *testing.T) {
	dangling := &topology.Snapshot{
		Edges: []topology.EdgeView{
			{Edge: graph.Edge{
				Source: "svc:shop/a",
				Target: "svc:shop/b",
			}},
		},
	}
	tests := []struct {
		Name string
		Args fakeQuery
		Want string
	}{
		{
			Name: "query failure",
			Args: fakeQuery{
				Err: errQueryBroken,
			},
			Want: "query broken",
		},
		{
			Name: "nil snapshot without an error",
			Args: fakeQuery{},
			Want: "nil snapshot",
		},
		{
			Name: "edge to a missing node",
			Args: fakeQuery{
				Snapshot: dangling,
			},
			Want: "edge points to a missing node: svc:shop/a → svc:shop/b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var logs bytes.Buffer
			_, err := getGraph(t, graphservice.NewHandler(&tt.Args, slog.New(slog.NewTextHandler(&logs, nil))))
			if diff := cmp.Diff(connect.CodeInternal, connect.CodeOf(err)); diff != "" {
				t.Errorf("code mismatch (-want +got):\n%s", diff)
			}
			if !errors.Is(err, graphservice.ErrInternal) {
				t.Errorf("err = %v, want %v", err, graphservice.ErrInternal)
			}
			if strings.Contains(err.Error(), tt.Want) {
				t.Errorf("the cause leaked to the client: %v", err)
			}
			for _, want := range []string{"api.get_graph_failed", tt.Want} {
				if !strings.Contains(logs.String(), want) {
					t.Errorf("log %q does not contain %q", logs.String(), want)
				}
			}
		})
	}
}

func silentLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
