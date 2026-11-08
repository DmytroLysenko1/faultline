package cli_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/cli"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

var (
	errServerDown = errors.New("server down")
	errDiskFull   = errors.New("disk full")
)

type clientFunc func() (*faultlinev1.GetGraphResponse, error)

func (f clientFunc) GetGraph(
	context.Context,
	*connect.Request[faultlinev1.GetGraphRequest],
) (*connect.Response[faultlinev1.GetGraphResponse], error) {
	response, err := f()
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(response), nil
}

func answering(built *faultlinev1.Graph) clientFunc {
	return func() (*faultlinev1.GetGraphResponse, error) {
		return &faultlinev1.GetGraphResponse{
			Graph: built,
		}, nil
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errDiskFull
}

func demoGraph() *faultlinev1.Graph {
	return graphOf(renderCases()[1].Nodes, renderCases()[1].Calls)
}

func edited(edit func(built *faultlinev1.Graph)) *faultlinev1.Graph {
	built := demoGraph()
	edit(built)
	return built
}

func TestExportRefusesABrokenResponseBeforeAnyOutput(t *testing.T) {
	tests := []struct {
		Name    string
		Args    clientFunc
		WantErr error
	}{
		{
			Name: "server error",
			Args: func() (*faultlinev1.GetGraphResponse, error) {
				return nil, connect.NewError(connect.CodeUnavailable, errServerDown)
			},
			WantErr: errServerDown,
		},
		{
			Name: "no graph in the response",
			Args: func() (*faultlinev1.GetGraphResponse, error) {
				return &faultlinev1.GetGraphResponse{}, nil
			},
			WantErr: cli.ErrMissingGraph,
		},
		{
			Name: "node without a service",
			Args: answering(edited(func(built *faultlinev1.Graph) {
				built.Nodes[1].Service = nil
			})),
			WantErr: cli.ErrMalformedNode,
		},
		{
			Name: "node with an empty id",
			Args: answering(edited(func(built *faultlinev1.Graph) {
				built.Nodes[0].Id = ""
			})),
			WantErr: cli.ErrMalformedNode,
		},
		{
			Name: "two nodes with one id",
			Args: answering(edited(func(built *faultlinev1.Graph) {
				built.Nodes[2].Id = built.Nodes[0].Id
			})),
			WantErr: cli.ErrMalformedNode,
		},
		{
			Name: "edge from a missing node",
			Args: answering(edited(func(built *faultlinev1.Graph) {
				built.Edges[0].SourceIndex = uint32(len(built.Nodes))
			})),
			WantErr: cli.ErrDanglingEdge,
		},
		{
			Name: "edge to a missing node",
			Args: answering(edited(func(built *faultlinev1.Graph) {
				built.Edges[1].TargetIndex = uint32(len(built.Nodes))
			})),
			WantErr: cli.ErrDanglingEdge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var out bytes.Buffer
			err := cli.Export(t.Context(), tt.Args, cli.DOT, &out)
			if !errors.Is(err, tt.WantErr) || !errors.Is(err, cli.ErrFetch) {
				t.Fatalf("Export = %v, want %v and ErrFetch", err, tt.WantErr)
			}
			if out.Len() != 0 {
				t.Errorf("output after a failed fetch:\n%s", out.String())
			}
		})
	}
}

func TestExportWritesExactlyTheRendering(t *testing.T) {
	tests := []struct {
		Name string
		Args cli.Renderer
	}{
		{
			Name: "dot",
			Args: cli.DOT,
		},
		{
			Name: "json",
			Args: cli.JSON,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var out bytes.Buffer
			if err := cli.Export(t.Context(), answering(demoGraph()), tt.Args, &out); err != nil {
				t.Fatalf("Export: %v", err)
			}
			if diff := cmp.Diff(string(rendered(t, tt.Args, demoGraph())), out.String()); diff != "" {
				t.Errorf("output (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExportReportsAFailedWrite(t *testing.T) {
	tests := []struct {
		Name string
		Args cli.Renderer
	}{
		{
			Name: "dot",
			Args: cli.DOT,
		},
		{
			Name: "json",
			Args: cli.JSON,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			err := cli.Export(t.Context(), answering(demoGraph()), tt.Args, failingWriter{})
			if !errors.Is(err, cli.ErrWrite) || !errors.Is(err, errDiskFull) {
				t.Errorf("Export = %v, want ErrWrite with the writer's error", err)
			}
		})
	}
}
