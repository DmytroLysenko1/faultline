package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/cli"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

const (
	network         = "tcp"
	loopback        = "127.0.0.1:0"
	shortTimeout    = "200ms"
	callBudget      = 2 * time.Second
	tinyReadLimit   = 16
	terminalEscape  = "\x1b[2J"
	shownEscape     = `\x1B[2J`
	deadlineCode    = "deadline_exceeded"
	unavailableCode = "unavailable"
)

var (
	errMaintenance = errors.New("maintenance")
	errDiskFull    = errors.New("disk full")
)

type graphService struct {
	faultlinev1connect.UnimplementedGraphServiceHandler
	answer func(ctx context.Context) (*faultlinev1.Graph, error)
}

func (s graphService) GetGraph(
	ctx context.Context,
	_ *connect.Request[faultlinev1.GetGraphRequest],
) (*connect.Response[faultlinev1.GetGraphResponse], error) {
	served, err := s.answer(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&faultlinev1.GetGraphResponse{
		Graph: served,
	}), nil
}

func serveGraph(t *testing.T, answer func(ctx context.Context) (*faultlinev1.Graph, error)) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(faultlinev1connect.NewGraphServiceHandler(graphService{answer: answer},
		connect.WithSendMaxBytes(connectapi.SendMaxBytes)))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

func fixed(served *faultlinev1.Graph) func(ctx context.Context) (*faultlinev1.Graph, error) {
	return func(context.Context) (*faultlinev1.Graph, error) {
		return served, nil
	}
}

func smallGraph() *faultlinev1.Graph {
	return &faultlinev1.Graph{
		Revision:    3,
		GeneratedAt: timestamppb.New(time.Date(2026, time.November, 8, 0, 0, 0, 0, time.UTC)),
		Nodes: []*faultlinev1.Node{
			{Id: "svc:demo/cart", Service: &faultlinev1.ServiceRef{Namespace: "demo", Name: "cart"}},
			{Id: "svc:demo/checkout", Service: &faultlinev1.ServiceRef{Namespace: "demo", Name: "checkout"}},
		},
		Edges: []*faultlinev1.Edge{
			{SourceIndex: 1, TargetIndex: 0, Calls: 4, FailedCalls: 1},
		},
	}
}

func renderedWith(t *testing.T, render cli.Renderer, served *faultlinev1.Graph) string {
	t.Helper()
	var out bytes.Buffer
	if err := render(&out, served); err != nil {
		t.Fatalf("render: %v", err)
	}
	return out.String()
}

type callResult struct {
	Code   int
	Stdout string
}

func call(t *testing.T, server, format string, extra ...string) (callResult, string) {
	t.Helper()
	args := append([]string{commandGraph, commandExport, "-o", format, "--server", server}, extra...)
	var stdout, stderr bytes.Buffer
	code := realMain(process{
		Args:   args,
		Lookup: environment(nil),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return callResult{Code: code, Stdout: stdout.String()}, stderr.String()
}

func TestExportPrintsTheServedGraph(t *testing.T) {
	server := serveGraph(t, fixed(smallGraph()))
	tests := []struct {
		Name string
		Args string
		Want callResult
	}{
		{
			Name: "json",
			Args: formatJSON,
			Want: callResult{Code: exitOK, Stdout: renderedWith(t, cli.JSON, smallGraph())},
		},
		{
			Name: "dot",
			Args: formatDOT,
			Want: callResult{Code: exitOK, Stdout: renderedWith(t, cli.DOT, smallGraph())},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, stderr := call(t, server, tt.Args)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("faultctl -o %s (-want +got):\n%s\nstderr:\n%s", tt.Args, diff, stderr)
			}
		})
	}
}

func closedServer(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	probe, err := config.Listen(t.Context(), network, loopback)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	return "http://" + address
}

func TestFailedCallsExitWithOneAndPrintNothing(t *testing.T) {
	tests := []struct {
		Name string
		Args func(t *testing.T) string
		Want string
	}{
		{
			Name: "server answers unavailable",
			Args: func(t *testing.T) string {
				return serveGraph(t, func(context.Context) (*faultlinev1.Graph, error) {
					return nil, connect.NewError(connect.CodeUnavailable, errMaintenance)
				})
			},
			Want: unavailableCode,
		},
		{
			Name: "server message with a terminal escape is shown as text",
			Args: func(t *testing.T) string {
				return serveGraph(t, func(context.Context) (*faultlinev1.Graph, error) {
					return nil, connect.NewError(connect.CodeUnavailable, errors.New(terminalEscape))
				})
			},
			Want: shownEscape,
		},
		{
			Name: "server slower than the timeout",
			Args: func(t *testing.T) string {
				return serveGraph(t, func(ctx context.Context) (*faultlinev1.Graph, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				})
			},
			Want: deadlineCode,
		},
		{
			Name: "nothing listens on the port",
			Args: closedServer,
			Want: unavailableCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			server := tt.Args(t)
			started := time.Now()
			got, stderr := call(t, server, formatJSON, "--timeout", shortTimeout)
			if took := time.Since(started); took > callBudget {
				t.Errorf("faultctl took %v, want at most %v", took, callBudget)
			}
			if diff := cmp.Diff(callResult{Code: exitFailure}, got); diff != "" {
				t.Errorf("faultctl (-want +got):\n%s", diff)
			}
			assertOneLine(t, stderr, "faultctl: server "+server+": get graph: ", tt.Want)
			if strings.Contains(stderr, terminalEscape) {
				t.Errorf("stderr carries a raw terminal escape: %q", stderr)
			}
		})
	}
}

func assertOneLine(t *testing.T, stderr, prefix, cause string) {
	t.Helper()
	line, rest, _ := strings.Cut(stderr, "\n")
	if !strings.HasPrefix(line, prefix) || !strings.Contains(line, cause) || rest != "" {
		t.Errorf("stderr = %q, want one line starting %q with %q", stderr, prefix, cause)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errDiskFull
}

func TestFailedWriteExitsWithOne(t *testing.T) {
	server := serveGraph(t, fixed(smallGraph()))
	var stderr bytes.Buffer
	code := realMain(process{
		Args:   []string{commandGraph, commandExport, "-o", formatDOT, "--server", server},
		Lookup: environment(nil),
		Stdout: failingWriter{},
		Stderr: &stderr,
	})
	if code != exitFailure {
		t.Errorf("faultctl with a failing stdout = %d, want %d", code, exitFailure)
	}
	assertOneLine(t, stderr.String(), "faultctl: write output: ", errDiskFull.Error())
}

func TestResponseAboveTheReadLimitIsRefused(t *testing.T) {
	server := serveGraph(t, fixed(smallGraph()))
	var stdout bytes.Buffer
	err := export(t.Context(), &stdout, &exportOptions{
		Server:       server,
		Timeout:      callBudget,
		ReadMaxBytes: tinyReadLimit,
		Render:       cli.JSON,
	})
	if connect.CodeOf(err) != connect.CodeResourceExhausted || stdout.Len() != 0 {
		t.Errorf("export over the read limit = %v with %d bytes out, want resource_exhausted and nothing", err, stdout.Len())
	}
}
