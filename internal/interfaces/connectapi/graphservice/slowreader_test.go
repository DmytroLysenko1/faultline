package graphservice_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
)

const (
	slowNodes        = 4_000
	slowNamePad      = 200
	shrinkDeadline   = 5 * time.Second
	shrinkPoll       = 10 * time.Millisecond
	grpcEmptyMessage = "\x00\x00\x00\x00\x00"
	slowSeed         = 11
	slowFirstRune    = 'a'
	slowRunes        = 26
)

type staticQuery struct {
	snapshot *topology.Snapshot
}

func (q staticQuery) Execute(context.Context) (*topology.Snapshot, error) {
	return q.snapshot, nil
}

// slowSnapshot is large and incompressible enough that even its gzip response fills the socket
// buffers, so a client that stops reading leaves the server inside its writes.
func slowSnapshot() *topology.Snapshot {
	random := rand.New(rand.NewPCG(slowSeed, slowSeed))
	nodes := make([]topology.NodeView, 0, slowNodes)
	for index := range slowNodes {
		var name strings.Builder
		for range slowNamePad {
			name.WriteRune(slowFirstRune + rune(random.IntN(slowRunes)))
		}
		service := resource.ServiceRef{
			Namespace: "slow",
			Name:      fmt.Sprintf("%s%d", name.String(), index),
		}
		nodes = append(nodes, topology.NodeView{Node: graph.NewNode(service)})
	}
	return topologytest.WithWorstRED(&topology.Snapshot{Nodes: nodes})
}

func slowServer(t *testing.T, http2 bool) (*httptest.Server, *graphservice.Admission) {
	t.Helper()
	snapshot := slowSnapshot()
	_, handler := faultlinev1connect.NewGraphServiceHandler(graphservice.NewHandler(staticQuery{snapshot}, silentLogger()))
	admission, err := graphservice.NewAdmission(graphservice.ResponseWeight(slowNodes, 0), handler)
	if err != nil {
		t.Fatalf("NewAdmission: %v", err)
	}
	server := httptest.NewUnstartedServer(admission)
	server.EnableHTTP2 = http2
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, admission
}

type slowRequest struct {
	contentType string
	body        string
	headers     map[string]string
	http2       bool
}

func openSlowly(t *testing.T, server *httptest.Server, args *slowRequest) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		server.URL+faultlinev1connect.GraphServiceGetGraphProcedure, bytes.NewBufferString(args.body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Content-Type", args.contentType)
	for name, value := range args.headers {
		request.Header.Set(name, value)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	return response
}

func waitForFree(admission *graphservice.Admission, want int64) int64 {
	deadline := time.Now().Add(shrinkDeadline)
	got := freeBytes(admission)
	for got != want && time.Now().Before(deadline) {
		time.Sleep(shrinkPoll)
		got = freeBytes(admission)
	}
	return got
}

// TestASlowReaderKeepsOnlyTheWrittenWeightOnTheRealStack: through connect itself, once the
// response is marshalled and its first bytes are out, the budget holds only the tree and the
// encoded bytes (and a compressed copy) for as long as the client takes to read.
func TestASlowReaderKeepsOnlyTheWrittenWeightOnTheRealStack(t *testing.T) {
	weight := graphservice.ResponseWeight(slowNodes, 0)
	written := graphservice.WrittenWeight(slowNodes, 0)
	encoded := graphservice.ResponseBytes(slowNodes, 0)
	tests := []struct {
		Name string
		Args slowRequest
		Want int64
	}{
		{
			Name: "connect json identity",
			Args: slowRequest{
				contentType: "application/json",
				body:        "{}",
				headers:     map[string]string{"Accept-Encoding": "identity"},
			},
			Want: weight - written,
		},
		{
			Name: "connect json gzip",
			Args: slowRequest{
				contentType: "application/json",
				body:        "{}",
				headers:     map[string]string{"Accept-Encoding": "gzip"},
			},
			Want: weight - written - encoded,
		},
		{
			Name: "grpc-web gzip",
			Args: slowRequest{
				contentType: "application/grpc-web+proto",
				body:        grpcEmptyMessage,
				headers:     map[string]string{"Grpc-Accept-Encoding": "gzip"},
			},
			Want: weight - written - encoded,
		},
		{
			Name: "grpc gzip over http/2",
			Args: slowRequest{
				contentType: "application/grpc+proto",
				body:        grpcEmptyMessage,
				headers:     map[string]string{"Grpc-Accept-Encoding": "gzip", "Te": "trailers"},
				http2:       true,
			},
			Want: weight - written - encoded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			server, admission := slowServer(t, tt.Args.http2)
			response := openSlowly(t, server, &tt.Args)
			defer func() {
				if err := response.Body.Close(); err != nil {
					t.Errorf("close response: %v", err)
				}
			}()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("GetGraph status %d", response.StatusCode)
			}
			if got := waitForFree(admission, tt.Want); got != tt.Want {
				t.Errorf("free budget while the client reads nothing = %d, want %d (capacity %d)", got, tt.Want, weight)
			}
		})
	}
}
