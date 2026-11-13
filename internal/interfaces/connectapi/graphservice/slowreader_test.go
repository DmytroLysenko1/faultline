package graphservice_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	shrinkDeadline   = 5 * time.Second
	shrinkPoll       = 10 * time.Millisecond
	heldFor          = 300 * time.Millisecond
	grpcEmptyMessage = "\x00\x00\x00\x00\x00"
	slowSeed         = 11
	slowAlphabet     = "abcdefghijklmnopqrstuvwxyz0123456789"
	socketBuffer     = 32 << 10
	streamWindow     = 64 << 10
)

var errNotTCP = errors.New("not a TCP connection")

type staticQuery struct {
	snapshot *topology.Snapshot
}

func (q staticQuery) Execute(context.Context) (*topology.Snapshot, error) {
	return q.snapshot, nil
}

// slowSnapshot carries names of the longest allowed size made of random letters and digits, so even
// its gzip response is several times what the fixed socket buffers and the HTTP/2 window can hold,
// and a client that stops reading leaves the server inside its writes.
func slowSnapshot() *topology.Snapshot {
	random := rand.New(rand.NewPCG(slowSeed, slowSeed))
	namePad := resource.MaxServiceNameBytes - len(strconv.Itoa(slowNodes))
	nodes := make([]topology.NodeView, 0, slowNodes)
	for index := range slowNodes {
		service := resource.ServiceRef{
			Namespace: randomName(random, resource.MaxServiceNamespaceBytes),
			Name:      fmt.Sprintf("%s%d", randomName(random, namePad), index),
		}
		nodes = append(nodes, topology.NodeView{Node: graph.NewNode(service)})
	}
	return topologytest.WithWorstRED(&topology.Snapshot{Nodes: nodes})
}

func randomName(random *rand.Rand, length int) string {
	var name strings.Builder
	for range length {
		name.WriteByte(slowAlphabet[random.IntN(len(slowAlphabet))])
	}
	return name.String()
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
	server.Listener = fixedSendBuffers{Listener: server.Listener, t: t}
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
	response, err := fixedReceiveBuffers(t, server).Do(request)
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	return response
}

// Fixed socket buffers on both ends: the kernel grows unset ones up to 4 MiB each on loopback, enough
// to swallow the whole gzip response, and then the server finishes while the client reads nothing.
func fixedBuffer(conn net.Conn, set func(*net.TCPConn, int) error) error {
	tcp, isTCP := conn.(*net.TCPConn)
	if !isTCP {
		return fmt.Errorf("%T: %w", conn, errNotTCP)
	}
	return set(tcp, socketBuffer)
}

// fixedSendBuffers returns Accept's errors as they are: http.Server retries only what still is a
// net.Error. A buffer it cannot fix fails the test, not the server.
type fixedSendBuffers struct {
	net.Listener
	t *testing.T
}

func (l fixedSendBuffers) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if err := fixedBuffer(conn, (*net.TCPConn).SetWriteBuffer); err != nil {
		l.t.Errorf("fix the send buffer: %v", err)
	}
	return conn, nil
}

func fixedReceiveBuffers(t *testing.T, server *httptest.Server) *http.Client {
	t.Helper()
	base, isHTTP := server.Client().Transport.(*http.Transport)
	if !isHTTP {
		t.Fatalf("test server transport is %T", server.Client().Transport)
	}
	transport := base.Clone()
	transport.HTTP2 = &http.HTTP2Config{MaxReceiveBufferPerConnection: streamWindow, MaxReceiveBufferPerStream: streamWindow}
	t.Cleanup(transport.CloseIdleConnections)
	var dialer net.Dialer
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, fmt.Errorf("dial: %w", err)
		}
		if err := fixedBuffer(conn, (*net.TCPConn).SetReadBuffer); err != nil {
			return nil, errors.Join(err, conn.Close())
		}
		return conn, nil
	}
	return &http.Client{Transport: transport}
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

// staysFree is false as soon as the budget moves away from want: a server that finished its writes
// gives everything back, and a passing glimpse of the shrunk budget proves nothing.
func staysFree(admission *graphservice.Admission, want int64) bool {
	deadline := time.Now().Add(heldFor)
	for time.Now().Before(deadline) {
		if freeBytes(admission) != want {
			return false
		}
		time.Sleep(shrinkPoll)
	}
	return true
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
				t.Fatalf("free budget while the client reads nothing = %d, want %d (capacity %d)", got, tt.Want, weight)
			}
			if !staysFree(admission, tt.Want) {
				t.Errorf("free budget left %d while the client reads nothing: the server finished its writes", tt.Want)
			}
		})
	}
}
