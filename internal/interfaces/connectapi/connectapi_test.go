package connectapi_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/DmytroLysenko1/faultline/internal/application/query"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/limit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/recovery"
)

const (
	maxConfiguredNodes  = 20_000
	maxConfiguredEdges  = 100_000
	slowClientNodes     = 10_000
	maxNamespaceBytes   = 255
	maxNameBytes        = 255
	nodeSuffixDigits    = 5
	shortTimeout        = 50 * time.Millisecond
	slowClientTimeout   = 500 * time.Millisecond
	enterTimeout        = 10 * time.Second
	tinySendMaxBytes    = 16
	grpcPrefixBytes     = 5
	grpcResourceStatus  = "8"
	getGraphProcedure   = "/faultline.v1.GraphService/GetGraph"
	reflectionProcedure = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
)

var errReaderPanicked = errors.New("reader panicked")

type staticReader struct {
	Snapshot *topology.Snapshot
}

func (r *staticReader) Latest(_ context.Context) (*topology.Snapshot, error) {
	return r.Snapshot, nil
}

type blockingReader struct {
	Snapshot *topology.Snapshot
	Entered  chan struct{}
	Release  chan struct{}
}

func (r *blockingReader) Latest(ctx context.Context) (*topology.Snapshot, error) {
	select {
	case r.Entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-r.Release:
		return r.Snapshot, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type panickingReader struct {
	Snapshot *topology.Snapshot
	Calls    atomic.Int64
}

func (r *panickingReader) Latest(_ context.Context) (*topology.Snapshot, error) {
	if r.Calls.Add(1) == 1 {
		panic(errReaderPanicked)
	}
	return r.Snapshot, nil
}

type largeThenSmallReader struct {
	Large *topology.Snapshot
	Small *topology.Snapshot
	Calls atomic.Int64
}

func (r *largeThenSmallReader) Latest(_ context.Context) (*topology.Snapshot, error) {
	if r.Calls.Add(1) == 1 {
		return r.Large, nil
	}
	return r.Small, nil
}

type reader interface {
	Latest(ctx context.Context) (*topology.Snapshot, error)
}

func silentLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func defaultOptions() connectapi.Options {
	return connectapi.DefaultOptions(silentLogger())
}

type transport struct {
	Name     string
	HTTP2    bool
	Protocol connect.ClientOption
}

func transports() []transport {
	return []transport{
		{
			Name:     "grpc over http2",
			HTTP2:    true,
			Protocol: connect.WithGRPC(),
		},
		{
			Name:     "connect json over http2",
			HTTP2:    true,
			Protocol: connect.WithProtoJSON(),
		},
		{
			Name:     "connect json over http1.1",
			HTTP2:    false,
			Protocol: connect.WithProtoJSON(),
		},
	}
}

func newMux(t *testing.T, graphs reader, options connectapi.Options) *http.ServeMux {
	t.Helper()
	mux, err := connectapi.NewMux(graphservice.NewHandler(query.NewGetGraph(graphs), options.Logger), options)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	return mux
}

func startServer(t *testing.T, handler http.Handler, http2 bool) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = http2
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func startGraphServer(t *testing.T, graphs reader, options connectapi.Options) *httptest.Server {
	t.Helper()
	return startServer(t, newMux(t, graphs, options), true)
}

func client(server *httptest.Server, options ...connect.ClientOption) faultlinev1connect.GraphServiceClient {
	return faultlinev1connect.NewGraphServiceClient(server.Client(), server.URL, options...)
}

func uncompressed(protocol connect.ClientOption) []connect.ClientOption {
	return []connect.ClientOption{
		protocol,
		connect.WithAcceptCompression("gzip", nil, nil),
	}
}

func getGraph(ctx context.Context, graphs faultlinev1connect.GraphServiceClient) (*faultlinev1.Graph, error) {
	response, err := graphs.GetGraph(ctx, connect.NewRequest(&faultlinev1.GetGraphRequest{}))
	if err != nil {
		return nil, err
	}
	return response.Msg.GetGraph(), nil
}

func smallSnapshot() *topology.Snapshot {
	frontend := telemetrytest.Service("frontend")
	cart := telemetrytest.Service("cart")
	return &topology.Snapshot{
		Revision:    4,
		GeneratedAt: telemetrytest.Epoch().Add(time.Second),
		Nodes: []graph.Node{
			graph.NewNode(cart),
			{
				ID:                    graph.ServiceNodeID(frontend),
				Service:               frontend,
				UnansweredCalls:       2,
				UnansweredFailedCalls: 1,
			},
		},
		Edges: []graph.Edge{
			{
				Source:      graph.ServiceNodeID(frontend),
				Target:      graph.ServiceNodeID(cart),
				Calls:       12,
				FailedCalls: 3,
			},
		},
	}
}

func TestProtocolsReturnTheSameGraph(t *testing.T) {
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, defaultOptions())
	want, err := getGraph(t.Context(), client(server, connect.WithGRPC()))
	if err != nil {
		t.Fatalf("gRPC GetGraph: %v", err)
	}
	if len(want.GetNodes()) != 2 || len(want.GetEdges()) != 1 {
		t.Fatalf("gRPC graph is incomplete: %v", want)
	}
	protocols := map[string][]connect.ClientOption{
		"connect proto": nil,
		"connect json": {
			connect.WithProtoJSON(),
		},
	}
	for name, options := range protocols {
		t.Run(name, func(t *testing.T) {
			got, err := getGraph(t.Context(), client(server, options...))
			if err != nil {
				t.Fatalf("GetGraph: %v", err)
			}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("graph differs from gRPC (-grpc +%s):\n%s", name, diff)
			}
		})
	}
}

func TestFifthConcurrentRequestIsRejectedWithRetryInfo(t *testing.T) {
	for _, tt := range transports() {
		t.Run(tt.Name, func(t *testing.T) {
			graphs := &blockingReader{
				Snapshot: smallSnapshot(),
				Entered:  make(chan struct{}, connectapi.MaxConcurrentGetGraph),
				Release:  make(chan struct{}),
			}
			release := sync.OnceFunc(func() {
				close(graphs.Release)
			})
			server := startServer(t, newMux(t, graphs, defaultOptions()), tt.HTTP2)
			graphClient := client(server, tt.Protocol)
			t.Cleanup(release)
			results := occupyAllSlots(t, graphs, graphClient)
			fifth, cancel := context.WithTimeout(t.Context(), enterTimeout)
			_, err := getGraph(fifth, graphClient)
			cancel()
			release()
			checkRetryable(t, err)
			for range connectapi.MaxConcurrentGetGraph {
				if err := <-results; err != nil {
					t.Errorf("one of the first %d requests failed: %v", connectapi.MaxConcurrentGetGraph, err)
				}
			}
		})
	}
}

func occupyAllSlots(
	t *testing.T,
	graphs *blockingReader,
	graphClient faultlinev1connect.GraphServiceClient,
) <-chan error {
	t.Helper()
	results := make(chan error, connectapi.MaxConcurrentGetGraph)
	for range connectapi.MaxConcurrentGetGraph {
		go func() {
			_, err := getGraph(t.Context(), graphClient)
			results <- err
		}()
	}
	for range connectapi.MaxConcurrentGetGraph {
		select {
		case <-graphs.Entered:
		case err := <-results:
			t.Fatalf("a request ended before it took a slot: %v", err)
		case <-time.After(enterTimeout):
			t.Fatalf("requests did not take the slots within %v", enterTimeout)
		}
	}
	return results
}

func checkRetryable(t *testing.T, err error) {
	t.Helper()
	if diff := cmp.Diff(connect.CodeResourceExhausted, connect.CodeOf(err)); diff != "" {
		t.Fatalf("code mismatch (-want +got):\n%s", diff)
	}
	if !strings.Contains(err.Error(), limit.ErrTooManyRequests.Error()) {
		t.Errorf("err = %v, want the too-many-requests message", err)
	}
	connectErr, ok := errors.AsType[*connect.Error](err)
	if !ok {
		t.Fatalf("err %T is not a *connect.Error", err)
	}
	var delays []time.Duration
	for _, detail := range connectErr.Details() {
		value, err := detail.Value()
		if err != nil {
			t.Fatalf("error detail: %v", err)
		}
		if info, ok := value.(*errdetails.RetryInfo); ok {
			delays = append(delays, info.GetRetryDelay().AsDuration())
		}
	}
	if diff := cmp.Diff([]time.Duration{limit.RetryDelay}, delays); diff != "" {
		t.Errorf("retry info mismatch (-want +got):\n%s", diff)
	}
}

func TestSlowRequestEndsWithDeadlineExceeded(t *testing.T) {
	for _, tt := range transports() {
		t.Run(tt.Name, func(t *testing.T) {
			graphs := &blockingReader{
				Entered: make(chan struct{}, 1),
				Release: make(chan struct{}),
			}
			options := defaultOptions()
			options.Timeout = shortTimeout
			options.SendTimeout = enterTimeout
			server := startServer(t, newMux(t, graphs, options), tt.HTTP2)
			_, err := getGraph(t.Context(), client(server, tt.Protocol))
			if diff := cmp.Diff(connect.CodeDeadlineExceeded, connect.CodeOf(err)); diff != "" {
				t.Errorf("code mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPanicBecomesInternalAndServerKeepsServing(t *testing.T) {
	graphs := &panickingReader{
		Snapshot: smallSnapshot(),
	}
	graphClient := client(startGraphServer(t, graphs, defaultOptions()), connect.WithGRPC())
	_, err := getGraph(t.Context(), graphClient)
	if diff := cmp.Diff(connect.CodeInternal, connect.CodeOf(err)); diff != "" {
		t.Fatalf("code mismatch (-want +got):\n%s", diff)
	}
	if !strings.Contains(err.Error(), recovery.ErrPanic.Error()) || strings.Contains(err.Error(), errReaderPanicked.Error()) {
		t.Errorf("err = %v, want the generic panic message without the panic value", err)
	}
	if _, err := getGraph(t.Context(), graphClient); err != nil {
		t.Errorf("GetGraph after a panic: %v", err)
	}
}

func TestDefaultOptions(t *testing.T) {
	logger := silentLogger()
	want := connectapi.Options{
		Timeout:                 30 * time.Second,
		SendTimeout:             30 * time.Second,
		SendMaxBytes:            80 << 20,
		ReadMaxBytes:            1 << 20,
		MaxConcurrent:           4,
		MaxConcurrentReflection: 2,
		Logger:                  logger,
	}
	got := connectapi.DefaultOptions(logger)
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(connectapi.Options{}, "Logger")); diff != "" {
		t.Errorf("options mismatch (-want +got):\n%s", diff)
	}
	if got.Logger != logger {
		t.Errorf("options carry another logger")
	}
}

func TestResponseLargerThanSendLimitIsRejected(t *testing.T) {
	options := defaultOptions()
	options.SendMaxBytes = tinySendMaxBytes
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, options)
	_, err := getGraph(t.Context(), client(server, uncompressed(connect.WithGRPC())...))
	if diff := cmp.Diff(connect.CodeResourceExhausted, connect.CodeOf(err)); diff != "" {
		t.Errorf("code mismatch (-want +got):\n%s", diff)
	}
}

func TestRequestLargerThanReadLimitIsRejected(t *testing.T) {
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, defaultOptions())
	body := `{"padding":"` + strings.Repeat("x", connectapi.ReadMaxBytes) + `"}`
	if reply := postConnectJSON(t, server, getGraphProcedure, body); !strings.Contains(reply, `"code":"resource_exhausted"`) {
		t.Errorf("reply %s is not resource_exhausted", reply)
	}
}

func TestReflectionRequestLargerThanReadLimitIsRejected(t *testing.T) {
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, defaultOptions())
	frame := make([]byte, grpcPrefixBytes+connectapi.ReadMaxBytes+1)
	binary.BigEndian.PutUint32(frame[1:grpcPrefixBytes], uint32(connectapi.ReadMaxBytes+1))
	request := newPost(t, server.URL+reflectionProcedure, bytes.NewReader(frame))
	request.Header.Set("Content-Type", "application/grpc")
	request.Header.Set("Te", "trailers")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer closeBody(t, response)
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	status := response.Trailer.Get("Grpc-Status")
	if status == "" {
		status = response.Header.Get("Grpc-Status")
	}
	if diff := cmp.Diff(grpcResourceStatus, status); diff != "" {
		t.Errorf("grpc-status mismatch (-want +got):\n%s", diff)
	}
}

func TestSlowClientReleasesTheSlotAfterTheWriteTimeout(t *testing.T) {
	versions := map[string]bool{
		"http2":   true,
		"http1.1": false,
	}
	for name, http2 := range versions {
		t.Run(name, func(t *testing.T) {
			options := defaultOptions()
			options.MaxConcurrent = 1
			options.Timeout = slowClientTimeout
			options.SendTimeout = slowClientTimeout
			served := make(chan struct{}, 2)
			server := startServer(t, signalServed(newMux(t, &largeThenSmallReader{
				Large: snapshotOfNodes(slowClientNodes),
				Small: smallSnapshot(),
			}, options), getGraphProcedure, served), http2)
			started := time.Now()
			stallRequest(t, server)
			waitServed(t, served)
			if elapsed := time.Since(started); elapsed < options.Timeout+options.SendTimeout {
				t.Fatalf("the stalled response ended after %v, before the write deadline: the client never stalled", elapsed)
			}
			if _, err := getGraph(t.Context(), client(server, connect.WithProtoJSON())); err != nil {
				t.Errorf("GetGraph after the slow client timed out: %v", err)
			}
		})
	}
}

func TestSlowRequestBodyReleasesTheSlot(t *testing.T) {
	options := defaultOptions()
	options.MaxConcurrent = 1
	options.Timeout = slowClientTimeout
	options.SendTimeout = slowClientTimeout
	served := make(chan struct{}, 2)
	server := startServer(t, signalServed(newMux(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, options), getGraphProcedure, served), false)
	started := time.Now()
	sendEndlessBody(t, server)
	waitServed(t, served)
	if elapsed := time.Since(started); elapsed < options.Timeout+options.SendTimeout {
		t.Fatalf("the slow body was cut after %v, before the deadline", elapsed)
	}
	if _, err := getGraph(t.Context(), client(server, connect.WithProtoJSON())); err != nil {
		t.Errorf("GetGraph after the slow body timed out: %v", err)
	}
}

func sendEndlessBody(t *testing.T, server *httptest.Server) {
	t.Helper()
	body, writer := io.Pipe()
	finished := make(chan struct{})
	t.Cleanup(func() {
		<-finished
	})
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Logf("close body writer: %v", err)
		}
	})
	request := newPost(t, server.URL+getGraphProcedure, body)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	go func() {
		defer close(finished)
		response, err := server.Client().Do(request)
		if err == nil {
			closeBody(t, response)
		}
	}()
}

func signalServed(next http.Handler, path string, served chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		next.ServeHTTP(response, request)
		if request.URL.Path == path {
			served <- struct{}{}
		}
	})
}

func waitServed(t *testing.T, served <-chan struct{}) {
	t.Helper()
	select {
	case <-served:
	case <-time.After(enterTimeout):
		t.Fatalf("the slow client still holds the slot after %v", enterTimeout)
	}
}

func stallRequest(t *testing.T, server *httptest.Server) {
	t.Helper()
	request := newPost(t, server.URL+getGraphProcedure, strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("Accept-Encoding", "identity")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Logf("the server aborted the stalled response before its headers: %v", err)
		return
	}
	t.Cleanup(func() {
		closeBody(t, response)
	})
}

func newPost(t *testing.T, url string, body io.Reader) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, body)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	return request
}

func closeBody(t *testing.T, response *http.Response) {
	t.Helper()
	if err := response.Body.Close(); err != nil {
		t.Logf("close body: %v", err)
	}
}

func TestReflectionStreamsAreLimited(t *testing.T) {
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, defaultOptions())
	reflection := grpcreflect.NewClient(server.Client(), server.URL, connect.WithGRPC())
	streams := make([]*grpcreflect.ClientStream, 0, connectapi.MaxConcurrentReflection)
	defer func() {
		closeStreams(t, streams)
	}()
	for range connectapi.MaxConcurrentReflection {
		stream := reflection.NewStream(t.Context())
		streams = append(streams, stream)
		if _, err := stream.ListServices(); err != nil {
			t.Fatalf("ListServices within the limit: %v", err)
		}
	}
	rejected := reflection.NewStream(t.Context())
	streams = append(streams, rejected)
	_, err := rejected.ListServices()
	checkRetryable(t, err)
}

func TestIdleReflectionStreamsReleaseTheirSlots(t *testing.T) {
	options := defaultOptions()
	options.Timeout = slowClientTimeout
	options.SendTimeout = slowClientTimeout
	served := make(chan struct{}, connectapi.MaxConcurrentReflection)
	server := startServer(t, signalServed(newMux(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, options), reflectionProcedure, served), true)
	reflection := grpcreflect.NewClient(server.Client(), server.URL, connect.WithGRPC())
	idle := make([]*grpcreflect.ClientStream, 0, connectapi.MaxConcurrentReflection)
	defer func() {
		closeStreams(t, idle)
	}()
	started := time.Now()
	for range connectapi.MaxConcurrentReflection {
		stream := reflection.NewStream(t.Context())
		idle = append(idle, stream)
		if _, err := stream.ListServices(); err != nil {
			t.Fatalf("ListServices: %v", err)
		}
	}
	for range connectapi.MaxConcurrentReflection {
		waitServed(t, served)
	}
	if elapsed := time.Since(started); elapsed < options.Timeout+options.SendTimeout {
		t.Fatalf("idle streams ended after %v, before the deadline", elapsed)
	}
	fresh := reflection.NewStream(t.Context())
	defer closeStreams(t, []*grpcreflect.ClientStream{fresh})
	if _, err := fresh.ListServices(); err != nil {
		t.Errorf("ListServices after idle streams timed out: %v", err)
	}
}

func TestEveryGraphServiceMethodIsLimited(t *testing.T) {
	options := defaultOptions()
	options.MaxConcurrent = 0
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, options)
	methods := faultlinev1.File_faultline_v1_graph_proto.Services().ByName("GraphService").Methods()
	for i := range methods.Len() {
		method := methods.Get(i)
		t.Run(string(method.Name()), func(t *testing.T) {
			procedure := "/" + string(method.Parent().FullName()) + "/" + string(method.Name())
			reply := postConnectJSON(t, server, procedure, "{}")
			if !strings.Contains(reply, `"code":"resource_exhausted"`) {
				t.Errorf("%s answered %s without passing the limiter", procedure, reply)
			}
		})
	}
}

func postConnectJSON(t *testing.T, server *httptest.Server, procedure, body string) string {
	t.Helper()
	request := newPost(t, server.URL+procedure, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer closeBody(t, response)
	reply, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return string(reply)
}

func closeStreams(t *testing.T, streams []*grpcreflect.ClientStream) {
	t.Helper()
	for _, stream := range streams {
		if _, err := stream.Close(); err != nil {
			t.Logf("close reflection stream: %v", err)
		}
	}
}

func TestReflectionListsGraphService(t *testing.T) {
	server := startGraphServer(t, &staticReader{
		Snapshot: smallSnapshot(),
	}, defaultOptions())
	stream := grpcreflect.NewClient(server.Client(), server.URL, connect.WithGRPC()).NewStream(t.Context())
	services, err := stream.ListServices()
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if _, err := stream.Close(); err != nil {
		t.Errorf("close reflection stream: %v", err)
	}
	want := protoreflect.FullName(faultlinev1connect.GraphServiceName)
	found := false
	for _, service := range services {
		found = found || service == want
	}
	if !found {
		t.Errorf("reflection services %v do not include %s", services, want)
	}
}

func TestLargestAllowedGraphFitsTheSendLimit(t *testing.T) {
	server := startGraphServer(t, &staticReader{
		Snapshot: largestSnapshot(),
	}, defaultOptions())
	protocols := map[string]connect.ClientOption{
		"proto": connect.WithGRPC(),
		"json":  connect.WithProtoJSON(),
	}
	for name, protocol := range protocols {
		t.Run(name, func(t *testing.T) {
			got, err := getGraph(t.Context(), client(server, uncompressed(protocol)...))
			if err != nil {
				t.Fatalf("GetGraph at the largest allowed config: %v", err)
			}
			if len(got.GetNodes()) != maxConfiguredNodes || len(got.GetEdges()) != maxConfiguredEdges {
				t.Errorf("got %d nodes and %d edges", len(got.GetNodes()), len(got.GetEdges()))
			}
			checkWireSize(t, got)
		})
	}
}

func checkWireSize(t *testing.T, got *faultlinev1.Graph) {
	t.Helper()
	encoded, err := protojson.Marshal(got)
	if err != nil {
		t.Fatalf("protojson: %v", err)
	}
	t.Logf("proto %.1f MiB, json %.1f MiB, limit %d MiB",
		float64(proto.Size(got))/(1<<20), float64(len(encoded))/(1<<20), connectapi.SendMaxBytes>>20)
	if proto.Size(got) > connectapi.SendMaxBytes || len(encoded) > connectapi.SendMaxBytes {
		t.Errorf("worst-case response exceeds SendMaxBytes: proto %d, json %d", proto.Size(got), len(encoded))
	}
}

func worstService(index int) resource.ServiceRef {
	return resource.ServiceRef{
		Namespace: strings.Repeat(`"`, maxNamespaceBytes),
		Name:      strings.Repeat(`"`, maxNameBytes-nodeSuffixDigits) + fmt.Sprintf("%0*d", nodeSuffixDigits, index),
	}
}

func worstNodes(count int) []graph.Node {
	nodes := make([]graph.Node, 0, count)
	for i := range count {
		node := graph.NewNode(worstService(i))
		node.UnansweredCalls = math.MaxUint64
		node.UnansweredFailedCalls = math.MaxUint64
		nodes = append(nodes, node)
	}
	return nodes
}

func snapshotOfNodes(count int) *topology.Snapshot {
	return &topology.Snapshot{
		GeneratedAt: telemetrytest.Epoch(),
		Nodes:       worstNodes(count),
	}
}

func largestSnapshot() *topology.Snapshot {
	nodes := worstNodes(maxConfiguredNodes)
	edges := make([]graph.Edge, 0, maxConfiguredEdges)
	for i := range maxConfiguredEdges {
		source := i % maxConfiguredNodes
		target := (source + 1 + i/maxConfiguredNodes) % maxConfiguredNodes
		edges = append(edges, graph.Edge{
			Source:      nodes[source].ID,
			Target:      nodes[target].ID,
			Calls:       math.MaxUint64,
			FailedCalls: math.MaxUint64,
		})
	}
	return &topology.Snapshot{
		Revision:    math.MaxUint64,
		GeneratedAt: telemetrytest.Epoch(),
		Nodes:       nodes,
		Edges:       edges,
	}
}
