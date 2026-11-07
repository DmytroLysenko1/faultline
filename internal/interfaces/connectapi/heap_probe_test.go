//go:build !race

package connectapi_test

import (
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

const (
	probeNodes         = 10_000
	probeEdges         = 50_000
	probeRequests      = 3
	jsonContentType    = "application/json"
	emptyJSONRequest   = "{}"
	printableFirst     = '!'
	printableLast      = '~'
	randomSeed         = 7
	encodingGzip       = "gzip"
	encodingIdentity   = "identity"
	headerContentType  = "Content-Type"
	headerAccept       = "Accept-Encoding"
	randomSuffixDigits = 7
	mebibyte           = 1 << 20
)

type serviceNaming func(index int) resource.ServiceRef

func randomServices() serviceNaming {
	source := rand.New(rand.NewPCG(randomSeed, randomSeed))
	random := func(length int) string {
		var text strings.Builder
		for range length {
			text.WriteRune(printableFirst + rune(source.IntN(printableLast-printableFirst+1)))
		}
		return text.String()
	}
	return func(index int) resource.ServiceRef {
		suffix := fmt.Sprintf("%0*d", randomSuffixDigits, index)
		return resource.ServiceRef{
			Namespace: random(resource.MaxServiceNamespaceBytes),
			Name:      random(resource.MaxServiceNameBytes-randomSuffixDigits) + suffix,
		}
	}
}

func snapshotOf(naming serviceNaming) *topology.Snapshot {
	nodes := make([]graph.Node, 0, probeNodes)
	for index := range probeNodes {
		node := graph.NewNode(naming(index))
		node.UnansweredCalls = math.MaxUint64
		node.UnansweredFailedCalls = math.MaxUint64
		nodes = append(nodes, node)
	}
	edges := make([]graph.Edge, 0, probeEdges)
	for index := range probeEdges {
		source := index % probeNodes
		target := (source + 1 + index/probeNodes) % probeNodes
		edges = append(edges, graph.Edge{
			Source:      nodes[source].ID,
			Target:      nodes[target].ID,
			Calls:       math.MaxUint64,
			FailedCalls: math.MaxUint64,
		})
	}
	return &topology.Snapshot{
		Revision: math.MaxUint64,
		Nodes:    nodes,
		Edges:    edges,
	}
}

func serverPeak(t *testing.T, snapshot *topology.Snapshot, encoding string) int64 {
	t.Helper()
	server := startGraphServer(t, &staticReader{
		Snapshot: snapshot,
	}, defaultOptions())
	var worst int64
	for range probeRequests {
		before := int64(telemetrytest.HeapInUse())
		sampler := telemetrytest.StartPeakSampler(t)
		discardResponse(t, server.URL, server.Client(), encoding)
		worst = max(worst, int64(sampler.Stop())-before)
	}
	runtime.KeepAlive(snapshot)
	return worst
}

func discardResponse(t *testing.T, address string, client *http.Client, encoding string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		address+faultlinev1connect.GraphServiceGetGraphProcedure, strings.NewReader(emptyJSONRequest))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set(headerContentType, jsonContentType)
	request.Header.Set(headerAccept, encoding)
	response, err := client.Transport.RoundTrip(request)
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close response: %v", err)
		}
	}()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GetGraph status %d", response.StatusCode)
	}
}

func TestWorstCaseHeapCoversOneGetGraphOnTheRealHandler(t *testing.T) {
	type args struct {
		naming   serviceNaming
		encoding string
	}
	model := connectapi.WorstHeapBytes(probeNodes, probeEdges, 0) / connectapi.MaxConcurrentGetGraph
	tests := []struct {
		Name string
		Args args
	}{
		{
			Name: "longest json, gzip",
			Args: args{
				naming:   telemetrytest.WorstService,
				encoding: encodingGzip,
			},
		},
		{
			Name: "longest json, identity",
			Args: args{
				naming:   telemetrytest.WorstService,
				encoding: encodingIdentity,
			},
		},
		{
			Name: "incompressible strings, gzip",
			Args: args{
				naming:   randomServices(),
				encoding: encodingGzip,
			},
		},
		{
			Name: "incompressible strings, identity",
			Args: args{
				naming:   randomServices(),
				encoding: encodingIdentity,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			peak := serverPeak(t, snapshotOf(tt.Args.naming), tt.Args.encoding)
			t.Logf("server held %.1f MiB, model %.1f MiB", float64(peak)/mebibyte, float64(model)/mebibyte)
			if peak > model {
				t.Errorf("server held %d bytes for one GetGraph, model %d", peak, model)
			}
		})
	}
}
