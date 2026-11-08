//go:build !race

package main

import (
	"bytes"
	"math"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

const (
	worstTimeout = "2m"
	maxNodes     = 20_000
	maxEdges     = 100_000
)

type countingWriter struct {
	written int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.written += len(p)
	return len(p), nil
}

func largestGraph() *faultlinev1.Graph {
	largest := &faultlinev1.Graph{
		Revision:    math.MaxUint64,
		GeneratedAt: timestamppb.New(telemetrytest.Epoch()),
	}
	for index := range maxNodes {
		service := telemetrytest.WorstService(index)
		largest.Nodes = append(largest.Nodes, &faultlinev1.Node{
			Id:                    graph.ServiceNodeID(service),
			Service:               &faultlinev1.ServiceRef{Namespace: service.Namespace, Name: service.Name},
			UnansweredCalls:       math.MaxUint64,
			UnansweredFailedCalls: math.MaxUint64,
		})
	}
	for index := range maxEdges {
		source := index % maxNodes
		largest.Edges = append(largest.Edges, &faultlinev1.Edge{
			SourceIndex: uint32(source),
			TargetIndex: uint32((source + 1 + index/maxNodes) % maxNodes),
			Calls:       math.MaxUint64,
			FailedCalls: math.MaxUint64,
		})
	}
	return largest
}

func TestLargestAllowedGraphPassesTheClientLimit(t *testing.T) {
	server := serveGraph(t, fixed(largestGraph()))
	for _, format := range []string{formatJSON, formatDOT} {
		t.Run(format, func(t *testing.T) {
			var stdout countingWriter
			var stderr bytes.Buffer
			code := realMain(process{
				Args:   []string{commandGraph, commandExport, "-o", format, "--server", server, "--timeout", worstTimeout},
				Lookup: environment(nil),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			t.Logf("-o %s wrote %.1f MiB", format, float64(stdout.written)/(1<<20))
			if code != exitOK || stdout.written != renderedSize(t, format) {
				t.Errorf("faultctl -o %s = %d after %d bytes, want 0; stderr: %s",
					format, code, stdout.written, stderr.String())
			}
		})
	}
}

func renderedSize(t *testing.T, format string) int {
	t.Helper()
	var local countingWriter
	if err := renderers()[format](&local, largestGraph()); err != nil {
		t.Fatalf("render locally: %v", err)
	}
	return local.written
}
