package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	statsMessage = "pipeline.stats"
	maxLogLine   = 1 << 20
)

var (
	errNoGraph      = errors.New("-graph is required")
	errDanglingEdge = errors.New("edge refers to a missing node")
	errNoStats      = errors.New("no pipeline.stats record in the log on stdin")
)

type shownGraph struct {
	revision     uint64
	edges        []reference.Edge
	calls        uint64
	timedFinals  map[reference.Edge]bool
	servedFinals map[string]bool
}

type pairing struct {
	Observed   uint64 `json:"observed"`
	Matched    uint64 `json:"matched"`
	Duplicate  uint64 `json:"duplicate"`
	Unpairable uint64 `json:"unpairable"`
	Evicted    uint64 `json:"evicted"`
	Expired    uint64 `json:"expired"`
	Pending    uint64 `json:"pending"`
}

type graphStats struct {
	GraphOverflow uint64  `json:"graph_overflow"`
	CallsTotal    *uint64 `json:"calls_total"`
	ServedTotal   *uint64 `json:"served_total"`
}

type statsRecord struct {
	Message     string       `json:"msg"`
	Revision    uint64       `json:"revision"`
	Pairing     pairing      `json:"pairing"`
	Graph       graphStats   `json:"graph"`
	Windows     *windows     `json:"windows"`
	Cardinality *cardinality `json:"cardinality"`
}

func readGraph(path string) (shownGraph, error) {
	raw, err := readFile(path)
	if err != nil {
		return shownGraph{}, err
	}
	var graph faultlinev1.Graph
	if err := protojson.Unmarshal(raw, &graph); err != nil {
		return shownGraph{}, fmt.Errorf("parse graph %s: %w", path, err)
	}
	return graphOf(&graph)
}

func readFile(path string) ([]byte, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("open directory of %s: %w", path, err)
	}
	raw, readErr := root.ReadFile(filepath.Base(path))
	if err := errors.Join(readErr, root.Close()); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return raw, nil
}

func graphOf(graph *faultlinev1.Graph) (shownGraph, error) {
	nodes := graph.GetNodes()
	shown := shownGraph{
		revision:     graph.GetRevision(),
		timedFinals:  map[reference.Edge]bool{},
		servedFinals: map[string]bool{},
	}
	for _, node := range nodes {
		shown.servedFinals[node.GetId()] = node.GetFinal().GetRequests() > 0
	}
	for index, edge := range graph.GetEdges() {
		if int(edge.GetSourceIndex()) >= len(nodes) || int(edge.GetTargetIndex()) >= len(nodes) {
			return shownGraph{}, fmt.Errorf("edge %d: %w", index, errDanglingEdge)
		}
		named := reference.Edge{
			Source: nodes[edge.GetSourceIndex()].GetId(),
			Target: nodes[edge.GetTargetIndex()].GetId(),
		}
		shown.edges = append(shown.edges, named)
		shown.calls += edge.GetCalls()
		shown.timedFinals[named] = timed(edge.GetFinal())
	}
	return shown, nil
}

func timed(final *faultlinev1.WindowStats) bool {
	return final.GetRequests() > 0 && final.GetLatencySamples() > 0
}

func lastStats(log io.Reader) (statsRecord, error) {
	var last *statsRecord
	scanner := bufio.NewScanner(log)
	scanner.Buffer(nil, maxLogLine)
	for scanner.Scan() {
		if record, ok := statsLine(scanner.Text()); ok {
			last = &record
		}
	}
	if err := scanner.Err(); err != nil {
		return statsRecord{}, fmt.Errorf("read log: %w", err)
	}
	if last == nil {
		return statsRecord{}, errNoStats
	}
	return *last, nil
}

func statsLine(line string) (statsRecord, bool) {
	if !strings.Contains(line, statsMessage) {
		return statsRecord{}, false
	}
	var record statsRecord
	if err := json.Unmarshal([]byte(line), &record); err != nil || record.Message != statsMessage {
		return statsRecord{}, false
	}
	return record, true
}
