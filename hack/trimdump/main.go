//go:build ignore

// Command trimdump cuts an OTel Demo trace dump (collector file exporter, one OTLP/JSON
// export per line) down to a small fixture for the step 1 replay test.
//
// Rules (design step 1, «Откуда эталон», item 4):
//   - only whole traces are kept: a trace is either complete in the output or absent,
//     otherwise pairs would lose a half;
//   - export boundaries and their order are kept: an output line is an input line with the
//     spans of dropped traces removed; lines left without spans are dropped;
//   - every edge in -edges keeps at least -per-edge traces with a full client/server pair,
//     and the run fails if an edge has none.
//
// Usage:
//
//	go run hack/trimdump/main.go -in tmp/capture/traces.jsonl -edges edges.txt \
//	    -per-edge 3 -extra 40 -out test/demo/otel-demo/traces.otlp.json
//
// Node ids are built from the resource attributes by the faultline rule (k8s.namespace.name,
// else service.namespace, else empty), so a service that lost its k8s enrichment gets a
// different id than the edges file and the run fails loudly instead of hiding it.
package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/url"
	"os"
	"slices"
	"strings"
)

const (
	spanKindServer = 2
	spanKindClient = 3
	maxLineBytes   = 64 << 20
)

var (
	errEdgeWithoutPair  = errors.New("edge has no complete client/server pair in the dump")
	errDumpWithoutSpans = errors.New("dump has no spans")
)

type spanRef struct {
	traceID string
	spanID  string
}

type spanInfo struct {
	node   string
	kind   int
	parent string
}

type options struct {
	in, edges, out string
	perEdge, extra int
	seed           uint64
}

func main() {
	var opts options
	flag.StringVar(&opts.in, "in", "", "input dump (OTLP/JSON lines)")
	flag.StringVar(&opts.edges, "edges", "", "edges file: '<source_id> <target_id>' per line, # comments")
	flag.StringVar(&opts.out, "out", "", "output file")
	flag.IntVar(&opts.perEdge, "per-edge", 3, "traces kept per edge")
	flag.IntVar(&opts.extra, "extra", 40, "additional random whole traces (unpaired halves, noise)")
	flag.Uint64Var(&opts.seed, "seed", 1, "seed for the random extra traces")
	flag.Parse()

	if err := run(opts); err != nil {
		slog.Error("trimdump failed", "error", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	lines, err := readLines(opts.in)
	if err != nil {
		return err
	}
	spans, err := indexSpans(lines)
	if err != nil {
		return err
	}
	edges, err := readEdges(opts.edges)
	if err != nil {
		return err
	}
	keep, err := selectTraces(spans, edges, opts)
	if err != nil {
		return err
	}
	return writeTrimmed(opts.out, lines, keep)
}

func readLines(path string) ([]map[string]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dump %s: %w", path, err)
	}
	defer file.Close()

	var lines []map[string]any
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), maxLineBytes)
	for scanner.Scan() {
		if len(strings.TrimSpace(scanner.Text())) == 0 {
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			return nil, fmt.Errorf("parse dump line %d: %w", len(lines)+1, err)
		}
		lines = append(lines, doc)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read dump %s: %w", path, err)
	}
	return lines, nil
}

func indexSpans(lines []map[string]any) (map[spanRef]spanInfo, error) {
	spans := make(map[spanRef]spanInfo)
	for _, doc := range lines {
		for _, resourceSpans := range list(doc["resourceSpans"]) {
			indexResource(spans, resourceSpans)
		}
	}
	if len(spans) == 0 {
		return nil, errDumpWithoutSpans
	}
	return spans, nil
}

func indexResource(spans map[spanRef]spanInfo, resourceSpans map[string]any) {
	node := resourceNodeID(resourceSpans)
	for _, scopeSpans := range list(resourceSpans["scopeSpans"]) {
		for _, span := range list(scopeSpans["spans"]) {
			ref := spanRef{traceID: str(span["traceId"]), spanID: str(span["spanId"])}
			spans[ref] = spanInfo{node: node, kind: int(num(span["kind"])), parent: str(span["parentSpanId"])}
		}
	}
}

func readEdges(path string) (map[string]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read edges %s: %w", path, err)
	}
	edges := make(map[string]bool)
	for line := range strings.Lines(string(data)) {
		line, _, _ = strings.Cut(line, "#")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		edges[fields[0]+" "+fields[1]] = true
	}
	return edges, nil
}

// selectTraces keeps perEdge traces with a full pair for every edge, plus extra random traces.
func selectTraces(spans map[spanRef]spanInfo, edges map[string]bool, opts options) (map[string]bool, error) {
	pairTraces := tracesByEdge(spans)
	keep := make(map[string]bool)
	for _, edge := range slices.Sorted(maps.Keys(edges)) {
		traces := slices.Sorted(maps.Keys(pairTraces[edge]))
		if len(traces) == 0 {
			return nil, fmt.Errorf("%s: %w", edge, errEdgeWithoutPair)
		}
		for _, traceID := range traces[:min(opts.perEdge, len(traces))] {
			keep[traceID] = true
		}
	}

	all := slices.Sorted(maps.Keys(traceIDs(spans)))
	random := rand.New(rand.NewPCG(opts.seed, opts.seed))
	random.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	for _, traceID := range all[:min(opts.extra, len(all))] {
		keep[traceID] = true
	}
	return keep, nil
}

func tracesByEdge(spans map[spanRef]spanInfo) map[string]map[string]bool {
	byEdge := make(map[string]map[string]bool)
	for ref, server := range spans {
		if server.kind != spanKindServer || server.parent == "" {
			continue
		}
		client, ok := spans[spanRef{traceID: ref.traceID, spanID: server.parent}]
		if !ok || client.kind != spanKindClient {
			continue
		}
		edge := client.node + " " + server.node
		if byEdge[edge] == nil {
			byEdge[edge] = make(map[string]bool)
		}
		byEdge[edge][ref.traceID] = true
	}
	return byEdge
}

func traceIDs(spans map[spanRef]spanInfo) map[string]bool {
	ids := make(map[string]bool)
	for ref := range spans {
		ids[ref.traceID] = true
	}
	return ids
}

func writeTrimmed(path string, lines []map[string]any, keep map[string]bool) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	written := 0
	for _, doc := range lines {
		if !filterExport(doc, keep) {
			continue
		}
		if err := encoder.Encode(doc); err != nil {
			return errors.Join(fmt.Errorf("write %s: %w", path, err), file.Close())
		}
		written++
	}
	if err := writer.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush %s: %w", path, err), file.Close())
	}
	slog.Info("trimmed dump written", "path", path, "exports", written, "traces", len(keep))
	return file.Close()
}

// filterExport drops spans of traces outside keep and reports whether any span is left.
// Each level keeps only children that still hold spans, so no empty resource or scope remains.
func filterExport(doc map[string]any, keep map[string]bool) bool {
	return keepNonEmpty(doc, "resourceSpans", func(resourceSpans map[string]any) bool {
		return keepNonEmpty(resourceSpans, "scopeSpans", func(scopeSpans map[string]any) bool {
			return keepNonEmpty(scopeSpans, "spans", func(span map[string]any) bool {
				return keep[str(span["traceId"])]
			})
		})
	})
}

// keepNonEmpty replaces parent[key] with the children that pass keepChild and reports
// whether any child is left. Hand-written instead of samber/lo: this file is build-ignored,
// and a module dependency used only here would be removed by `go mod tidy` (make verify).
func keepNonEmpty(parent map[string]any, key string, keepChild func(map[string]any) bool) bool {
	var kept []any
	for _, child := range list(parent[key]) {
		if keepChild(child) {
			kept = append(kept, child)
		}
	}
	parent[key] = kept
	return len(kept) > 0
}

// resourceNodeID mirrors the faultline node id contract (design step 1, ServiceRef and Node.id).
func resourceNodeID(resourceSpans map[string]any) string {
	resource, _ := resourceSpans["resource"].(map[string]any)
	attributes := make(map[string]string)
	for _, attribute := range list(resource["attributes"]) {
		value, _ := attribute["value"].(map[string]any)
		attributes[str(attribute["key"])] = str(value["stringValue"])
	}
	namespace := cmp.Or(attributes["k8s.namespace.name"], attributes["service.namespace"])
	return "svc:" + url.PathEscape(namespace) + "/" + url.PathEscape(attributes["service.name"])
}

func list(value any) []map[string]any {
	items, _ := value.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if object, ok := item.(map[string]any); ok {
			out = append(out, object)
		}
	}
	return out
}

func str(value any) string {
	text, _ := value.(string)
	return text
}

func num(value any) float64 {
	number, _ := value.(float64)
	return number
}
