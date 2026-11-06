//go:build ignore

package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	TraceID string
	SpanID  string
}

type spanInfo struct {
	Node   string
	Kind   int
	Parent string
}

type options struct {
	In, Edges, Out string
	PerEdge, Extra int
	Seed           uint64
}

func main() {
	var opts options
	flag.StringVar(&opts.In, "in", "", "input dump (OTLP/JSON lines)")
	flag.StringVar(&opts.Edges, "edges", "", "edges file: '<source_id> <target_id>' per line, # comments")
	flag.StringVar(&opts.Out, "out", "", "output file")
	flag.IntVar(&opts.PerEdge, "per-edge", 3, "traces kept per edge")
	flag.IntVar(&opts.Extra, "extra", 40, "additional random whole traces (unpaired halves, noise)")
	flag.Uint64Var(&opts.Seed, "seed", 1, "seed for the random extra traces")
	flag.Parse()

	if err := run(opts); err != nil {
		slog.Error("trimdump failed", "error", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	lines, err := readLines(opts.In)
	if err != nil {
		return err
	}
	spans, err := indexSpans(lines)
	if err != nil {
		return err
	}
	edges, err := readEdges(opts.Edges)
	if err != nil {
		return err
	}
	keep, err := selectTraces(spans, edges, opts)
	if err != nil {
		return err
	}
	return writeTrimmed(opts.Out, lines, keep)
}

func readLines(path string) ([]map[string]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dump %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), maxLineBytes)
	lines, err := scanLines(scanner)
	if err != nil {
		return nil, err
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read dump %s: %w", path, err)
	}
	return lines, nil
}

func scanLines(scanner *bufio.Scanner) ([]map[string]any, error) {
	var lines []map[string]any
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
			ref := spanRef{
				TraceID: str(span["traceId"]),
				SpanID:  str(span["spanId"]),
			}
			spans[ref] = spanInfo{
				Node:   node,
				Kind:   int(num(span["kind"])),
				Parent: str(span["parentSpanId"]),
			}
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

func selectTraces(spans map[spanRef]spanInfo, edges map[string]bool, opts options) (map[string]bool, error) {
	keep, err := edgeTraces(tracesByEdge(spans), edges, opts.PerEdge)
	if err != nil {
		return nil, err
	}
	addRandomTraces(keep, spans, opts)
	return keep, nil
}

func edgeTraces(pairTraces map[string]map[string]bool, edges map[string]bool, perEdge int) (map[string]bool, error) {
	keep := make(map[string]bool)
	for _, edge := range slices.Sorted(maps.Keys(edges)) {
		traces := slices.Sorted(maps.Keys(pairTraces[edge]))
		if len(traces) == 0 {
			return nil, fmt.Errorf("%s: %w", edge, errEdgeWithoutPair)
		}
		for _, traceID := range traces[:min(perEdge, len(traces))] {
			keep[traceID] = true
		}
	}
	return keep, nil
}

func addRandomTraces(keep map[string]bool, spans map[spanRef]spanInfo, opts options) {
	all := slices.Sorted(maps.Keys(traceIDs(spans)))
	random := rand.New(rand.NewPCG(opts.Seed, opts.Seed))
	random.Shuffle(len(all), func(i, j int) {
		all[i], all[j] = all[j], all[i]
	})
	for _, traceID := range all[:min(opts.Extra, len(all))] {
		keep[traceID] = true
	}
}

func tracesByEdge(spans map[spanRef]spanInfo) map[string]map[string]bool {
	byEdge := make(map[string]map[string]bool)
	for ref, server := range spans {
		edge, paired := pairEdge(spans, ref, server)
		if !paired {
			continue
		}
		if byEdge[edge] == nil {
			byEdge[edge] = make(map[string]bool)
		}
		byEdge[edge][ref.TraceID] = true
	}
	return byEdge
}

func pairEdge(spans map[spanRef]spanInfo, ref spanRef, server spanInfo) (string, bool) {
	if server.Kind != spanKindServer || server.Parent == "" {
		return "", false
	}
	client, ok := spans[spanRef{
		TraceID: ref.TraceID,
		SpanID:  server.Parent,
	}]
	if !ok || client.Kind != spanKindClient {
		return "", false
	}
	return client.Node + " " + server.Node, true
}

func traceIDs(spans map[spanRef]spanInfo) map[string]bool {
	ids := make(map[string]bool)
	for ref := range spans {
		ids[ref.TraceID] = true
	}
	return ids
}

func writeTrimmed(path string, lines []map[string]any, keep map[string]bool) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	written, err := encodeKept(file, lines, keep)
	if err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", path, err), file.Close())
	}
	slog.Info("trimmed dump written", "path", path, "exports", written, "traces", len(keep))
	return file.Close()
}

func encodeKept(out io.Writer, lines []map[string]any, keep map[string]bool) (written int, err error) {
	writer := bufio.NewWriter(out)
	encoder := json.NewEncoder(writer)
	for _, doc := range lines {
		if !filterExport(doc, keep) {
			continue
		}
		if err := encoder.Encode(doc); err != nil {
			return written, err
		}
		written++
	}
	return written, writer.Flush()
}

func filterExport(doc map[string]any, keep map[string]bool) bool {
	return keepNonEmpty(doc, "resourceSpans", func(resourceSpans map[string]any) bool {
		return keepNonEmpty(resourceSpans, "scopeSpans", func(scopeSpans map[string]any) bool {
			return keepNonEmpty(scopeSpans, "spans", func(span map[string]any) bool {
				return keep[str(span["traceId"])]
			})
		})
	})
}

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
