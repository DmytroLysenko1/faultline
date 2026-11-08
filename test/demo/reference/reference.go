package reference

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	commentPrefix   = "#"
	reasonSeparator = " # "
	fieldsInLine    = 2
	Namespace       = "svc:otel-demo/"
)

var (
	ErrMalformedLine = errors.New("edge line is not '<source_id> <target_id>'")
	ErrUnjustified   = errors.New("optional edge has no reason after ' # '")
)

type Edge struct {
	Source string
	Target string
}

type Reference struct {
	Required map[Edge]bool
	Optional map[Edge]bool
}

type Result struct {
	Missing     []Edge
	Phantoms    []Edge
	Unqualified []string
}

func (r Result) Passed() bool {
	return len(r.Missing) == 0 && len(r.Phantoms) == 0 && len(r.Unqualified) == 0
}

func Load(requiredPath, optionalPath string) (Reference, error) {
	required, err := readFile(requiredPath, false)
	if err != nil {
		return Reference{}, err
	}
	optional, err := readFile(optionalPath, true)
	if err != nil {
		return Reference{}, err
	}
	return Reference{
		Required: required,
		Optional: optional,
	}, nil
}

func readFile(path string, needsReason bool) (map[Edge]bool, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("open reference directory of %s: %w", path, err)
	}
	file, err := root.Open(filepath.Base(path))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open reference %s: %w", path, err), root.Close())
	}
	edges, readErr := Read(file, needsReason)
	if err := errors.Join(readErr, file.Close(), root.Close()); err != nil {
		return nil, fmt.Errorf("read reference %s: %w", path, err)
	}
	return edges, nil
}

func Read(source io.Reader, needsReason bool) (map[Edge]bool, error) {
	edges := map[Edge]bool{}
	scanner := bufio.NewScanner(source)
	for number := 1; scanner.Scan(); number++ {
		edge, found, err := parseLine(scanner.Text(), needsReason)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", number, err)
		}
		if found {
			edges[edge] = true
		}
	}
	return edges, scanner.Err()
}

func parseLine(line string, needsReason bool) (Edge, bool, error) {
	line = strings.TrimSpace(line)
	if isBlank(line) {
		return Edge{}, false, nil
	}
	edge, reason, _ := strings.Cut(line, reasonSeparator)
	if needsReason && strings.TrimSpace(reason) == "" {
		return Edge{}, false, fmt.Errorf("%q: %w", line, ErrUnjustified)
	}
	fields := strings.Fields(edge)
	if len(fields) != fieldsInLine {
		return Edge{}, false, fmt.Errorf("%q: %w", line, ErrMalformedLine)
	}
	return Edge{
		Source: fields[0],
		Target: fields[1],
	}, true, nil
}

func isBlank(line string) bool {
	return line == "" || strings.HasPrefix(line, commentPrefix)
}

func (r Reference) Check(edges []Edge) Result {
	return Result{
		Missing:     r.missing(edges),
		Phantoms:    r.phantoms(edges),
		Unqualified: unqualified(edges),
	}
}

func (r Reference) missing(edges []Edge) []Edge {
	var missing []Edge
	for edge := range r.Required {
		if !slices.Contains(edges, edge) {
			missing = append(missing, edge)
		}
	}
	return sortedEdges(missing)
}

func (r Reference) phantoms(edges []Edge) []Edge {
	var phantoms []Edge
	for _, edge := range edges {
		if !r.Required[edge] && !r.Optional[edge] {
			phantoms = append(phantoms, edge)
		}
	}
	return sortedEdges(phantoms)
}

func unqualified(edges []Edge) []string {
	nodes := map[string]bool{}
	for _, edge := range edges {
		nodes[edge.Source] = !strings.HasPrefix(edge.Source, Namespace)
		nodes[edge.Target] = !strings.HasPrefix(edge.Target, Namespace)
	}
	var found []string
	for node, outside := range nodes {
		if outside {
			found = append(found, node)
		}
	}
	slices.Sort(found)
	return found
}

func sortedEdges(edges []Edge) []Edge {
	slices.SortFunc(edges, func(a, b Edge) int {
		return strings.Compare(a.Source+" "+a.Target, b.Source+" "+b.Target)
	})
	return edges
}
