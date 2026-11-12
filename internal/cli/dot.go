package cli

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

const (
	dotHeader   = "digraph faultline {\n"
	dotFooter   = "}\n"
	nodeFormat  = "\t%s [label=%s];\n"
	edgeFormat  = "\t%s -> %s [label=%s];\n"
	callsFormat = "calls %d, failed %d"
	unanswered  = "unanswered %d (failed %d)"
	pathJoin    = "/"
	lineBreak   = "\n"
	served      = "served %s"
)

func DOT(w io.Writer, graph *faultlinev1.Graph) error {
	buffered := bufio.NewWriter(w)
	if _, err := buffered.WriteString(dotHeader); err != nil {
		return err
	}
	if err := writeNodes(buffered, graph.GetNodes()); err != nil {
		return err
	}
	if err := writeEdges(buffered, graph); err != nil {
		return err
	}
	if _, err := buffered.WriteString(dotFooter); err != nil {
		return err
	}
	return buffered.Flush()
}

func writeNodes(w io.Writer, nodes []*faultlinev1.Node) error {
	for _, node := range nodes {
		if _, err := fmt.Fprintf(w, nodeFormat, quoteID(node.GetId()), quoteLabel(nodeLabel(node))); err != nil {
			return err
		}
	}
	return nil
}

func writeEdges(w io.Writer, graph *faultlinev1.Graph) error {
	nodes := graph.GetNodes()
	for _, edge := range graph.GetEdges() {
		source := nodes[edge.GetSourceIndex()].GetId()
		target := nodes[edge.GetTargetIndex()].GetId()
		if _, err := fmt.Fprintf(w, edgeFormat, quoteID(source), quoteID(target), quoteLabel(edgeLabel(edge))); err != nil {
			return err
		}
	}
	return nil
}

func edgeLabel(edge *faultlinev1.Edge) string {
	lines := []string{
		fmt.Sprintf(callsFormat, edge.GetCalls(), edge.GetFailedCalls()),
	}
	if summary := redSummary(edge.GetFinal(), edge.GetLive()); summary != "" {
		lines = append(lines, summary)
	}
	return strings.Join(lines, lineBreak)
}

func nodeLabel(node *faultlinev1.Node) string {
	lines := []string{
		serviceName(node.GetService()),
	}
	if summary := redSummary(node.GetFinal(), node.GetLive()); summary != "" {
		lines = append(lines, fmt.Sprintf(served, summary))
	}
	if node.GetUnansweredCalls() > 0 {
		lines = append(lines, fmt.Sprintf(unanswered, node.GetUnansweredCalls(), node.GetUnansweredFailedCalls()))
	}
	return strings.Join(lines, lineBreak)
}

func serviceName(service *faultlinev1.ServiceRef) string {
	if service.GetNamespace() == "" {
		return service.GetName()
	}
	return service.GetNamespace() + pathJoin + service.GetName()
}

func quoteID(text string) string {
	return quote(text, idChar)
}

func quoteLabel(text string) string {
	return quote(text, labelChar)
}

func quote(text string, escape func(char rune) string) string {
	var quoted strings.Builder
	quoted.Grow(len(text) + len(`""`))
	quoted.WriteByte('"')
	for _, char := range text {
		quoted.WriteString(escape(char))
	}
	quoted.WriteByte('"')
	return quoted.String()
}

func labelChar(char rune) string {
	if char == '&' {
		return "&amp;"
	}
	return idChar(char)
}

func idChar(char rune) string {
	switch char {
	case '\\':
		return `\\`
	case '"':
		return `\"`
	case '\n':
		return `\n`
	default:
		return strings.ReplaceAll(printable(char), `\`, `\\`)
	}
}
