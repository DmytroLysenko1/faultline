package cli_test

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/cli"
)

const (
	graphvizBinary = "dot"
	svgFormat      = "-Tsvg"
	svgText        = "text"
	labelLineBreak = "\n"
	edgeLabelText  = "calls %d, failed %d"
)

func drawnTexts(t *testing.T, source []byte) []string {
	t.Helper()
	command := exec.CommandContext(t.Context(), graphvizBinary, svgFormat)
	command.Stdin = bytes.NewReader(source)
	svg, err := command.Output()
	if err != nil {
		t.Fatalf("graphviz rejected the DOT: %v\n%s", err, source)
	}
	var texts []string
	decoder := xml.NewDecoder(bytes.NewReader(svg))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return texts
		}
		if err != nil {
			t.Fatalf("graphviz SVG: %v", err)
		}
		if start, ok := token.(xml.StartElement); ok && start.Name.Local == svgText {
			texts = append(texts, textOf(t, decoder, &start))
		}
	}
}

func textOf(t *testing.T, decoder *xml.Decoder, start *xml.StartElement) string {
	t.Helper()
	var text string
	if err := decoder.DecodeElement(&text, start); err != nil {
		t.Fatalf("graphviz SVG text: %v", err)
	}
	return text
}

func expectedTexts(tt *renderCase) []string {
	var texts []string
	for _, label := range tt.Labels {
		texts = append(texts, strings.Split(label, labelLineBreak)...)
	}
	for _, edge := range tt.Calls {
		texts = append(texts, fmt.Sprintf(edgeLabelText, edge.calls, edge.failed))
	}
	slices.Sort(texts)
	return texts
}

func TestGraphvizDrawsEveryLabelAsPlainText(t *testing.T) {
	if _, err := exec.LookPath(graphvizBinary); err != nil {
		t.Skipf("graphviz %q is not installed", graphvizBinary)
	}
	for _, tt := range renderCases() {
		t.Run(tt.Name, func(t *testing.T) {
			got := drawnTexts(t, rendered(t, cli.DOT, graphOf(tt.Nodes, tt.Calls)))
			slices.Sort(got)
			if diff := cmp.Diff(expectedTexts(&tt), got); diff != "" {
				t.Errorf("text drawn by graphviz (-want +got):\n%s", diff)
			}
		})
	}
}
