package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	runbookMarker  = "runbook "
	headingMarks   = "#"
	headingDivider = " "
)

func readmeHeadings(t *testing.T) map[string]bool {
	t.Helper()
	path := filepath.Join(otlptest.ModuleRoot(t), "deploy", "dev", "README.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	headings := map[string]bool{}
	for line := range strings.Lines(string(raw)) {
		if title, found := headingTitle(line); found {
			headings[title] = true
		}
	}
	return headings
}

func headingTitle(line string) (string, bool) {
	marks, title, found := strings.Cut(strings.TrimSpace(line), headingDivider)
	return title, found && marks != "" && strings.Trim(marks, headingMarks) == ""
}

func printedHints() []string {
	hints := []string{
		tokenStartHint(tokens.ErrBucketDrift),
	}
	for _, rule := range (&tokenOutput{}).hints() {
		hints = append(hints, rule.text)
	}
	return hints
}

func namedRunbooks(hints []string, headings map[string]bool) map[string]bool {
	named := map[string]bool{}
	for _, hint := range hints {
		if _, runbook, found := strings.Cut(hint, runbookMarker); found {
			named[runbook] = headings[runbook]
		}
	}
	return named
}

func TestEveryRunbookAHintNamesIsAHeadingOfTheREADME(t *testing.T) {
	got := namedRunbooks(printedHints(), readmeHeadings(t))
	if len(got) == 0 {
		t.Fatalf("no hint names a runbook: %q", printedHints())
	}
	want := map[string]bool{}
	for runbook := range got {
		want[runbook] = true
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("runbooks named by hints and found as deploy/dev/README.md headings (-want +got):\n%s", diff)
	}
}
