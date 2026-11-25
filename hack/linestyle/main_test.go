package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"
)

const fileMode = 0o600

func TestOnlyWhatFitsOnOneLineIsReported(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []int
	}{
		{
			name: "a call that fits on one line",
			body: "\tf(\n\t\t1,\n\t\t2,\n\t)\n",
			want: []int{4},
		},
		{
			name: "a call on one line",
			body: "\tf(1, 2)\n",
			want: nil,
		},
		{
			name: "a call that does not fit",
			body: "\tf(\n\t\t" + longArg() + ",\n\t\t" + longArg() + ",\n\t\t" + longArg() + ",\n\t)\n",
			want: nil,
		},
		{
			name: "a call holding a function literal",
			body: "\tf(func() {\n\t\tg()\n\t})\n",
			want: nil,
		},
		{
			name: "a call holding a literal of two fields",
			body: "\tf(pair{\n\t\ta: 1,\n\t\tb: 2,\n\t})\n",
			want: nil,
		},
		{
			name: "a call holding a comment",
			body: "\tf(\n\t\t1, // why\n\t)\n",
			want: nil,
		},
		{
			name: "a literal of one named field on one line",
			body: "\t_ = single{a: 1}\n",
			want: []int{4},
		},
		{
			name: "a literal of two named fields on one line",
			body: "\t_ = pair{a: 1, b: 2}\n",
			want: []int{4},
		},
		{
			name: "a literal of one named field on its own line",
			body: "\t_ = single{\n\t\ta: 1,\n\t}\n",
			want: nil,
		},
		{
			name: "a list without names on one line",
			body: "\t_ = []int{1, 2}\n",
			want: nil,
		},
		{
			name: "a call holding a named literal",
			body: "\tf(single{\n\t\ta: 1,\n\t})\n",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found, err := check([]string{sourceFile(t, tt.body)})
			if err != nil {
				t.Fatalf("check: %v", err)
			}
			if diff := cmp.Diff(tt.want, linesOf(found)); diff != "" {
				t.Errorf("reported lines (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheExitCodeSaysWhetherAnythingWasFound(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		want  int
	}{
		{
			name:  "nothing found",
			paths: []string{sourceFile(t, "\tf(1)\n")},
			want:  exitClean,
		},
		{
			name:  "something found",
			paths: []string{sourceFile(t, "\tf(\n\t\t1,\n\t)\n")},
			want:  exitFound,
		},
		{
			name:  "a file that cannot be read",
			paths: []string{filepath.Join(t.TempDir(), "missing.go")},
			want:  exitBroken,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := run(tt.paths, &stdout, &stderr); got != tt.want {
				t.Errorf("run = %d, want %d (stdout %q, stderr %q)", got, tt.want, stdout.String(), stderr.String())
			}
		})
	}
}

func sourceFile(t *testing.T, body string) string {
	t.Helper()
	text := "package sample\n\nfunc sample() {\n" + body + "}\n"
	path := filepath.Join(t.TempDir(), "sample.go")
	if err := os.WriteFile(path, []byte(text), fileMode); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func linesOf(found []finding) []int {
	if len(found) == 0 {
		return nil
	}
	return lo.Map(found, func(each finding, _ int) int {
		return each.Line
	})
}

func longArg() string {
	return "argumentWithAVeryLongNameThatTakesManyColumnsOnItsOwnLine"
}
