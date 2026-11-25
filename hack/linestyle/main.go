package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
)

const (
	lineLimit    = 128
	tabWidth     = 4
	exitClean    = 0
	exitFound    = 1
	exitBroken   = 2
	lineComment  = "//"
	trailingDiff = ","

	callFits       = "call fits on one line of 128: put it on one line"
	keyedOnOneLine = "literal with named fields on one line: one field per line"
)

var (
	errUnreadable = errors.New("cannot read")
	gaps          = regexp.MustCompile(`\s*\n\s*`)
)

type finding struct {
	Path string
	Line int
	Kind string
}

func (f finding) String() string {
	return fmt.Sprintf("%s:%d: %s", f.Path, f.Line, f.Kind)
}

type source struct {
	path  string
	text  []byte
	files *token.FileSet
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(paths []string, stdout, stderr io.Writer) int {
	found, err := check(paths)
	if err != nil {
		return broken(stderr, err)
	}
	if err := report(stdout, found); err != nil {
		return broken(stderr, err)
	}
	if len(found) > 0 {
		return exitFound
	}
	return exitClean
}

func report(stdout io.Writer, found []finding) error {
	errs := make([]error, 0, len(found))
	for _, each := range found {
		_, err := fmt.Fprintln(stdout, each)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func broken(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintln(stderr, err)
	return exitBroken
}

func check(paths []string) ([]finding, error) {
	var found []finding
	for _, path := range paths {
		more, err := checkFile(path)
		if err != nil {
			return nil, err
		}
		found = append(found, more...)
	}
	return found, nil
}

func checkFile(path string) ([]finding, error) {
	text, err := os.ReadFile(path) //nolint:gosec // G304: the paths are the repository's own Go files given by make
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", errUnreadable, path, err)
	}
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, text, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", errUnreadable, path, err)
	}
	src := &source{
		path:  path,
		text:  text,
		files: files,
	}
	var found []finding
	ast.Inspect(parsed, func(node ast.Node) bool {
		found = append(found, src.inspect(node)...)
		return true
	})
	return found, nil
}

func (s *source) inspect(node ast.Node) []finding {
	switch typed := node.(type) {
	case *ast.CallExpr:
		return s.call(typed)
	case *ast.CompositeLit:
		return s.literal(typed)
	}
	return nil
}

func (s *source) call(call *ast.CallExpr) []finding {
	if len(call.Args) == 0 || !s.spans(call.Lparen, call.Rparen) || holdsBlock(call) {
		return nil
	}
	return s.foldable(call.Lparen, call.Rparen, callFits)
}

func (s *source) literal(literal *ast.CompositeLit) []finding {
	if !keyed(literal) || s.spans(literal.Lbrace, literal.Rbrace) {
		return nil
	}
	return []finding{s.at(literal.Lbrace, keyedOnOneLine)}
}

func keyed(literal *ast.CompositeLit) bool {
	return slices.ContainsFunc(literal.Elts, func(element ast.Expr) bool {
		_, isKeyed := element.(*ast.KeyValueExpr)
		return isKeyed
	})
}

func (s *source) spans(open, closing token.Pos) bool {
	return s.files.Position(open).Line != s.files.Position(closing).Line
}

func (s *source) foldable(open, closing token.Pos, kind string) []finding {
	start, end := s.files.Position(open).Offset, s.files.Position(closing).Offset
	inner := string(s.text[start+1 : end])
	if strings.Contains(inner, lineComment) {
		return nil
	}
	joined := strings.TrimSuffix(strings.TrimSpace(gaps.ReplaceAllString(inner, " ")), trailingDiff)
	lineStart := strings.LastIndexByte(string(s.text[:start]), '\n') + 1
	lineEnd := end + 1 + strings.IndexByte(string(s.text[end+1:]), '\n')
	candidate := string(s.text[lineStart:start+1]) + joined + string(s.text[end:lineEnd])
	if width(candidate) > lineLimit {
		return nil
	}
	return []finding{s.at(open, kind)}
}

func (s *source) at(position token.Pos, kind string) finding {
	return finding{
		Path: s.path,
		Line: s.files.Position(position).Line,
		Kind: kind,
	}
}

func holdsBlock(node ast.Node) bool {
	held := false
	ast.Inspect(node, func(inner ast.Node) bool {
		held = held || isBlock(inner)
		return !held
	})
	return held
}

func isBlock(node ast.Node) bool {
	switch typed := node.(type) {
	case *ast.FuncLit:
		return true
	case *ast.CompositeLit:
		return len(typed.Elts) > 1 || keyed(typed)
	}
	return false
}

func width(line string) int {
	return len(line) + (tabWidth-1)*strings.Count(line, "\t")
}
